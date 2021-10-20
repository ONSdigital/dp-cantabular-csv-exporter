package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/ONSdigital/dp-cantabular-csv-exporter/config"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/handler"
	"github.com/ONSdigital/dp-healthcheck/v2/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v3"
	"github.com/ONSdigital/log.go/v2/log"

	"github.com/gorilla/mux"
)

// Service contains all the configs, server and clients to run the event handler service
type Service struct {
	cfg              *config.Config
	server           HTTPServer
	healthCheck      HealthChecker
	consumer         kafka.IConsumerGroup
	producer         kafka.IProducer
	processor        Processor
	datasetAPIClient DatasetAPIClient
	cantabularClient CantabularClient
	s3Uploader       S3Uploader
	vaultClient      VaultClient
	generator        Generator
}

func New() *Service {
	return &Service{}
}

// Init initialises the service and it's dependencies
func (svc *Service) Init(ctx context.Context, cfg *config.Config, buildTime, gitCommit, version string) error {
	var err error

	if cfg == nil {
		return errors.New("nil config passed to service init")
	}

	svc.cfg = cfg

	if svc.consumer, err = GetKafkaConsumer(ctx, cfg); err != nil {
		return fmt.Errorf("failed to create kafka consumer: %w", err)
	}
	if svc.producer, err = GetKafkaProducer(ctx, cfg); err != nil {
		return fmt.Errorf("failed to create kafka producer: %w", err)
	}
	if svc.s3Uploader, err = GetS3Uploader(cfg); err != nil {
		return fmt.Errorf("failed to initialise s3 uploader: %w", err)
	}
	if !cfg.EncryptionDisabled {
		if svc.vaultClient, err = GetVault(cfg); err != nil {
			return fmt.Errorf("failed to initialise vault client: %w", err)
		}
	}

	svc.cantabularClient = GetCantabularClient(cfg)
	svc.datasetAPIClient = GetDatasetAPIClient(cfg)

	svc.processor = GetProcessor(cfg)
	svc.generator = GetGenerator()

	// Get HealthCheck
	if svc.healthCheck, err = GetHealthCheck(cfg, buildTime, gitCommit, version); err != nil {
		return fmt.Errorf("could not instantiate healthcheck: %w", err)
	}

	if err := svc.registerCheckers(); err != nil {
		return fmt.Errorf("error initialising checkers: %w", err)
	}

	r := mux.NewRouter()
	r.StrictSlash(true).Path("/health").HandlerFunc(svc.healthCheck.Handler)
	svc.server = GetHTTPServer(cfg.BindAddr, r)

	return nil
}

// Start the service
func (svc *Service) Start(ctx context.Context, svcErrors chan error) {
	log.Info(ctx, "starting service")

	// Kafka error logging go-routine
	svc.consumer.LogErrors(ctx)

	// Event Handler for Kafka Consumer
	svc.processor.Consume(
		ctx,
		svc.consumer,
		handler.NewInstanceComplete(
			*svc.cfg,
			svc.cantabularClient,
			svc.datasetAPIClient,
			svc.s3Uploader,
			svc.vaultClient,
			svc.producer,
			svc.generator,
		),
	)

	if !svc.cfg.StopConsumingOnUnhealthy {
		svc.consumer.Start()
	}

	svc.healthCheck.Start(ctx)

	// Run the http server in a new go-routine
	go func() {
		if err := svc.server.ListenAndServe(); err != nil {
			svcErrors <- fmt.Errorf("failure in http listen and serve: %w", err)
		}
	}()
}

// Close gracefully shuts the service down in the required order, with timeout
func (svc *Service) Close(ctx context.Context) error {
	timeout := svc.cfg.GracefulShutdownTimeout
	log.Info(ctx, "commencing graceful shutdown", log.Data{"graceful_shutdown_timeout": timeout})
	ctx, cancel := context.WithTimeout(ctx, timeout)
	hasShutdownError := false

	go func() {
		defer cancel()

		// stop healthcheck, as it depends on everything else
		if svc.healthCheck != nil {
			svc.healthCheck.Stop()
			log.Info(ctx, "stopped health checker")
		}

		// If kafka consumer exists, stop listening to it.
		// This will automatically stop the event consumer loops and no more messages will be processed.
		// The kafka consumer will be closed after the service shuts down.
		if svc.consumer != nil {
			log.Info(ctx, "stoping kafka consumer listener")
			svc.consumer.StopAndWait()
			log.Info(ctx, "stopped kafka consumer listener")
		}

		// stop any incoming requests before closing any outbound connections
		if svc.server != nil {
			if err := svc.server.Shutdown(ctx); err != nil {
				log.Error(ctx, "failed to shutdown http server", err)
				hasShutdownError = true
			}
			log.Info(ctx, "stopped http server")
		}

		// If kafka consumer exists, close it.
		if svc.consumer != nil {
			if err := svc.consumer.Close(ctx); err != nil {
				log.Error(ctx, "error closing kafka consumer", err)
				hasShutdownError = true
			}
			log.Info(ctx, "closed kafka consumer")
		}
	}()

	// wait for shutdown success (via cancel) or failure (timeout)
	<-ctx.Done()

	// timeout expired
	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("shutdown timed out: %w", ctx.Err())
	}

	// other error
	if hasShutdownError {
		return fmt.Errorf("failed to shutdown gracefully")
	}

	log.Info(ctx, "graceful shutdown was successful")
	return nil
}

// registerCheckers adds the checkers for the service clients to the health check object.
func (svc *Service) registerCheckers() error {
	if _, err := svc.healthCheck.AddCheck("Kafka consumer", svc.consumer.Checker); err != nil {
		return fmt.Errorf("error adding check for Kafka consumer: %w", err)
	}

	if _, err := svc.healthCheck.AddCheck("Kafka producer", svc.producer.Checker); err != nil {
		return fmt.Errorf("error adding check for Kafka producer: %w", err)
	}

	// TODO - when Cantabular server is deployed to Production, remove this placeholder and the flag,
	// and always use the real Checker instead: svc.cantabularClient.Checker
	cantabularChecker := svc.cantabularClient.Checker
	if !svc.cfg.CantabularHealthcheckEnabled {
		cantabularChecker = func(ctx context.Context, state *healthcheck.CheckState) error {
			return state.Update(healthcheck.StatusOK, "Cantabular healthcheck placeholder", http.StatusOK)
		}
	}
	checkCantabular, err := svc.healthCheck.AddCheck("Cantabular client", cantabularChecker)
	if err != nil {
		return fmt.Errorf("error adding check for Cantabular client: %w", err)
	}

	checkDataset, err := svc.healthCheck.AddCheck("Dataset API client", svc.datasetAPIClient.Checker)
	if err != nil {
		return fmt.Errorf("error adding check for dataset API client: %w", err)
	}

	checkS3, err := svc.healthCheck.AddCheck("S3 uploader", svc.s3Uploader.Checker)
	if err != nil {
		return fmt.Errorf("error adding check for s3 uploader: %w", err)
	}

	if svc.cfg.StopConsumingOnUnhealthy {
		svc.healthCheck.Subscribe(svc.consumer, checkCantabular, checkDataset, checkS3)
	}

	if !svc.cfg.EncryptionDisabled {
		checkVault, err := svc.healthCheck.AddCheck("Vault", svc.vaultClient.Checker)
		if err != nil {
			return fmt.Errorf("error adding check for vault client: %w", err)
		}
		if svc.cfg.StopConsumingOnUnhealthy {
			svc.healthCheck.Subscribe(svc.consumer, checkVault)
		}
		return nil
	}

	return nil
}
