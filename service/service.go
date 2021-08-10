package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/ONSdigital/dp-cantabular-csv-exporter/config"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/handler"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/log.go/log"
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
	if svc.vaultClient, err = GetVault(cfg); err != nil {
		return fmt.Errorf("failed to initialise vault client: %w", err)
	}

	svc.cantabularClient = GetCantabularClient(cfg)
	svc.datasetAPIClient = GetDatasetAPIClient(cfg)

	svc.processor = GetProcessor(cfg)

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
	log.Event(ctx, "starting service", log.INFO)

	// Kafka error logging go-routine
	svc.consumer.Channels().LogErrors(ctx, "kafka consumer")

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
		),
	)

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
	log.Event(ctx, "commencing graceful shutdown", log.Data{"graceful_shutdown_timeout": timeout}, log.INFO)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	hasShutdownError := false

	go func() {
		defer cancel()

		// stop healthcheck, as it depends on everything else
		if svc.healthCheck != nil {
			svc.healthCheck.Stop()
			log.Event(ctx, "stopped health checker", log.INFO)
		}

		// If kafka consumer exists, stop listening to it.
		// This will automatically stop the event consumer loops and no more messages will be processed.
		// The kafka consumer will be closed after the service shuts down.
		if svc.consumer != nil {
			if err := svc.consumer.StopListeningToConsumer(ctx); err != nil {
				log.Event(ctx, "error stopping kafka consumer listener", log.ERROR, log.Error(err))
				hasShutdownError = true
			}
			log.Event(ctx, "stopped kafka consumer listener", log.INFO)
		}

		// stop any incoming requests before closing any outbound connections
		if svc.server != nil {
			if err := svc.server.Shutdown(ctx); err != nil {
				log.Event(ctx, "failed to shutdown http server", log.Error(err), log.ERROR)
				hasShutdownError = true
			}
			log.Event(ctx, "stopped http server", log.INFO)
		}

		// If kafka consumer exists, close it.
		if svc.consumer != nil {
			if err := svc.consumer.Close(ctx); err != nil {
				log.Event(ctx, "error closing kafka consumer", log.ERROR, log.Error(err))
				hasShutdownError = true
			}
			log.Event(ctx, "closed kafka consumer", log.INFO)
		}
	}()

	// wait for shutdown success (via cancel) or failure (timeout)
	<-ctx.Done()

	// timeout expired
	if ctx.Err() == context.DeadlineExceeded {
		log.Event(ctx, "shutdown timed out", log.ERROR, log.Error(ctx.Err()))
		return ctx.Err()
	}

	// other error
	if hasShutdownError {
		err := fmt.Errorf("failed to shutdown gracefully")
		log.Event(ctx, "failed to shutdown gracefully ", log.ERROR, log.Error(err))
		return err
	}

	log.Event(ctx, "graceful shutdown was successful", log.INFO)
	return nil
}

// registerCheckers adds the checkers for the service clients to the health check object.
func (svc *Service) registerCheckers() error {
	if err := svc.healthCheck.AddCheck("Kafka consumer", svc.consumer.Checker); err != nil {
		return fmt.Errorf("error adding check for Kafka: %w", err)
	}

	if err := svc.healthCheck.AddCheck("Cantabular client", svc.cantabularClient.Checker); err != nil {
		return fmt.Errorf("error adding check for cantabular client: %w", err)
	}

	if err := svc.healthCheck.AddCheck("Dataset API client", svc.datasetAPIClient.Checker); err != nil {
		return fmt.Errorf("error adding check for dataset API client: %w", err)
	}

	if err := svc.healthCheck.AddCheck("S3 uploader", svc.s3Uploader.Checker); err != nil {
		return fmt.Errorf("error adding check for s3 uploader: %w", err)
	}

	if err := svc.healthCheck.AddCheck("Vault", svc.vaultClient.Checker); err != nil {
		return fmt.Errorf("error adding check for vault client: %w", err)
	}

	return nil
}
