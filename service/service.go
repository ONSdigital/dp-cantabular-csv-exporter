package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/ONSdigital/dp-cantabular-csv-exporter/config"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/handler"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v3"
	"github.com/ONSdigital/log.go/v2/log"

	"github.com/gorilla/mux"
)

// Service contains all the configs, server and clients to run the event handler service
type Service struct {
	Cfg              *config.Config
	Server           HTTPServer
	HealthCheck      HealthChecker
	Consumer         kafka.IConsumerGroup
	Producer         kafka.IProducer
	DatasetAPIClient DatasetAPIClient
	CantabularClient CantabularClient
	S3PrivateClient  S3Client
	S3PublicClient   S3Client
	VaultClient      VaultClient
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

	svc.Cfg = cfg

	if svc.Consumer, err = GetKafkaConsumer(ctx, cfg); err != nil {
		return fmt.Errorf("failed to create kafka consumer: %w", err)
	}
	if svc.Producer, err = GetKafkaProducer(ctx, cfg); err != nil {
		return fmt.Errorf("failed to create kafka producer: %w", err)
	}
	if svc.S3PrivateClient, svc.S3PublicClient, err = GetS3Clients(cfg); err != nil {
		return fmt.Errorf("failed to initialise s3 clients: %w", err)
	}
	if !cfg.EncryptionDisabled {
		if svc.VaultClient, err = GetVault(cfg); err != nil {
			return fmt.Errorf("failed to initialise vault client: %w", err)
		}
	}

	svc.CantabularClient = GetCantabularClient(cfg)
	svc.DatasetAPIClient = GetDatasetAPIClient(cfg)
	svc.generator = GetGenerator()

	h := handler.NewInstanceComplete(
		*svc.Cfg,
		svc.CantabularClient,
		svc.DatasetAPIClient,
		svc.S3PrivateClient,
		svc.S3PublicClient,
		svc.VaultClient,
		svc.Producer,
		svc.generator,
	)
	if err := svc.Consumer.RegisterHandler(ctx, h.Handle); err != nil {
		return fmt.Errorf("could not register kafka handler: %w", err)
	}

	// Get HealthCheck
	if svc.HealthCheck, err = GetHealthCheck(cfg, buildTime, gitCommit, version); err != nil {
		return fmt.Errorf("could not instantiate healthcheck: %w", err)
	}

	if err := svc.registerCheckers(); err != nil {
		return fmt.Errorf("error initialising checkers: %w", err)
	}

	r := mux.NewRouter()
	r.StrictSlash(true).Path("/health").HandlerFunc(svc.HealthCheck.Handler)
	svc.Server = GetHTTPServer(cfg.BindAddr, r)

	return nil
}

// Start the service
func (svc *Service) Start(ctx context.Context, svcErrors chan error) error {
	log.Info(ctx, "starting service")

	// Kafka error logging go-routine
	svc.Consumer.LogErrors(ctx)
	svc.Producer.LogErrors(ctx)

	// If start/stop on health updates is disabled, start consuming as soon as possible
	if !svc.Cfg.StopConsumingOnUnhealthy {
		if err := svc.Consumer.Start(); err != nil {
			return fmt.Errorf("consumer failed to start: %w", err)
		}
	}

	// Always start healthcheck.
	// If start/stop on health updates is enabled,
	// the consumer will start consuming on the first healthy update
	svc.HealthCheck.Start(ctx)

	// Run the http server in a new go-routine
	go func() {
		if err := svc.Server.ListenAndServe(); err != nil {
			svcErrors <- fmt.Errorf("failure in http listen and serve: %w", err)
		}
	}()

	return nil
}

// Close gracefully shuts the service down in the required order, with timeout
func (svc *Service) Close(ctx context.Context) error {
	timeout := svc.Cfg.GracefulShutdownTimeout
	log.Info(ctx, "commencing graceful shutdown", log.Data{"graceful_shutdown_timeout": timeout})
	ctx, cancel := context.WithTimeout(ctx, timeout)
	hasShutdownError := false

	go func() {
		defer cancel()

		// stop healthcheck, as it depends on everything else
		if svc.HealthCheck != nil {
			svc.HealthCheck.Stop()
			log.Info(ctx, "stopped health checker")
		}

		// If kafka consumer exists, stop listening to it.
		// This will automatically stop the event consumer loops and no more messages will be processed.
		// The kafka consumer will be closed after the service shuts down.
		if svc.Consumer != nil {
			if err := svc.Consumer.StopAndWait(); err != nil {
				log.Error(ctx, "failed to stop kafka consumer", err)
				hasShutdownError = true
			} else {
				log.Info(ctx, "stopped kafka consumer")
			}
		}

		// stop any incoming requests before closing any outbound connections
		if svc.Server != nil {
			if err := svc.Server.Shutdown(ctx); err != nil {
				log.Error(ctx, "failed to shutdown http server", err)
				hasShutdownError = true
			} else {
				log.Info(ctx, "stopped http server")
			}
		}

		// If kafka producer exists, close it.
		if svc.Producer != nil {
			if err := svc.Producer.Close(ctx); err != nil {
				log.Error(ctx, "error closing kafka producer", err)
				hasShutdownError = true
			} else {
				log.Info(ctx, "closed kafka producer")
			}
		}

		// If kafka consumer exists, close it.
		if svc.Consumer != nil {
			if err := svc.Consumer.Close(ctx); err != nil {
				log.Error(ctx, "error closing kafka consumer", err)
				hasShutdownError = true
			} else {
				log.Info(ctx, "closed kafka consumer")
			}
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
	if _, err := svc.HealthCheck.AddAndGetCheck("Kafka consumer", svc.Consumer.Checker); err != nil {
		return fmt.Errorf("error adding check for Kafka consumer: %w", err)
	}

	if _, err := svc.HealthCheck.AddAndGetCheck("Kafka producer", svc.Producer.Checker); err != nil {
		return fmt.Errorf("error adding check for Kafka producer: %w", err)
	}

	// TODO - when Cantabular server is deployed to Production, remove this placeholder and the flag,
	// and always use the real Checker instead: svc.cantabularClient.Checker
	cantabularChecker := svc.CantabularClient.Checker
	cantabularAPIExtChecker := svc.CantabularClient.CheckerAPIExt
	if !svc.Cfg.CantabularHealthcheckEnabled {
		cantabularChecker = func(ctx context.Context, state *healthcheck.CheckState) error {
			return state.Update(healthcheck.StatusOK, "Cantabular healthcheck placeholder", http.StatusOK)
		}
		cantabularAPIExtChecker = func(ctx context.Context, state *healthcheck.CheckState) error {
			return state.Update(healthcheck.StatusOK, "Cantabular APIExt healthcheck placeholder", http.StatusOK)
		}
	}

	checkCantabular, err := svc.HealthCheck.AddAndGetCheck("Cantabular client", cantabularChecker)
	if err != nil {
		return fmt.Errorf("error adding check for Cantabular client: %w", err)
	}

	checkCantabularAPIExt, err := svc.HealthCheck.AddAndGetCheck("Cantabular API Extension", cantabularAPIExtChecker)
	if err != nil {
		return fmt.Errorf("error adding check for Cantabular api extension: %w", err)
	}

	checkDataset, err := svc.HealthCheck.AddAndGetCheck("Dataset API client", svc.DatasetAPIClient.Checker)
	if err != nil {
		return fmt.Errorf("error adding check for dataset API client: %w", err)
	}

	checkS3Private, err := svc.HealthCheck.AddAndGetCheck("S3 private client", svc.S3PrivateClient.Checker)
	if err != nil {
		return fmt.Errorf("error adding check for s3 private client: %w", err)
	}

	checkS3Public, err := svc.HealthCheck.AddAndGetCheck("S3 public client", svc.S3PublicClient.Checker)
	if err != nil {
		return fmt.Errorf("error adding check for s3 public client: %w", err)
	}

	if svc.Cfg.StopConsumingOnUnhealthy {
		svc.HealthCheck.Subscribe(svc.Consumer, checkCantabular, checkCantabularAPIExt, checkDataset, checkS3Private, checkS3Public)
	}

	if !svc.Cfg.EncryptionDisabled {
		checkVault, err := svc.HealthCheck.AddAndGetCheck("Vault", svc.VaultClient.Checker)
		if err != nil {
			return fmt.Errorf("error adding check for vault client: %w", err)
		}
		if svc.Cfg.StopConsumingOnUnhealthy {
			svc.HealthCheck.Subscribe(svc.Consumer, checkVault)
		}
		return nil
	}

	return nil
}
