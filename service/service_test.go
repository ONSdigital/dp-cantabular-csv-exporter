package service_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/ONSdigital/dp-api-clients-go/v2/filter"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/config"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/service"
	serviceMock "github.com/ONSdigital/dp-cantabular-csv-exporter/service/mock"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v4"
	"github.com/ONSdigital/dp-kafka/v4/kafkatest"

	. "github.com/smartystreets/goconvey/convey"
)

var (
	ctx           = context.Background()
	testBuildTime = "BuildTime"
	testGitCommit = "GitCommit"
	testVersion   = "Version"
	testChecks    = map[string]*healthcheck.Check{
		"Cantabular client":  {},
		"Dataset API client": {},
		"Filter API client":  {},
		"S3 client":          {},
		"Vault":              {},
	}
)

var (
	errKafkaConsumer = fmt.Errorf("kafka consumer error")
	errHealthcheck   = fmt.Errorf("healthCheck error")
	errServer        = fmt.Errorf("HTTP Server error")
	errAddCheck      = fmt.Errorf("healthcheck add check error")
)

func TestInit(t *testing.T) {
	Convey("Having a set of mocked dependencies", t, func() {
		cfg, err := config.Get()
		So(err, ShouldBeNil)

		consumerMock := &kafkatest.IConsumerGroupMock{
			RegisterHandlerFunc: func(ctx context.Context, h kafka.Handler) error {
				return nil
			},
		}
		service.GetKafkaConsumer = func(ctx context.Context, cfg *config.Config) (kafka.IConsumerGroup, error) {
			return consumerMock, nil
		}

		producerMock := &kafkatest.IProducerMock{}
		service.GetKafkaProducer = func(ctx context.Context, cfg *config.Config) (kafka.IProducer, error) {
			return producerMock, nil
		}

		subscribedTo := []*healthcheck.Check{}
		hcMock := &serviceMock.HealthCheckerMock{
			AddAndGetCheckFunc: func(name string, checker healthcheck.Checker) (*healthcheck.Check, error) {
				return testChecks[name], nil
			},
			SubscribeFunc: func(s healthcheck.Subscriber, checks ...*healthcheck.Check) {
				subscribedTo = append(subscribedTo, checks...)
			},
		}
		service.GetHealthCheck = func(cfg *config.Config, buildTime, gitCommit, version string) (service.HealthChecker, error) {
			return hcMock, nil
		}

		serverMock := &serviceMock.HTTPServerMock{}
		service.GetHTTPServer = func(bindAddr string, router http.Handler) service.HTTPServer {
			return serverMock
		}

		cantabularMock := &serviceMock.CantabularClientMock{
			CheckerFunc: func(context.Context, *healthcheck.CheckState) error {
				return nil
			},
		}
		service.GetCantabularClient = func(cfg *config.Config) service.CantabularClient {
			return cantabularMock
		}

		datasetAPIMock := &serviceMock.DatasetAPIClientMock{
			CheckerFunc: func(context.Context, *healthcheck.CheckState) error {
				return nil
			},
		}
		service.GetDatasetAPIClient = func(cfg *config.Config) service.DatasetAPIClient {
			return datasetAPIMock
		}

		filterAPIMock := &serviceMock.FilterAPIClientMock{
			GetDimensionsFunc: func(ctx context.Context, uaToken string, saToken string, cID string, fID string, q *filter.QueryParams) (filter.Dimensions, string, error) {
				return filter.Dimensions{}, "", nil
			},

			GetOutputFunc: func(ctx context.Context, uaToken string, saToken string, dsToken string, cID string, fID string) (filter.Model, error) {
				return filter.Model{}, nil
			},
		}
		service.GetFilterAPIClient = func(cfg *config.Config) service.FilterAPIClient {
			return filterAPIMock
		}

		s3PrivateClientMock := &serviceMock.S3ClientMock{
			CheckerFunc: func(context.Context, *healthcheck.CheckState) error {
				return nil
			},
		}
		s3PublicClientMock := &serviceMock.S3ClientMock{
			CheckerFunc: func(context.Context, *healthcheck.CheckState) error {
				return nil
			},
		}
		service.GetS3Clients = func(cfg *config.Config) (service.S3Client, service.S3Client, error) {
			return s3PrivateClientMock, s3PublicClientMock, nil
		}

		vaultMock := &serviceMock.VaultClientMock{
			CheckerFunc: func(context.Context, *healthcheck.CheckState) error {
				return nil
			},
		}
		service.GetVault = func(cfg *config.Config) (service.VaultClient, error) {
			return vaultMock, nil
		}

		svc := &service.Service{}

		Convey("Given that initialising Kafka consumer returns an error", func() {
			service.GetKafkaConsumer = func(ctx context.Context, cfg *config.Config) (kafka.IConsumerGroup, error) {
				return nil, errKafkaConsumer
			}

			Convey("Then service Init fails with the same error and no further initialisations are attempted", func() {
				err := svc.Init(ctx, cfg, testBuildTime, testGitCommit, testVersion)
				So(errors.Unwrap(err), ShouldResemble, errKafkaConsumer)
				So(svc.Cfg, ShouldResemble, cfg)

				Convey("And no checkers are registered ", func() {
					So(hcMock.AddAndGetCheckCalls(), ShouldHaveLength, 0)
				})
			})
		})

		Convey("Given that initialising healthcheck returns an error", func() {
			service.GetHealthCheck = func(cfg *config.Config, buildTime, gitCommit, version string) (service.HealthChecker, error) {
				return nil, errHealthcheck
			}

			Convey("Then service Init fails with the same error and no further initialisations are attempted", func() {
				err := svc.Init(ctx, cfg, testBuildTime, testGitCommit, testVersion)
				So(errors.Unwrap(err), ShouldResemble, errHealthcheck)
				So(svc.Cfg, ShouldResemble, cfg)
				So(svc.Consumer, ShouldResemble, consumerMock)

				Convey("And no checkers are registered ", func() {
					So(hcMock.AddAndGetCheckCalls(), ShouldHaveLength, 0)
				})
			})
		})

		Convey("Given that Checkers cannot be registered", func() {
			hcMock.AddAndGetCheckFunc = func(name string, checker healthcheck.Checker) (*healthcheck.Check, error) { return nil, errAddCheck }

			Convey("Then service Init fails with the expected error", func() {
				err := svc.Init(ctx, cfg, testBuildTime, testGitCommit, testVersion)
				So(err, ShouldNotBeNil)
				So(errors.Is(err, errAddCheck), ShouldBeTrue)
				So(svc.Cfg, ShouldResemble, cfg)
				So(svc.Consumer, ShouldResemble, consumerMock)

				Convey("And other checkers don't try to register", func() {
					So(hcMock.AddAndGetCheckCalls(), ShouldHaveLength, 1)
				})
			})
		})

		Convey("Given that all dependencies are successfully initialised with encryption enabled", func() {
			cfg.EncryptionDisabled = false

			Convey("Then service Init succeeds, all dependencies are initialised", func() {
				err := svc.Init(ctx, cfg, testBuildTime, testGitCommit, testVersion)
				So(err, ShouldBeNil)
				So(svc.Cfg, ShouldResemble, cfg)
				So(svc.Server, ShouldEqual, serverMock)
				So(svc.HealthCheck, ShouldResemble, hcMock)
				So(svc.Consumer, ShouldResemble, consumerMock)
				So(svc.CantabularClient, ShouldResemble, cantabularMock)
				So(svc.Producer, ShouldResemble, producerMock)
				So(svc.DatasetAPIClient, ShouldResemble, datasetAPIMock)
				So(svc.FilterAPIClient, ShouldResemble, filterAPIMock)
				So(svc.S3PrivateClient, ShouldResemble, s3PrivateClientMock)
				So(svc.S3PublicClient, ShouldResemble, s3PublicClientMock)
				So(svc.VaultClient, ShouldResemble, vaultMock)

				Convey("Then all checks are registered", func() {
					So(hcMock.AddAndGetCheckCalls(), ShouldHaveLength, 9)
					So(hcMock.AddAndGetCheckCalls()[0].Name, ShouldResemble, "Kafka consumer")
					So(hcMock.AddAndGetCheckCalls()[1].Name, ShouldResemble, "Kafka producer")
					So(hcMock.AddAndGetCheckCalls()[2].Name, ShouldResemble, "Cantabular client")
					So(hcMock.AddAndGetCheckCalls()[3].Name, ShouldResemble, "Cantabular API Extension")
					So(hcMock.AddAndGetCheckCalls()[4].Name, ShouldResemble, "Dataset API client")
					So(hcMock.AddAndGetCheckCalls()[5].Name, ShouldResemble, "Filter API client")
					So(hcMock.AddAndGetCheckCalls()[6].Name, ShouldResemble, "S3 private client")
					So(hcMock.AddAndGetCheckCalls()[7].Name, ShouldResemble, "S3 public client")
					So(hcMock.AddAndGetCheckCalls()[8].Name, ShouldResemble, "Vault")
				})

				Convey("Then kafka consumer subscribes to the expected healthcheck checks", func() {
					So(subscribedTo, ShouldHaveLength, 7)
					So(subscribedTo[0], ShouldEqual, testChecks["Cantabular client"])
					So(subscribedTo[1], ShouldEqual, testChecks["Cantabular API Extension"])
					So(subscribedTo[2], ShouldEqual, testChecks["Dataset API client"])
					So(subscribedTo[3], ShouldEqual, testChecks["Filter API client"])
					So(subscribedTo[4], ShouldEqual, testChecks["S3 private client"])
					So(subscribedTo[5], ShouldEqual, testChecks["S3 public client"])
					So(subscribedTo[6], ShouldEqual, testChecks["Vault"])
				})

				Convey("Then the handler is registered to the kafka consumer", func() {
					So(consumerMock.RegisterHandlerCalls(), ShouldHaveLength, 1)
				})
			})
		})

		Convey("Given that all dependencies are successfully initialised with encryption disabled", func() {
			cfg.EncryptionDisabled = true

			Convey("Then service Init succeeds, all dependencies are initialised, except Vault", func() {
				err := svc.Init(ctx, cfg, testBuildTime, testGitCommit, testVersion)
				So(err, ShouldBeNil)
				So(svc.Cfg, ShouldResemble, cfg)
				So(svc.Server, ShouldEqual, serverMock)
				So(svc.HealthCheck, ShouldResemble, hcMock)
				So(svc.Consumer, ShouldResemble, consumerMock)
				So(svc.CantabularClient, ShouldResemble, cantabularMock)
				So(svc.Producer, ShouldResemble, producerMock)
				So(svc.DatasetAPIClient, ShouldResemble, datasetAPIMock)
				So(svc.S3PrivateClient, ShouldResemble, s3PrivateClientMock)
				So(svc.S3PublicClient, ShouldResemble, s3PublicClientMock)

				Convey("Then all checks are registered, except Vault", func() {
					So(hcMock.AddAndGetCheckCalls(), ShouldHaveLength, 8)
					So(hcMock.AddAndGetCheckCalls()[0].Name, ShouldResemble, "Kafka consumer")
					So(hcMock.AddAndGetCheckCalls()[1].Name, ShouldResemble, "Kafka producer")
					So(hcMock.AddAndGetCheckCalls()[2].Name, ShouldResemble, "Cantabular client")
					So(hcMock.AddAndGetCheckCalls()[3].Name, ShouldResemble, "Cantabular API Extension")
					So(hcMock.AddAndGetCheckCalls()[4].Name, ShouldResemble, "Dataset API client")
					So(hcMock.AddAndGetCheckCalls()[5].Name, ShouldResemble, "Filter API client")
					So(hcMock.AddAndGetCheckCalls()[6].Name, ShouldResemble, "S3 private client")
					So(hcMock.AddAndGetCheckCalls()[7].Name, ShouldResemble, "S3 public client")
				})

				Convey("Then kafka consumer subscribes to the expected healthcheck checks", func() {
					So(subscribedTo, ShouldHaveLength, 6)
					So(subscribedTo[0], ShouldEqual, testChecks["Cantabular client"])
					So(subscribedTo[1], ShouldEqual, testChecks["Cantabular API Extension"])
					So(subscribedTo[2], ShouldEqual, testChecks["Dataset API client"])
					So(subscribedTo[3], ShouldEqual, testChecks["Filter API client"])
					So(subscribedTo[4], ShouldEqual, testChecks["S3 private client"])
					So(subscribedTo[5], ShouldEqual, testChecks["S3 public client"])
				})

				Convey("Then the handler is registered to the kafka consumer", func() {
					So(consumerMock.RegisterHandlerCalls(), ShouldHaveLength, 1)
				})
			})
		})

		Convey("Given that all dependencies are successfully initialised and StopConsumingOnUnhealthy is disabled", func() {
			cfg.StopConsumingOnUnhealthy = false

			Convey("Then service Init succeeds, and the kafka consumer does not subscribe to the healthcheck library", func() {
				err := svc.Init(ctx, cfg, testBuildTime, testGitCommit, testVersion)
				So(err, ShouldBeNil)
				So(hcMock.SubscribeCalls(), ShouldHaveLength, 0)
			})
		})
	})
}

func TestStart(t *testing.T) {
	Convey("Having a correctly initialised Service with mocked dependencies", t, func() {
		cfg, err := config.Get()
		So(err, ShouldBeNil)

		consumerMock := &kafkatest.IConsumerGroupMock{
			LogErrorsFunc: func(ctx context.Context) {},
		}

		producerMock := &kafkatest.IProducerMock{
			LogErrorsFunc: func(ctx context.Context) {},
		}

		hcMock := &serviceMock.HealthCheckerMock{
			StartFunc: func(ctx context.Context) {},
		}

		serverWg := &sync.WaitGroup{}
		serverMock := &serviceMock.HTTPServerMock{}

		svc := &service.Service{
			Cfg:         cfg,
			Server:      serverMock,
			HealthCheck: hcMock,
			Consumer:    consumerMock,
			Producer:    producerMock,
		}

		Convey("When a service with a successful HTTP server is started", func() {
			cfg.StopConsumingOnUnhealthy = true
			serverMock.ListenAndServeFunc = func() error {
				serverWg.Done()
				return nil
			}
			serverWg.Add(1)
			err := svc.Start(ctx, make(chan error, 1))
			So(err, ShouldBeNil)

			Convey("Then healthcheck is started and HTTP server starts listening", func() {
				So(len(hcMock.StartCalls()), ShouldEqual, 1)
				serverWg.Wait() // Wait for HTTP server go-routine to finish
				So(len(serverMock.ListenAndServeCalls()), ShouldEqual, 1)
			})
		})

		Convey("When a service is started with StopConsumingOnUnhealthy disabled", func() {
			cfg.StopConsumingOnUnhealthy = false
			consumerMock.StartFunc = func() error { return nil }
			serverMock.ListenAndServeFunc = func() error { return nil }
			err := svc.Start(ctx, make(chan error, 1))
			So(err, ShouldBeNil)

			Convey("Then the kafka consumer is manually started", func() {
				So(consumerMock.StartCalls(), ShouldHaveLength, 1)
			})
		})

		Convey("When a service with a failing HTTP server is started", func() {
			cfg.StopConsumingOnUnhealthy = true
			serverMock.ListenAndServeFunc = func() error {
				serverWg.Done()
				return errServer
			}
			errChan := make(chan error, 1)
			serverWg.Add(1)
			err := svc.Start(ctx, errChan)
			So(err, ShouldBeNil)

			Convey("Then HTTP server errors are reported to the provided errors channel", func() {
				rxErr := <-errChan
				So(rxErr.Error(), ShouldResemble, fmt.Sprintf("failure in http listen and serve: %s", errServer.Error()))
			})
		})
	})
}

func TestClose(t *testing.T) {
	Convey("Having a correctly initialised service", t, func() {
		cfg, err := config.Get()
		So(err, ShouldBeNil)

		hcStopped := false

		// kafka consumer mock
		consumerMock := &kafkatest.IConsumerGroupMock{
			StopAndWaitFunc: func() error { return nil },
			CloseFunc:       func(ctx context.Context, optFuncs ...kafka.OptFunc) error { return nil },
		}

		// healthcheck Stop does not depend on any other service being closed/stopped
		hcMock := &serviceMock.HealthCheckerMock{
			StopFunc: func() { hcStopped = true },
		}

		// server Shutdown will fail if healthcheck is not stopped
		serverMock := &serviceMock.HTTPServerMock{
			ShutdownFunc: func(ctx context.Context) error {
				if !hcStopped {
					return fmt.Errorf("server stopped before healthcheck")
				}
				return nil
			},
		}

		svc := &service.Service{
			Cfg:         cfg,
			Server:      serverMock,
			HealthCheck: hcMock,
			Consumer:    consumerMock,
		}

		Convey("Closing the service results in all the dependencies being closed in the expected order", func() {
			err := svc.Close(context.Background())
			So(err, ShouldBeNil)
			So(consumerMock.StopAndWaitCalls(), ShouldHaveLength, 1)
			So(hcMock.StopCalls(), ShouldHaveLength, 1)
			So(consumerMock.CloseCalls(), ShouldHaveLength, 1)
			So(serverMock.ShutdownCalls(), ShouldHaveLength, 1)
		})

		Convey("If services fail to stop, the Close operation tries to close all dependencies and returns an error", func() {
			consumerMock.StopAndWaitFunc = func() error { return nil }
			consumerMock.CloseFunc = func(ctx context.Context, optFuncs ...kafka.OptFunc) error {
				return errKafkaConsumer
			}
			serverMock.ShutdownFunc = func(ctx context.Context) error {
				return errServer
			}

			err = svc.Close(context.Background())
			So(err, ShouldNotBeNil)
			So(consumerMock.StopAndWaitCalls(), ShouldHaveLength, 1)
			So(consumerMock.CloseCalls(), ShouldHaveLength, 1)
			So(hcMock.StopCalls(), ShouldHaveLength, 1)
			So(serverMock.ShutdownCalls(), ShouldHaveLength, 1)
		})
	})
}
