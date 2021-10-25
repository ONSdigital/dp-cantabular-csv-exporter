package service

import (
	"context"
	"io"
	"net/http"

	"github.com/ONSdigital/dp-api-clients-go/v2/cantabular"
	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/config"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v3"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

//go:generate moq -out mock/server.go -pkg mock . HTTPServer
//go:generate moq -out mock/health_check.go -pkg mock . HealthChecker
//go:generate moq -out mock/cantabular_client.go -pkg mock . CantabularClient
//go:generate moq -out mock/dataset_api_client.go -pkg mock . DatasetAPIClient
//go:generate moq -out mock/s3_uploader.go -pkg mock . S3Uploader
//go:generate moq -out mock/vault.go -pkg mock . VaultClient

// Initialiser defines the methods to initialise external services
type Initialiser interface {
	DoGetHTTPServer(bindAddr string, router http.Handler) HTTPServer
	DoGetHealthCheck(cfg *config.Config, buildTime, gitCommit, version string) (HealthChecker, error)
	DoGetKafkaConsumer(ctx context.Context, cfg *config.Config) (kafka.IConsumerGroup, error)
}

// HTTPServer defines the required methods from the HTTP server
type HTTPServer interface {
	ListenAndServe() error
	Shutdown(ctx context.Context) error
}

// HealthChecker defines the required methods from Healthcheck
type HealthChecker interface {
	Handler(w http.ResponseWriter, req *http.Request)
	Start(ctx context.Context)
	Stop()
	AddAndGetCheck(name string, checker healthcheck.Checker) (check *healthcheck.Check, err error)
	Subscribe(s healthcheck.Subscriber, checks ...*healthcheck.Check)
}

type CantabularClient interface {
	Checker(context.Context, *healthcheck.CheckState) error
	StaticDatasetQuery(context.Context, cantabular.StaticDatasetQueryRequest) (*cantabular.StaticDatasetQuery, error)
}

type DatasetAPIClient interface {
	GetInstance(ctx context.Context, userAuthToken, serviceAuthToken, collectionID, instanceID, ifMatch string) (m dataset.Instance, eTag string, err error)
	PutInstance(ctx context.Context, userAuthToken, serviceAuthToken, collectionID, instanceID string, instanceUpdate dataset.UpdateInstance, ifMatch string) (eTag string, err error)
	Checker(context.Context, *healthcheck.CheckState) error
}

type S3Uploader interface {
	Get(key string) (io.ReadCloser, *int64, error)
	Upload(input *s3manager.UploadInput, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error)
	UploadWithPSK(input *s3manager.UploadInput, psk []byte) (*s3manager.UploadOutput, error)
	BucketName() string
	Checker(context.Context, *healthcheck.CheckState) error
}

type VaultClient interface {
	WriteKey(path, key, value string) error
	Checker(context.Context, *healthcheck.CheckState) error
}

// Generator contains methods for dynamically required strings and tokens
// e.g. UUIDs, PSKs.
type Generator interface {
	NewPSK() ([]byte, error)
}
