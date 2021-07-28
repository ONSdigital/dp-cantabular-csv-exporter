package service

import (
	"context"
	"fmt"
	"net/http"

	"github.com/ONSdigital/dp-api-clients-go/v2/cantabular"
	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/config"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/event"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	dpkafka "github.com/ONSdigital/dp-kafka/v2"
	dphttp "github.com/ONSdigital/dp-net/http"
	dps3 "github.com/ONSdigital/dp-s3"
)

// GetHTTPServer creates an http server and sets the Server flag to true
var GetHTTPServer = func(bindAddr string, router http.Handler) HTTPServer {
	s := dphttp.NewServer(bindAddr, router)
	s.HandleOSSignals = false
	return s
}

// GetKafkaConsumer creates a Kafka consumer and sets the consumer flag to true
var GetKafkaConsumer = func(ctx context.Context, cfg *config.Config) (dpkafka.IConsumerGroup, error) {
	cgChannels := dpkafka.CreateConsumerGroupChannels(1)

	kafkaOffset := dpkafka.OffsetNewest
	if cfg.KafkaOffsetOldest {
		kafkaOffset = dpkafka.OffsetOldest
	}

	consumer, err := dpkafka.NewConsumerGroup(
		ctx,
		cfg.KafkaAddr,
		cfg.InstanceCompleteTopic,
		cfg.InstanceCompleteGroup,
		cgChannels,
		&dpkafka.ConsumerGroupConfig{
			KafkaVersion: &cfg.KafkaVersion,
			Offset:       &kafkaOffset,
		},
	)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

// GetCantabularClient gets and initialises the Cantabular Client
var GetCantabularClient = func(cfg *config.Config) CantabularClient {
	return cantabular.NewClient(
		dphttp.NewClient(),
		cantabular.Config{
			Host: cfg.CantabularURL,
		},
	)
}

// GetDatasetAPIClient gets and initialises the DatasetAPI Client
var GetDatasetAPIClient = func(cfg *config.Config) DatasetAPIClient {
	return dataset.NewAPIClient(cfg.DatasetAPIURL)
}

// GetS3Client gets and initialises the S3 Client
var GetS3Client = func(cfg *config.Config) (S3Client, error) {
	s3Client, err := dps3.NewClient(cfg.AWSRegion, cfg.UploadBucketName, true)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 Client: %w", err)
	}
	return s3Client, nil
}

// GetProcessor gets and initialises the event Processor
var GetProcessor = func(cfg *config.Config) Processor {
	return event.NewProcessor(*cfg)
}

// GetHealthCheck creates a healthcheck with versionInfo and sets teh HealthCheck flag to true
var GetHealthCheck = func(cfg *config.Config, buildTime, gitCommit, version string) (HealthChecker, error) {
	versionInfo, err := healthcheck.NewVersionInfo(buildTime, gitCommit, version)
	if err != nil {
		return nil, err
	}

	hc := healthcheck.New(versionInfo, cfg.HealthCheckCriticalTimeout, cfg.HealthCheckInterval)
	return &hc, nil
}
