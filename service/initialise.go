package service

import (
	"context"
	"fmt"
	"net/http"

	"github.com/ONSdigital/dp-api-clients-go/v3/cantabular"
	"github.com/ONSdigital/dp-api-clients-go/v3/dataset"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/config"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/generator"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/handler"
	"github.com/ONSdigital/dp-healthcheck/v2/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v3"
	dphttp "github.com/ONSdigital/dp-net/http"
	dps3 "github.com/ONSdigital/dp-s3/v2"
	vault "github.com/ONSdigital/dp-vault/v2"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

const VaultRetries = 3

// GetHTTPServer creates an http server and sets the Server
var GetHTTPServer = func(bindAddr string, router http.Handler) HTTPServer {
	s := dphttp.NewServer(bindAddr, router)
	s.HandleOSSignals = false
	return s
}

// GetKafkaConsumer creates a Kafka consumer
var GetKafkaConsumer = func(ctx context.Context, cfg *config.Config) (kafka.IConsumerGroup, error) {
	kafkaOffset := kafka.OffsetNewest
	if cfg.KafkaConfig.OffsetOldest {
		kafkaOffset = kafka.OffsetOldest
	}
	cgConfig := &kafka.ConsumerGroupConfig{
		BrokerAddrs:  cfg.KafkaConfig.Addr,
		Topic:        cfg.KafkaConfig.InstanceCompleteTopic,
		GroupName:    cfg.KafkaConfig.InstanceCompleteGroup,
		KafkaVersion: &cfg.KafkaConfig.Version,
		Offset:       &kafkaOffset,
	}
	if cfg.KafkaConfig.SecProtocol == config.KafkaTLSProtocolFlag {
		cgConfig.SecurityConfig = kafka.GetSecurityConfig(
			cfg.KafkaConfig.SecCACerts,
			cfg.KafkaConfig.SecClientCert,
			cfg.KafkaConfig.SecClientKey,
			cfg.KafkaConfig.SecSkipVerify,
		)
	}
	return kafka.NewConsumerGroup(ctx, cgConfig)
}

// GetKafkaProducer creates a Kafka producer
var GetKafkaProducer = func(ctx context.Context, cfg *config.Config) (kafka.IProducer, error) {
	pConfig := &kafka.ProducerConfig{
		BrokerAddrs:     cfg.KafkaConfig.Addr,
		Topic:           cfg.KafkaConfig.CommonOutputCreatedTopic,
		KafkaVersion:    &cfg.KafkaConfig.Version,
		MaxMessageBytes: &cfg.KafkaConfig.MaxBytes,
	}
	if cfg.KafkaConfig.SecProtocol == config.KafkaTLSProtocolFlag {
		pConfig.SecurityConfig = kafka.GetSecurityConfig(
			cfg.KafkaConfig.SecCACerts,
			cfg.KafkaConfig.SecClientCert,
			cfg.KafkaConfig.SecClientKey,
			cfg.KafkaConfig.SecSkipVerify,
		)
	}
	return kafka.NewProducer(ctx, pConfig)
}

// GetCantabularClient gets and initialises the Cantabular Client
var GetCantabularClient = func(cfg *config.Config) CantabularClient {
	return cantabular.NewClient(
		cantabular.Config{
			Host:           cfg.CantabularURL,
			ExtApiHost:     cfg.CantabularExtURL,
			GraphQLTimeout: cfg.DefaultRequestTimeout,
		},
		dphttp.NewClient(),
		nil,
	)
}

// GetDatasetAPIClient gets and initialises the DatasetAPI Client
var GetDatasetAPIClient = func(cfg *config.Config) DatasetAPIClient {
	return dataset.NewAPIClient(cfg.DatasetAPIURL)
}

// GetS3Uploader creates an S3 Uploader
var GetS3Uploader = func(cfg *config.Config) (S3Uploader, error) {
	if cfg.LocalObjectStore != "" {
		s3Config := &aws.Config{
			Credentials:      credentials.NewStaticCredentials(cfg.MinioAccessKey, cfg.MinioSecretKey, ""),
			Endpoint:         aws.String(cfg.LocalObjectStore),
			Region:           aws.String(cfg.AWSRegion),
			DisableSSL:       aws.Bool(true),
			S3ForcePathStyle: aws.Bool(true),
		}

		s, err := session.NewSession(s3Config)
		if err != nil {
			return nil, fmt.Errorf("failed to create aws session: %w", err)
		}
		return dps3.NewUploaderWithSession(cfg.UploadBucketName, s), nil
	}

	uploader, err := dps3.NewUploader(cfg.AWSRegion, cfg.UploadBucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 Client: %w", err)
	}

	return uploader, nil
}

// GetVault creates a VaultClient
var GetVault = func(cfg *config.Config) (VaultClient, error) {
	return vault.CreateClient(cfg.VaultToken, cfg.VaultAddress, VaultRetries)
}

// GetHandler gets and initialises the kafka consumer-group Handler
var GetHandler = func(cfg config.Config, c CantabularClient, d DatasetAPIClient, s3 S3Uploader, v VaultClient, p kafka.IProducer, g Generator) Handler {
	return handler.NewInstanceComplete(cfg, c, d, s3, v, p, g)
}

// GetHealthCheck creates a healthcheck with versionInfo
var GetHealthCheck = func(cfg *config.Config, buildTime, gitCommit, version string) (HealthChecker, error) {
	versionInfo, err := healthcheck.NewVersionInfo(buildTime, gitCommit, version)
	if err != nil {
		return nil, fmt.Errorf("failed to get version info: %w", err)
	}

	hc := healthcheck.New(
		versionInfo,
		cfg.HealthCheckCriticalTimeout,
		cfg.HealthCheckInterval,
	)
	return &hc, nil
}

var GetGenerator = func() Generator {
	return generator.New()
}
