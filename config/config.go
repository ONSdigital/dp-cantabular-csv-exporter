package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

// Config represents service configuration for dp-cantabular-csv-exporter
type Config struct {
	BindAddr                   string        `envconfig:"BIND_ADDR"`
	GracefulShutdownTimeout    time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	HealthCheckInterval        time.Duration `envconfig:"HEALTHCHECK_INTERVAL"`
	HealthCheckCriticalTimeout time.Duration `envconfig:"HEALTHCHECK_CRITICAL_TIMEOUT"`
	DefaultRequestTimeout      time.Duration `envconfig:"DEFAULT_REQUEST_TIMEOUT"`
	KafkaAddr                  []string      `envconfig:"KAFKA_ADDR"                     json:"-"`
	KafkaVersion               string        `envconfig:"KAFKA_VERSION"`
	KafkaOffsetOldest          bool          `envconfig:"KAFKA_OFFSET_OLDEST"`
	KafkaNumWorkers            int           `envconfig:"KAFKA_NUM_WORKERS"`
	InstanceCompleteGroup      string        `envconfig:"INSTANCE_COMPLETE_GROUP"`
	InstanceCompleteTopic      string        `envconfig:"INSTANCE_COMPLETE_TOPIC"`
	CommonOutputCreatedTopic   string        `envconfig:"COMMON_OUTPUT_CREATED_TOPIC`
	OutputFilePath             string        `envconfig:"OUTPUT_FILE_PATH"`
	ServiceAuthToken           string        `envconfig:"SERVICE_AUTH_TOKEN"         json:"-"`
	CantabularURL              string        `envconfig:"CANTABULAR_URL"`
	CantabularExtURL           string        `envconfig:"CANTABULAR_EXT_API_URL"`
	DatasetAPIURL              string        `envconfig:"DATASET_API_URL"`
	AWSRegion                  string        `envconfig:"AWS_REGION"`
	UploadBucketName           string        `envconfig:"UPLOAD_BUCKET_NAME"`
	VaultToken                 string        `envconfig:"VAULT_TOKEN"                   json:"-"`
	VaultAddress               string        `envconfig:"VAULT_ADDR"`
	VaultPath                  string        `envconfig:"VAULT_PATH"`
}

var cfg *Config

// Get returns the default config with any modifications through environment
// variables
func Get() (*Config, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg = &Config{
		BindAddr:                   ":26300",
		GracefulShutdownTimeout:    5 * time.Second,
		HealthCheckInterval:        30 * time.Second,
		HealthCheckCriticalTimeout: 90 * time.Second,
		DefaultRequestTimeout:      10 * time.Second,
		KafkaAddr:                  []string{"localhost:9092"},
		KafkaVersion:               "1.0.2",
		KafkaOffsetOldest:          true,
		KafkaNumWorkers:            1,
		InstanceCompleteGroup:      "dp-cantabular-csv-exporter",
		InstanceCompleteTopic:      "cantabular-dataset-instance-complete",
		CommonOutputCreatedTopic:   "common-output-created",
		OutputFilePath:             "/tmp/helloworld.txt",
		ServiceAuthToken:           "",
		CantabularURL:              "http://localhost:8491",
		CantabularExtURL:           "http://localhost:8492",
		DatasetAPIURL:              "http://localhost:22000",
		AWSRegion:                  "eu-west-1",
		UploadBucketName:           "dp-cantabular-csv-exporter",
		VaultPath:                  "secret/shared/psk",
		VaultAddress:               "http://localhost:8200",
		VaultToken:                 "",
	}

	return cfg, envconfig.Process("", cfg)
}
