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
	KafkaAddr                  []string      `envconfig:"KAFKA_ADDR"                     json:"-"`
	KafkaVersion               string        `envconfig:"KAFKA_VERSION"`
	KafkaOffsetOldest          bool          `envconfig:"KAFKA_OFFSET_OLDEST"`
	KafkaNumWorkers            int           `envconfig:"KAFKA_NUM_WORKERS"`
	InstanceCompleteGroup      string        `envconfig:"INSTANCE_COMPLETE_GROUP"`
	InstanceCompleteTopic      string        `envconfig:"INSTANCE_COMPLETE_TOPIC"`
	OutputFilePath             string        `envconfig:"OUTPUT_FILE_PATH"`
	ServiceAuthToken           string        `envconfig:"SERVICE_AUTH_TOKEN"         json:"-"`
	CantabularURL              string        `envconfig:"CANTABULAR_URL"`
	DatasetAPIURL              string        `envconfig:"DATASET_API_URL"`
}

var cfg *Config

// Get returns the default config with any modifications through environment
// variables
func Get() (*Config, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg = &Config{
		BindAddr:                   "localhost:26300",
		GracefulShutdownTimeout:    5 * time.Second,
		HealthCheckInterval:        30 * time.Second,
		HealthCheckCriticalTimeout: 90 * time.Second,
		KafkaAddr:                  []string{"localhost:9092"},
		KafkaVersion:               "1.0.2",
		KafkaOffsetOldest:          true,
		KafkaNumWorkers:            1,
		InstanceCompleteGroup:      "dp-cantabular-csv-exporter",
		InstanceCompleteTopic:      "cantabular-dataset-instance-complete",
		OutputFilePath:             "/tmp/helloworld.txt",
		ServiceAuthToken:           "",
		CantabularURL:              "localhost:8491",
		DatasetAPIURL:              "localhost:22000",
	}

	return cfg, envconfig.Process("", cfg)
}
