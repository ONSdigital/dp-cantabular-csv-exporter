package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

// KafkaTLSProtocolFlag informs service to use TLS protocol for kafka
const KafkaTLSProtocolFlag = "TLS"

// Config represents service configuration for dp-cantabular-csv-exporter
type Config struct {
	BindAddr                     string        `envconfig:"BIND_ADDR"`
	GracefulShutdownTimeout      time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	HealthCheckInterval          time.Duration `envconfig:"HEALTHCHECK_INTERVAL"`
	HealthCheckCriticalTimeout   time.Duration `envconfig:"HEALTHCHECK_CRITICAL_TIMEOUT"`
	DefaultRequestTimeout        time.Duration `envconfig:"DEFAULT_REQUEST_TIMEOUT"`
	ServiceAuthToken             string        `envconfig:"SERVICE_AUTH_TOKEN"         json:"-"`
	CantabularURL                string        `envconfig:"CANTABULAR_URL"`
	CantabularExtURL             string        `envconfig:"CANTABULAR_EXT_API_URL"`
	DatasetAPIURL                string        `envconfig:"DATASET_API_URL"`
	DownloadServiceURL           string        `envconfig:"DOWNLOAD_SERVICE_URL"`
	CantabularHealthcheckEnabled bool          `envconfig:"CANTABULAR_HEALTHCHECK_ENABLED"`
	AWSRegion                    string        `envconfig:"AWS_REGION"`
	UploadBucketName             string        `envconfig:"UPLOAD_BUCKET_NAME"`
	LocalObjectStore             string        `envconfig:"LOCAL_OBJECT_STORE"`
	MinioAccessKey               string        `envconfig:"MINIO_ACCESS_KEY"`
	MinioSecretKey               string        `envconfig:"MINIO_SECRET_KEY"`
	VaultToken                   string        `envconfig:"VAULT_TOKEN"                   json:"-"`
	VaultAddress                 string        `envconfig:"VAULT_ADDR"`
	VaultPath                    string        `envconfig:"VAULT_PATH"`
	ComponentTestUseLogFile      bool          `envconfig:"COMPONENT_TEST_USE_LOG_FILE"`
	EncryptionDisabled           bool          `envconfig:"ENCRYPTION_DISABLED"`
	StopConsumingOnUnhealthy     bool          `envconfig:"STOP_CONSUMING_ON_UNHEALTHY"`
	KafkaConfig                  KafkaConfig
}

// KafkaConfig contains the config required to connect to Kafka
type KafkaConfig struct {
	Addr                  []string `envconfig:"KAFKA_ADDR"                            json:"-"`
	Version               string   `envconfig:"KAFKA_VERSION"`
	OffsetOldest          bool     `envconfig:"KAFKA_OFFSET_OLDEST"`
	NumWorkers            int      `envconfig:"KAFKA_NUM_WORKERS"`
	MaxBytes              int      `envconfig:"KAFKA_MAX_BYTES"`
	SecProtocol           string   `envconfig:"KAFKA_SEC_PROTO"`
	SecCACerts            string   `envconfig:"KAFKA_SEC_CA_CERTS"`
	SecClientKey          string   `envconfig:"KAFKA_SEC_CLIENT_KEY"                  json:"-"`
	SecClientCert         string   `envconfig:"KAFKA_SEC_CLIENT_CERT"`
	SecSkipVerify         bool     `envconfig:"KAFKA_SEC_SKIP_VERIFY"`
	InstanceCompleteGroup string   `envconfig:"INSTANCE_COMPLETE_GROUP"`
	InstanceCompleteTopic string   `envconfig:"INSTANCE_COMPLETE_TOPIC"`
	CsvCreatedTopic       string   `envconfig:"COMMON_OUTPUT_CREATED_TOPIC"` // TODO rename env var
}

var cfg *Config

// Get returns the default config with any modifications through environment
// variables
func Get() (*Config, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg = &Config{
		BindAddr:                     ":26300",
		GracefulShutdownTimeout:      5 * time.Second,
		HealthCheckInterval:          30 * time.Second,
		HealthCheckCriticalTimeout:   90 * time.Second,
		DefaultRequestTimeout:        10 * time.Second,
		ServiceAuthToken:             "",
		CantabularURL:                "http://localhost:8491",
		CantabularExtURL:             "http://localhost:8492",
		DatasetAPIURL:                "http://localhost:22000",
		DownloadServiceURL:           "http://localhost:23600",
		CantabularHealthcheckEnabled: false,
		AWSRegion:                    "eu-west-1",
		UploadBucketName:             "dp-cantabular-csv-exporter",
		LocalObjectStore:             "",
		MinioAccessKey:               "",
		MinioSecretKey:               "",
		VaultPath:                    "secret/shared/psk",
		VaultAddress:                 "http://localhost:8200",
		VaultToken:                   "",
		ComponentTestUseLogFile:      false,
		EncryptionDisabled:           false,
		StopConsumingOnUnhealthy:     true,
		KafkaConfig: KafkaConfig{
			Addr:                  []string{"localhost:9092"},
			Version:               "1.0.2",
			OffsetOldest:          true,
			NumWorkers:            1,
			MaxBytes:              2000000,
			SecProtocol:           "",
			SecCACerts:            "",
			SecClientKey:          "",
			SecClientCert:         "",
			SecSkipVerify:         false,
			InstanceCompleteGroup: "dp-cantabular-csv-exporter",
			InstanceCompleteTopic: "cantabular-dataset-instance-complete",
			CsvCreatedTopic:       "cantabular-csv-created",
		},
	}

	return cfg, envconfig.Process("", cfg)
}
