package config

import (
	"os"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestConfig(t *testing.T) {
	Convey("Given an environment with no environment variables set", t, func() {
		os.Clearenv()
		cfg, err := Get()

		Convey("When the config values are retrieved", func() {

			Convey("Then there should be no error returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the values should be set to the expected defaults", func() {
				So(cfg.BindAddr, ShouldEqual, ":26300")
				So(cfg.GracefulShutdownTimeout, ShouldEqual, 5*time.Second)
				So(cfg.HealthCheckInterval, ShouldEqual, 30*time.Second)
				So(cfg.HealthCheckCriticalTimeout, ShouldEqual, 90*time.Second)
				So(cfg.DefaultRequestTimeout, ShouldEqual, 10*time.Second)
				So(cfg.ServiceAuthToken, ShouldEqual, "")
				So(cfg.CantabularURL, ShouldEqual, "http://localhost:8491")
				So(cfg.CantabularExtURL, ShouldEqual, "http://localhost:8492")
				So(cfg.DatasetAPIURL, ShouldEqual, "http://localhost:22000")
				So(cfg.DownloadServiceURL, ShouldEqual, "http://localhost:23600")
				So(cfg.CantabularHealthcheckEnabled, ShouldBeFalse)
				So(cfg.AWSRegion, ShouldEqual, "eu-west-1")
				So(cfg.PublicUploadBucketName, ShouldEqual, "public-bucket")
				So(cfg.PrivateUploadBucketName, ShouldEqual, "private-bucket")
				So(cfg.VaultPath, ShouldEqual, "secret/shared/psk")
				So(cfg.VaultAddress, ShouldEqual, "http://localhost:8200")
				So(cfg.VaultToken, ShouldEqual, "")
				So(cfg.EncryptionDisabled, ShouldBeFalse)
				So(cfg.StopConsumingOnUnhealthy, ShouldBeTrue)
				So(cfg.KafkaConfig.Addr, ShouldResemble, []string{"localhost:9092"})
				So(cfg.KafkaConfig.Version, ShouldEqual, "1.0.2")
				So(cfg.KafkaConfig.OffsetOldest, ShouldBeTrue)
				So(cfg.KafkaConfig.NumWorkers, ShouldEqual, 1)
				So(cfg.KafkaConfig.MaxBytes, ShouldEqual, 2000000)
				So(cfg.KafkaConfig.SecProtocol, ShouldEqual, "")
				So(cfg.KafkaConfig.SecCACerts, ShouldEqual, "")
				So(cfg.KafkaConfig.SecClientKey, ShouldEqual, "")
				So(cfg.KafkaConfig.SecClientCert, ShouldEqual, "")
				So(cfg.KafkaConfig.SecSkipVerify, ShouldBeFalse)
				So(cfg.KafkaConfig.ExportStartGroup, ShouldEqual, "dp-cantabular-csv-exporter")
				So(cfg.KafkaConfig.ExportStartTopic, ShouldEqual, "cantabular-export-start")
				So(cfg.KafkaConfig.CsvCreatedTopic, ShouldEqual, "cantabular-csv-created")
			})

			Convey("Then a second call to config should return the same config", func() {
				newCfg, newErr := Get()
				So(newErr, ShouldBeNil)
				So(newCfg, ShouldResemble, cfg)
			})
		})
	})
}
