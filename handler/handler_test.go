package handler_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/ONSdigital/dp-api-clients-go/v2/cantabular"
	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-api-clients-go/v2/filter"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/config"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/event"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/handler"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/handler/mock"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/schema"
	kafka "github.com/ONSdigital/dp-kafka/v4"
	"github.com/ONSdigital/dp-kafka/v4/kafkatest"
	"github.com/ONSdigital/log.go/v2/log"
)

const (
	testBucket             = "test-bucket"
	testVaultPath          = "vault-root"
	testInstanceID         = "test-instance-id"
	testDatasetID          = "test-dataset-id"
	testEdition            = "test-edition"
	testVersion            = "test-version"
	testFilterOutputID     = "test-filter-output-id"
	testS3Location         = "s3://myBucket/my-file.csv"
	testS3PublicURL        = "test-bucket"
	testDownloadServiceURL = "http://test-download-service:8200"
	testNumBytes           = 123
	testRowCount           = 18
	testFileName           = "datasets/test-version.csv"
)

var (
	testExportStartEvent = &event.ExportStart{
		InstanceID: testInstanceID,
		DatasetID:  testDatasetID,
		Edition:    testEdition,
		Version:    testVersion,
	}

	testExportStartFilterEvent = &event.ExportStart{
		InstanceID:     testInstanceID,
		DatasetID:      testDatasetID,
		Edition:        testEdition,
		Version:        testVersion,
		FilterOutputID: testFilterOutputID,
	}

	testCsvBody = bufio.NewReader(bytes.NewReader([]byte("a,b,c,d,e,f,g,h,i,j,k,l")))
	testPsk     = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	testReq     = cantabular.StaticDatasetQueryRequest{
		Dataset:   "Example",
		Variables: []string{"city", "siblings"},
		Filters:   []cantabular.Filter{{Variable: "city", Codes: []string{"0", "1"}}},
	}
	errCantabular = errors.New("test Cantabular error")
	errS3         = errors.New("test S3Upload error")
	errVault      = errors.New("test Vault error")
	errPsk        = errors.New("test PSK error")
	errDataset    = errors.New("test DatasetAPI error")
	errFilter     = errors.New("test FilterAPI error")
)

func testCfg() config.Config {
	return config.Config{
		PublicUploadBucketName: testBucket,
		VaultPath:              testVaultPath,
		EncryptionDisabled:     true,
		DownloadServiceURL:     testDownloadServiceURL,
		KafkaConfig: config.KafkaConfig{
			Addr:            []string{"localhost:9092", "localhost:9093"},
			CsvCreatedTopic: "csv-created-topic",
		},
	}
}

var ctx = context.Background()

func TestValidateInstance(t *testing.T) {
	h := handler.NewInstanceComplete(testCfg(), nil, nil, nil, nil, nil, nil, nil, nil)

	Convey("Given an instance with 2 CSV headers in 'published' state", t, func() {
		i := dataset.Instance{
			Version: dataset.Version{
				CSVHeader: []string{"1", "2"},
				IsBasedOn: &dataset.IsBasedOn{ID: "myID"},
				State:     dataset.StatePublished.String(),
			},
		}
		Convey("Then ValidateInstance determines that instance is published, without error", func() {
			isPublished, err := h.ValidateInstance(i)
			So(err, ShouldBeNil)
			So(isPublished, ShouldBeTrue)
		})
	})

	Convey("Given an instance with 2 CSV headers in 'associated' state", t, func() {
		i := dataset.Instance{
			Version: dataset.Version{
				CSVHeader: []string{"1", "2"},
				IsBasedOn: &dataset.IsBasedOn{ID: "myID"},
				State:     dataset.StateAssociated.String(),
			},
		}
		Convey("Then ValidateInstance determines that instance is not published, without error", func() {
			isPublished, err := h.ValidateInstance(i)
			So(err, ShouldBeNil)
			So(isPublished, ShouldBeFalse)
		})
	})

	Convey("Given an instance with 2 CSV headers and no state", t, func() {
		i := dataset.Instance{
			Version: dataset.Version{
				CSVHeader: []string{"1", "2"},
				IsBasedOn: &dataset.IsBasedOn{ID: "myID"},
			},
		}
		Convey("Then ValidateInstance determines that instance is not published, without error", func() {
			isPublished, err := h.ValidateInstance(i)
			So(err, ShouldBeNil)
			So(isPublished, ShouldBeFalse)
		})
	})

	Convey("Given an instance wit only 1 CSV header", t, func() {
		i := dataset.Instance{
			Version: dataset.Version{
				CSVHeader: []string{"1"},
				IsBasedOn: &dataset.IsBasedOn{ID: "myID"},
			},
		}
		Convey("Then ValidateInstance determines that instance is not published, without error", func() {
			isPublished, err := h.ValidateInstance(i)
			So(err, ShouldBeNil)
			So(isPublished, ShouldBeFalse)
		})
	})

	Convey("Given an instance wit 0 CSV header", t, func() {
		i := dataset.Instance{
			Version: dataset.Version{
				CSVHeader: []string{},
				IsBasedOn: &dataset.IsBasedOn{ID: "myID"},
			},
		}
		Convey("Then ValidateInstance returns the expected error", func() {
			_, err := h.ValidateInstance(i)
			So(err, ShouldNotBeNil)
		})
	})

	Convey("Given an instance without isBasedOn field", t, func() {
		i := dataset.Instance{
			Version: dataset.Version{
				CSVHeader: []string{"1", "2"},
				State:     dataset.StatePublished.String(),
			},
		}
		Convey("Then ValidateInstance returns the expected error", func() {
			_, err := h.ValidateInstance(i)
			So(err, ShouldNotBeNil)
		})
	})

	Convey("Given an instance with an empty isBasedOn field", t, func() {
		i := dataset.Instance{
			Version: dataset.Version{
				CSVHeader: []string{"1", "2"},
				State:     dataset.StatePublished.String(),
				IsBasedOn: &dataset.IsBasedOn{},
			},
		}
		Convey("Then ValidateInstance returns the expected error", func() {
			_, err := h.ValidateInstance(i)
			So(err, ShouldNotBeNil)
		})
	})
}

func TestUploadPrivateUnEncryptedCSVFile(t *testing.T) {
	isPublished := false
	isCustom := false
	expectedS3Key := fmt.Sprintf("datasets/%s-%s-%s.csv", testDatasetID, testEdition, testVersion)

	Convey("Given an event handler with a successful cantabular client and private S3Client", t, func() {
		c := cantabularMock(testCsvBody)
		sPrivate := s3ClientHappy(false)
		eventHandler := handler.NewInstanceComplete(testCfg(), &c, nil, nil, &sPrivate, nil, nil, nil, nil)

		Convey("When UploadCSVFile is triggered with valid paramters and encryption disbled", func() {
			loc, rowCount, _, err := eventHandler.UploadCSVFile(ctx, testExportStartEvent, testReq, isPublished, isCustom)

			Convey("Then the expected location and rowCount is returned with no error ", func() {
				So(err, ShouldBeNil)
				So(loc, ShouldEqual, testS3Location)
				So(rowCount, ShouldEqual, testRowCount)
			})

			Convey("Then the expected UploadWithContext call is executed", func() {
				So(sPrivate.UploadWithContextCalls(), ShouldHaveLength, 1)
				So(sPrivate.UploadWithContextCalls()[0].Ctx, ShouldResemble, ctx)
				So(*sPrivate.UploadWithContextCalls()[0].Input.Key, ShouldResemble, expectedS3Key)
				So(*sPrivate.UploadWithContextCalls()[0].Input.Bucket, ShouldResemble, testBucket)
				So(sPrivate.UploadWithContextCalls()[0].Input.Body, ShouldResemble, testCsvBody)
			})
		})
	})

	Convey("Given an event handler with a successful cantabular client and an unsuccessful private S3Client", t, func() {
		c := cantabularMock(testCsvBody)
		sPrivate := s3ClientUnhappy(false)
		eventHandler := handler.NewInstanceComplete(testCfg(), &c, nil, nil, &sPrivate, nil, nil, nil, nil)

		Convey("When UploadCSVFile is triggered", func() {
			_, _, _, err := eventHandler.UploadCSVFile(ctx, testExportStartEvent, testReq, isPublished, isCustom)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, handler.NewError(
					fmt.Errorf("failed to stream csv data: %w",
						fmt.Errorf("failed to upload un-encrypted private file to S3: %w", errS3),
					),
					log.Data{
						"bucket":              testBucket,
						"filename":            expectedS3Key,
						"encryption_disabled": true,
						"is_published":        false,
					},
				))
			})
		})
	})

	Convey("Given an event handler with an unsuccessful cantabular client", t, func() {
		c := cantabularUnhappy()
		sPrivate := mock.S3ClientMock{
			BucketNameFunc: func() string { return testBucket },
		}
		eventHandler := handler.NewInstanceComplete(testCfg(), &c, nil, nil, &sPrivate, nil, nil, nil, nil)

		Convey("When UploadCSVFile is triggered", func() {
			_, _, _, err := eventHandler.UploadCSVFile(ctx, testExportStartEvent, testReq, isPublished, isCustom)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, handler.NewError(
					fmt.Errorf("failed to stream csv data: %w", errCantabular),
					log.Data{
						"bucket":              testBucket,
						"filename":            expectedS3Key,
						"encryption_disabled": true,
						"is_published":        false,
					},
				))
			})
		})
	})

	Convey("Given an empty event handler", t, func() {
		eventHandler := handler.NewInstanceComplete(testCfg(), nil, nil, nil, nil, nil, nil, nil, nil)

		Convey("When UploadCSVFile is triggered with an empty export-start event", func() {
			_, _, _, err := eventHandler.UploadCSVFile(ctx, &event.ExportStart{}, testReq, isPublished, isCustom)
			So(err, ShouldNotBeNil)
		})
	})
}

func TestUploadPrivateEncryptedCSVFile(t *testing.T) {
	generator := &mock.GeneratorMock{
		NewPSKFunc: func() ([]byte, error) {
			return testPsk, nil
		},
	}
	isPublished := false
	isCustom := false
	expectedS3Key := fmt.Sprintf("datasets/%s-%s-%s.csv", testDatasetID, testEdition, testVersion)
	expectedVaultPath := fmt.Sprintf("%s/%s-%s-%s.csv", testVaultPath, testDatasetID, testEdition, testVersion)
	cfg := testCfg()
	cfg.EncryptionDisabled = false

	Convey("Given an event handler with a successful cantabular client, private S3Client, Vault client and encryption enabled", t, func() {
		c := cantabularMock(testCsvBody)
		sPrivate := s3ClientHappy(true)
		v := vaultHappy()
		eventHandler := handler.NewInstanceComplete(cfg, &c, nil, nil, &sPrivate, nil, &v, nil, generator)

		Convey("When UploadCSVFile is triggered with valid paramters", func() {
			loc, rowCount, filename, err := eventHandler.UploadCSVFile(ctx, testExportStartEvent, testReq, isPublished, isCustom)

			Convey("Then the expected location, rowCount and file name is returned with no error ", func() {
				So(err, ShouldBeNil)
				So(loc, ShouldEqual, testS3Location)
				So(rowCount, ShouldEqual, testRowCount)
				So(filename, ShouldEqual, "datasets/test-dataset-id-test-edition-test-version.csv") // No filter id, thus time stamp not added
			})

			Convey("Then the expected key is stored in vault", func() {
				So(v.WriteKeyCalls(), ShouldHaveLength, 1)
				expectedPsk := hex.EncodeToString(testPsk)
				So(v.WriteKeyCalls()[0].Path, ShouldResemble, expectedVaultPath)
				So(v.WriteKeyCalls()[0].Key, ShouldResemble, "key")
				So(v.WriteKeyCalls()[0].Value, ShouldResemble, expectedPsk)
			})

			Convey("Then the expected call UploadWithPSK call is executed with the expected psk", func() {
				So(sPrivate.UploadWithPSKCalls(), ShouldHaveLength, 1)
				So(*sPrivate.UploadWithPSKCalls()[0].Input.Key, ShouldResemble, expectedS3Key)
				So(*sPrivate.UploadWithPSKCalls()[0].Input.Bucket, ShouldResemble, testBucket)
				So(sPrivate.UploadWithPSKCalls()[0].Input.Body, ShouldResemble, testCsvBody)
				So(sPrivate.UploadWithPSKCalls()[0].Psk, ShouldResemble, testPsk)
			})
		})
	})

	Convey("Given an event handler with an unsuccessful Vault client and encryption enabled", t, func() {
		sPrivate := mock.S3ClientMock{
			BucketNameFunc: func() string { return testBucket },
		}
		vaultClient := vaultUnhappy()
		eventHandler := handler.NewInstanceComplete(cfg, nil, nil, nil, &sPrivate, nil, &vaultClient, nil, generator)

		Convey("When UploadCSVFile is triggered", func() {
			_, _, _, err := eventHandler.UploadCSVFile(ctx, testExportStartEvent, testReq, isPublished, isCustom)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, handler.NewError(
					fmt.Errorf("failed to write key to vault: %w", errVault),
					log.Data{
						"bucket":              testBucket,
						"filename":            expectedS3Key,
						"encryption_disabled": false,
						"is_published":        false,
					},
				))
			})
		})
	})

	Convey("Given an event handler with a successful Vault client, a successful cantabular client, an unsuccessful private S3 client and encryption enabled", t, func() {
		c := cantabularMock(testCsvBody)
		sPrivate := s3ClientUnhappy(true)
		vaultClient := vaultHappy()
		eventHandler := handler.NewInstanceComplete(cfg, &c, nil, nil, &sPrivate, nil, &vaultClient, nil, generator)

		Convey("When UploadCSVFile is triggered", func() {
			_, _, _, err := eventHandler.UploadCSVFile(ctx, testExportStartEvent, testReq, isPublished, isCustom)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, handler.NewError(
					fmt.Errorf("failed to stream csv data: %w",
						fmt.Errorf("failed to upload encrypted private file to S3: %w", errS3),
					),
					log.Data{
						"bucket":              testBucket,
						"filename":            expectedS3Key,
						"encryption_disabled": false,
						"is_published":        false,
					},
				))
			})
		})
	})

	Convey("Given an event handler with a successful Vault client, an unsuccessful cantabular client and encryption enabled", t, func() {
		c := cantabularUnhappy()
		sPrivate := mock.S3ClientMock{
			BucketNameFunc: func() string { return testBucket },
		}
		vaultClient := vaultHappy()
		eventHandler := handler.NewInstanceComplete(cfg, &c, nil, nil, &sPrivate, nil, &vaultClient, nil, generator)

		Convey("When UploadCSVFile is triggered", func() {
			_, _, _, err := eventHandler.UploadCSVFile(ctx, testExportStartEvent, testReq, isPublished, isCustom)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, handler.NewError(
					fmt.Errorf("failed to stream csv data: %w", errCantabular),
					log.Data{
						"bucket":              testBucket,
						"filename":            expectedS3Key,
						"encryption_disabled": false,
						"is_published":        false,
					},
				))
			})
		})
	})

	Convey("Given an event handler, a failing createPSK function and encryption enabled", t, func() {
		sPrivate := mock.S3ClientMock{
			BucketNameFunc: func() string { return testBucket },
		}

		generator.NewPSKFunc = func() ([]byte, error) {
			return nil, errPsk
		}

		eventHandler := handler.NewInstanceComplete(cfg, nil, nil, nil, &sPrivate, nil, nil, nil, generator)

		Convey("When UploadCSVFile is triggered with valid paramters", func() {
			_, _, _, err := eventHandler.UploadCSVFile(ctx, testExportStartEvent, testReq, isPublished, isCustom)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, fmt.Sprintf("failed to generate a PSK for encryption: %s", errPsk.Error()))
			})
		})
	})
}

func TestUploadPublishedCSVFile(t *testing.T) {
	isPublished := true
	isCustom := false
	expectedS3Key := fmt.Sprintf("datasets/%s-%s-%s.csv", testDatasetID, testEdition, testVersion)

	Convey("Given an event handler with a successful cantabular client and public S3Client", t, func() {
		c := cantabularMock(testCsvBody)
		sPublic := s3ClientHappy(false)
		eventHandler := handler.NewInstanceComplete(testCfg(), &c, nil, nil, nil, &sPublic, nil, nil, nil)

		Convey("When UploadCSVFile is triggered with valid paramters", func() {
			loc, rowCount, filename, err := eventHandler.UploadCSVFile(ctx, testExportStartEvent, testReq, isPublished, isCustom)

			Convey("Then the expected location, rowCount and file name is returned with no error ", func() {
				So(err, ShouldBeNil)
				So(loc, ShouldEqual, testS3Location)
				So(rowCount, ShouldEqual, testRowCount)
				So(filename, ShouldEqual, "datasets/test-dataset-id-test-edition-test-version.csv") // No filter id, thus time stamp not added
			})

			Convey("Then the expected UploadWithContext call is executed", func() {
				So(sPublic.UploadWithContextCalls(), ShouldHaveLength, 1)
				So(sPublic.UploadWithContextCalls()[0].Ctx, ShouldResemble, ctx)
				So(*sPublic.UploadWithContextCalls()[0].Input.Key, ShouldResemble, expectedS3Key)
				So(*sPublic.UploadWithContextCalls()[0].Input.Bucket, ShouldResemble, testBucket)
				So(sPublic.UploadWithContextCalls()[0].Input.Body, ShouldResemble, testCsvBody)
			})
		})
	})

	Convey("Given an event handler with a successful cantabular client and an unsuccessful public S3Client", t, func() {
		c := cantabularMock(testCsvBody)
		publicS3Client := s3ClientUnhappy(false)
		eventHandler := handler.NewInstanceComplete(testCfg(), &c, nil, nil, nil, &publicS3Client, nil, nil, nil)

		Convey("When UploadCSVFile is triggered", func() {
			_, _, _, err := eventHandler.UploadCSVFile(ctx, testExportStartEvent, testReq, isPublished, isCustom)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, handler.NewError(
					fmt.Errorf("failed to stream csv data: %w",
						fmt.Errorf("failed to upload published file to S3: %w", errS3),
					),
					log.Data{
						"bucket":       testBucket,
						"filename":     expectedS3Key,
						"is_published": true,
					}),
				)
			})
		})
	})

	Convey("Given an event handler with an unsuccessful cantabular client", t, func() {
		cfg := testCfg()
		c := cantabularUnhappy()
		publicS3Client := mock.S3ClientMock{
			BucketNameFunc: func() string { return testBucket },
		}
		eventHandler := handler.NewInstanceComplete(cfg, &c, nil, nil, nil, &publicS3Client, nil, nil, nil)

		Convey("When UploadCSVFile is triggered", func() {
			_, _, _, err := eventHandler.UploadCSVFile(ctx, testExportStartEvent, testReq, isPublished, isCustom)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, handler.NewError(
					fmt.Errorf("failed to stream csv data: %w", errCantabular),
					log.Data{
						"bucket":       testBucket,
						"filename":     expectedS3Key,
						"is_published": true,
					}),
				)
			})
		})
	})
}

func TestGetS3ContentLength(t *testing.T) {
	var ContentLength int64 = testNumBytes
	headOk := func(key string) (*s3.HeadObjectOutput, error) {
		return &s3.HeadObjectOutput{
			ContentLength: &ContentLength,
		}, nil
	}
	headErr := func(key string) (*s3.HeadObjectOutput, error) {
		return nil, errS3
	}

	Convey("Given an event handler with a successful s3 private client", t, func() {
		sPrivate := mock.S3ClientMock{HeadFunc: headOk}
		eventHandler := handler.NewInstanceComplete(testCfg(), nil, nil, nil, &sPrivate, nil, nil, nil, nil)

		Convey("Then GetS3ContentLength returns the expected size with no error", func() {
			numBytes, err := eventHandler.GetS3ContentLength(ctx, testExportStartEvent, false, "")
			So(err, ShouldBeNil)
			So(numBytes, ShouldEqual, testNumBytes)
		})
	})

	Convey("Given an event handler with a failing s3 private client", t, func() {
		sPrivate := mock.S3ClientMock{HeadFunc: headErr}
		eventHandler := handler.NewInstanceComplete(testCfg(), nil, nil, nil, &sPrivate, nil, nil, nil, nil)

		Convey("Then GetS3ContentLength returns the expected error", func() {
			_, err := eventHandler.GetS3ContentLength(ctx, testExportStartEvent, false, "")
			So(err, ShouldResemble, fmt.Errorf("private s3 head object error: %w", errS3))
		})
	})

	Convey("Given an event handler with a successful s3 public client", t, func() {
		sPublic := mock.S3ClientMock{HeadFunc: headOk}
		eventHandler := handler.NewInstanceComplete(testCfg(), nil, nil, nil, nil, &sPublic, nil, nil, nil)

		Convey("Then GetS3ContentLength returns the expected size with no error", func() {
			numBytes, err := eventHandler.GetS3ContentLength(ctx, testExportStartEvent, true, "")
			So(err, ShouldBeNil)
			So(numBytes, ShouldEqual, testNumBytes)
		})
	})

	Convey("Given an event handler with a failing s3 public client", t, func() {
		sPublic := mock.S3ClientMock{HeadFunc: headErr}
		eventHandler := handler.NewInstanceComplete(testCfg(), nil, nil, nil, nil, &sPublic, nil, nil, nil)

		Convey("Then GetS3ContentLength returns the expected error", func() {
			_, err := eventHandler.GetS3ContentLength(ctx, testExportStartEvent, true, "")
			So(err, ShouldResemble, fmt.Errorf("public s3 head object error: %w", errS3))
		})
	})
}

func TestUpdateFilterOutput(t *testing.T) {
	testSize := testCsvBody.Size()

	Convey("Given an event handler with a successful dataset API mock", t, func() {
		filterAPIMock := filterAPIClientHappy()
		eventHandler := handler.NewInstanceComplete(testCfg(), nil, nil, &filterAPIMock, nil, nil, nil, nil, nil)

		Convey("When UpdateFilterOutput is called for a valid event", func() {
			err := eventHandler.UpdateFilterOutput(ctx, testExportStartFilterEvent, testSize, false, "", testFileName)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("And the expected UpdateInstance call is executed with the expected paramters", func() {
				expectedURL := fmt.Sprintf("%s/downloads/filter-outputs/%s.csv", testDownloadServiceURL, testFilterOutputID)
				So(filterAPIMock.UpdateFilterOutputCalls(), ShouldHaveLength, 1)
				So(filterAPIMock.UpdateFilterOutputCalls()[0].FilterOutputID, ShouldEqual, testFilterOutputID)
				So(filterAPIMock.UpdateFilterOutputCalls()[0].M, ShouldResemble, &filter.Model{
					Downloads: map[string]filter.Download{
						"CSV": {
							URL:  expectedURL,
							Size: fmt.Sprintf("%d", testSize),
						},
					},
				})
			})
		})
	})

	Convey("Given an event handler with a failing dataset API mock", t, func() {
		filterAPIMock := filterAPIClientUnhappy()
		eventHandler := handler.NewInstanceComplete(testCfg(), nil, nil, &filterAPIMock, nil, nil, nil, nil, nil)

		Convey("When UpdateFilterOutput is called", func() {
			err := eventHandler.UpdateFilterOutput(ctx, testExportStartFilterEvent, testSize, false, "", testFileName)

			Convey("Then an error is returned", func() {
				So(err, ShouldNotBeNil)
			})
		})
	})
}

func TestUpdateInstance(t *testing.T) {
	testSize := testCsvBody.Size()

	Convey("Given an event handler with a successful dataset API mock", t, func() {
		datasetAPIMock := datasetAPIClientHappy()
		eventHandler := handler.NewInstanceComplete(testCfg(), nil, &datasetAPIMock, nil, nil, nil, nil, nil, nil)

		Convey("When UpdateInstance is called for a private csv file", func() {
			err := eventHandler.UpdateInstance(ctx, testExportStartEvent, testSize, false, "", testFileName)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the expected UpdateInstance call is executed with the expected paramters", func() {
				expectedURL := fmt.Sprintf("%s/downloads/datasets/%s/editions/%s/versions/%s.csv", testDownloadServiceURL, testDatasetID, testEdition, testVersion)
				So(datasetAPIMock.GetInstanceCalls(), ShouldHaveLength, 0)
				So(datasetAPIMock.PutVersionCalls(), ShouldHaveLength, 1)
				So(datasetAPIMock.PutVersionCalls()[0].DatasetID, ShouldEqual, testDatasetID)
				So(datasetAPIMock.PutVersionCalls()[0].Edition, ShouldEqual, testEdition)
				So(datasetAPIMock.PutVersionCalls()[0].Version, ShouldEqual, testVersion)
				So(datasetAPIMock.PutVersionCalls()[0].V, ShouldResemble, dataset.Version{
					Downloads: map[string]dataset.Download{
						"CSV": {
							URL:  expectedURL,
							Size: fmt.Sprintf("%d", testSize),
						},
					},
				})
			})
		})

		Convey("When UpdateInstance is called for a public csv file", func() {
			err := eventHandler.UpdateInstance(ctx, testExportStartEvent, testSize, true, "publicURL", testFileName)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the expected UpdateInstance call is executed once to update the public download link and associate it, and once more to publish it", func() {
				expectedURL := fmt.Sprintf("%s/downloads/datasets/%s/editions/%s/versions/%s.csv", testDownloadServiceURL, testDatasetID, testEdition, testVersion)
				So(datasetAPIMock.GetInstanceCalls(), ShouldHaveLength, 0)
				So(datasetAPIMock.PutVersionCalls(), ShouldHaveLength, 1)
				So(datasetAPIMock.PutVersionCalls()[0].DatasetID, ShouldEqual, testDatasetID)
				So(datasetAPIMock.PutVersionCalls()[0].Edition, ShouldEqual, testEdition)
				So(datasetAPIMock.PutVersionCalls()[0].Version, ShouldEqual, testVersion)
				So(datasetAPIMock.PutVersionCalls()[0].V, ShouldResemble, dataset.Version{
					Downloads: map[string]dataset.Download{
						"CSV": {
							Public: fmt.Sprintf("/datasets/%s.csv", testVersion),
							URL:    expectedURL,
							Size:   fmt.Sprintf("%d", testSize),
						},
					},
				})
			})
		})
	})

	Convey("Given an event handler with a failing dataset API mock", t, func() {
		datasetAPIMock := datasetAPIClientUnhappy()
		eventHandler := handler.NewInstanceComplete(testCfg(), nil, &datasetAPIMock, nil, nil, nil, nil, nil, nil)

		Convey("When UpdateInstance is called", func() {
			err := eventHandler.UpdateInstance(ctx, testExportStartEvent, testSize, false, "", testFileName)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, fmt.Errorf("error while attempting update version downloads: %w", errDataset))
			})
		})
	})
}

func TestProduceExportCompleteEvent(t *testing.T) {
	Convey("Given an event handler with a successful Kafka Producer", t, func(c C) {
		testCfg := testCfg()
		producer, _ := kafkatest.NewProducer(
			ctx,
			&kafka.ProducerConfig{
				BrokerAddrs: testCfg.KafkaConfig.Addr,
				Topic:       testCfg.KafkaConfig.CsvCreatedTopic,
			},
			nil,
		)
		eventHandler := handler.NewInstanceComplete(testCfg, nil, nil, nil, nil, nil, nil, producer.Mock, nil)

		Convey("When ProduceExportCompleteEvent is called for a private csv file", func(c C) {
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := eventHandler.ProduceExportCompleteEvent(ctx, testExportStartEvent, testRowCount, "")
				c.So(err, ShouldBeNil)
			}()

			expectedEvent := event.CSVCreated{
				InstanceID: testInstanceID,
				DatasetID:  testDatasetID,
				Edition:    testEdition,
				Version:    testVersion,
				RowCount:   testRowCount,
				Dimensions: []string{},
			}

			Convey("Then the expected message is produced", func() {
				producedMessage := event.CSVCreated{}
				err := producer.WaitForMessageSent(schema.CSVCreated, &producedMessage, 5*time.Second)
				So(producedMessage, ShouldResemble, expectedEvent)
				So(err, ShouldBeNil)
			})

			// make sure the go-routine finishes its execution
			wg.Wait()
		})

		Convey("When ProduceExportCompleteEvent is called for a public csv file", func(c C) {
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := eventHandler.ProduceExportCompleteEvent(ctx, testExportStartEvent, testRowCount, "")
				c.So(err, ShouldBeNil)
			}()

			expectedEvent := event.CSVCreated{
				InstanceID: testInstanceID,
				DatasetID:  testDatasetID,
				Edition:    testEdition,
				Version:    testVersion,
				RowCount:   testRowCount,
				Dimensions: []string{},
			}

			Convey("Then the expected message is produced", func() {
				producedMessage := event.CSVCreated{}
				err := producer.WaitForMessageSent(schema.CSVCreated, &producedMessage, 5*time.Second)
				So(producedMessage, ShouldResemble, expectedEvent)
				So(err, ShouldBeNil)
			})

			// make sure the go-routine finishes its execution
			wg.Wait()
		})
	})
}

// cantabularMock creates a Cantabular Client mock that executes consume with the provided io.Reader and propagates any error returned by it
func cantabularMock(r io.Reader) mock.CantabularClientMock {
	return mock.CantabularClientMock{
		StaticDatasetQueryStreamCSVFunc: func(ctx context.Context, req cantabular.StaticDatasetQueryRequest, consume func(ctx context.Context, r io.Reader) error) (int32, error) {
			err := consume(ctx, r)
			return testRowCount, err
		},
	}
}

// cantabularUnhappy creates a Cantabular Client mock that returns an error and does not execute the consumer
func cantabularUnhappy() mock.CantabularClientMock {
	return mock.CantabularClientMock{
		StaticDatasetQueryStreamCSVFunc: func(ctx context.Context, req cantabular.StaticDatasetQueryRequest, consume func(ctx context.Context, r io.Reader) error) (int32, error) {
			return 0, errCantabular
		},
	}
}

func s3ClientHappy(encryptionEnabled bool) mock.S3ClientMock {
	if encryptionEnabled {
		return mock.S3ClientMock{
			UploadWithPSKFunc: func(input *s3manager.UploadInput, psk []byte) (*s3manager.UploadOutput, error) {
				return &s3manager.UploadOutput{
					Location: testS3Location,
				}, nil
			},
			BucketNameFunc: func() string {
				return testBucket
			},
		}
	}
	return mock.S3ClientMock{
		UploadWithContextFunc: func(ctx context.Context, input *s3manager.UploadInput, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
			return &s3manager.UploadOutput{
				Location: testS3Location,
			}, nil
		},
		BucketNameFunc: func() string {
			return testBucket
		},
	}
}

func vaultHappy() mock.VaultClientMock {
	return mock.VaultClientMock{
		WriteKeyFunc: func(path string, key string, value string) error {
			return nil
		},
	}
}

func vaultUnhappy() mock.VaultClientMock {
	return mock.VaultClientMock{
		WriteKeyFunc: func(path string, key string, value string) error {
			return errVault
		},
	}
}

func s3ClientUnhappy(encryptionEnabled bool) mock.S3ClientMock {
	if encryptionEnabled {
		return mock.S3ClientMock{
			UploadWithPSKFunc: func(input *s3manager.UploadInput, psk []byte) (*s3manager.UploadOutput, error) {
				return nil, errS3
			},
			BucketNameFunc: func() string {
				return testBucket
			},
		}
	}
	return mock.S3ClientMock{
		UploadWithContextFunc: func(ctx context.Context, input *s3manager.UploadInput, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
			return nil, errS3
		},
		BucketNameFunc: func() string {
			return testBucket
		},
	}
}

func datasetAPIClientHappy() mock.DatasetAPIClientMock {
	return mock.DatasetAPIClientMock{
		GetInstanceFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, instanceID string, ifMatch string) (dataset.Instance, string, error) {
			return dataset.Instance{}, "", nil
		},
		PutVersionFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, datasetID string, edition string, version string, v dataset.Version) error {
			return nil
		},
	}
}

func datasetAPIClientUnhappy() mock.DatasetAPIClientMock {
	return mock.DatasetAPIClientMock{
		GetInstanceFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, instanceID string, ifMatch string) (dataset.Instance, string, error) {
			return dataset.Instance{}, "", errDataset
		},
		PutVersionFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, datasetID string, edition string, version string, v dataset.Version) error {
			return errDataset
		},
	}
}

func filterAPIClientHappy() mock.FilterAPIClientMock {
	return mock.FilterAPIClientMock{
		UpdateFilterOutputFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, filterOutputID string, m *filter.Model) error {
			return nil
		},
	}
}

func filterAPIClientUnhappy() mock.FilterAPIClientMock {
	return mock.FilterAPIClientMock{
		UpdateFilterOutputFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, filterOutputID string, m *filter.Model) error {
			return errFilter
		},
	}
}
