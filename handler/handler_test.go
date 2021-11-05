package handler_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/ONSdigital/dp-api-clients-go/v2/cantabular"
	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-api-clients-go/v2/headers"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/config"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/event"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/handler"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/handler/mock"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/schema"
	"github.com/ONSdigital/dp-kafka/v3/kafkatest"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	testBucket             = "test-bucket"
	testVaultPath          = "vault-root"
	testInstanceID         = "test-instance-id"
	testS3Location         = "s3://myBucket/my-file.csv"
	testDownloadServiceURL = "http://test-download-service:8200"
	testETag               = "testETag"
	testNumBytes           = 123
	testRowCount           = 18
)

var (
	testCsvBody = bufio.NewReader(bytes.NewReader([]byte("a,b,c,d,e,f,g,h,i,j,k,l")))
	testPsk     = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	testReq     = cantabular.StaticDatasetQueryRequest{
		Dataset:   "Example",
		Variables: []string{"city", "siblings"},
	}
	errCantabular = errors.New("test Cantabular error")
	errS3         = errors.New("test S3Upload error")
	errVault      = errors.New("test Vault error")
	errPsk        = errors.New("test PSK error")
	errDataset    = errors.New("test DatasetAPI error")
)

func testCfg() config.Config {
	return config.Config{
		UploadBucketName:   testBucket,
		VaultPath:          testVaultPath,
		EncryptionDisabled: true,
		DownloadServiceURL: testDownloadServiceURL,
	}
}

var ctx = context.Background()

func TestValidateInstance(t *testing.T) {
	h := handler.NewInstanceComplete(testCfg(), nil, nil, nil, nil, nil, nil, nil)

	Convey("Given an instance with 2 CSV headers in 'published' state", t, func() {
		i := dataset.Instance{
			Version: dataset.Version{
				CSVHeader: []string{"1", "2"},
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
			},
		}
		Convey("Then ValidateInstance returns the expected error", func() {
			_, err := h.ValidateInstance(i)
			So(err, ShouldResemble, handler.NewError(
				errors.New("no dimensions in headers"),
				log.Data{"headers": []string{"1"}},
			))
		})
	})
}

func TestUploadPrivateUnEncryptedCSVFile(t *testing.T) {
	isPublished := false
	expectedS3Key := fmt.Sprintf("instances/%s.csv", testInstanceID)

	Convey("Given an event handler with a successful cantabular client and private S3Uploader", t, func() {
		c := cantabularMock(testCsvBody)
		sPrivate := s3UploaderHappy(false)
		eventHandler := handler.NewInstanceComplete(testCfg(), &c, nil, &sPrivate, nil, nil, nil, nil)

		Convey("When UploadCSVFile is triggered with valid paramters and encryption disbled", func() {
			loc, rowCount, err := eventHandler.UploadCSVFile(ctx, testInstanceID, isPublished, testReq)

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

	Convey("Given an event handler with a successful cantabular client and an unsuccessful private S3Uploader", t, func() {
		c := cantabularMock(testCsvBody)
		sPrivate := s3UploaderUnhappy(false)
		eventHandler := handler.NewInstanceComplete(testCfg(), &c, nil, &sPrivate, nil, nil, nil, nil)

		Convey("When UploadCSVFile is triggered", func() {
			_, _, err := eventHandler.UploadCSVFile(ctx, testInstanceID, isPublished, testReq)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, handler.NewError(
					fmt.Errorf("failed to stream csv data: %w",
						fmt.Errorf("failed to upload private file to S3: %w", errS3),
					),
					log.Data{
						"bucket":              testBucket,
						"filename":            fmt.Sprintf("instances/%s.csv", testInstanceID),
						"encryption_disabled": true,
						"is_published":        false,
					},
				))
			})
		})
	})

	Convey("Given an event handler with an unsuccessful cantabular client", t, func() {
		c := cantabularUnhappy()
		sPrivate := mock.S3UploaderMock{
			BucketNameFunc: func() string { return testBucket },
		}
		eventHandler := handler.NewInstanceComplete(testCfg(), &c, nil, &sPrivate, nil, nil, nil, nil)

		Convey("When UploadCSVFile is triggered", func() {
			_, _, err := eventHandler.UploadCSVFile(ctx, testInstanceID, isPublished, testReq)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, handler.NewError(
					fmt.Errorf("failed to stream csv data: %w", errCantabular),
					log.Data{
						"bucket":              testBucket,
						"filename":            fmt.Sprintf("instances/%s.csv", testInstanceID),
						"encryption_disabled": true,
						"is_published":        false,
					},
				))
			})
		})
	})

	Convey("Given an empty event handler", t, func() {
		eventHandler := handler.NewInstanceComplete(testCfg(), nil, nil, nil, nil, nil, nil, nil)

		Convey("When UploadCSVFile is triggered with an empty instanceID", func() {
			_, _, err := eventHandler.UploadCSVFile(ctx, "", isPublished, testReq)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, errors.New("empty instance id not allowed"))
			})
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
	expectedS3Key := fmt.Sprintf("instances/%s.csv", testInstanceID)
	expectedVaultPath := fmt.Sprintf("%s/%s.csv", testVaultPath, testInstanceID)
	cfg := testCfg()
	cfg.EncryptionDisabled = false

	Convey("Given an event handler with a successful cantabular client, private S3Uploader, Vault client and encryption enabled", t, func() {
		c := cantabularMock(testCsvBody)
		sPrivate := s3UploaderHappy(true)
		v := vaultHappy()
		eventHandler := handler.NewInstanceComplete(cfg, &c, nil, &sPrivate, nil, &v, nil, generator)

		Convey("When UploadCSVFile is triggered with valid paramters", func() {
			loc, rowCount, err := eventHandler.UploadCSVFile(ctx, testInstanceID, isPublished, testReq)

			Convey("Then the expected location and rowCount is returned with no error ", func() {
				So(err, ShouldBeNil)
				So(loc, ShouldEqual, testS3Location)
				So(rowCount, ShouldEqual, testRowCount)
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
		sPrivate := mock.S3UploaderMock{
			BucketNameFunc: func() string { return testBucket },
		}
		vaultClient := vaultUnhappy()
		eventHandler := handler.NewInstanceComplete(cfg, nil, nil, &sPrivate, nil, &vaultClient, nil, generator)

		Convey("When UploadCSVFile is triggered", func() {
			_, _, err := eventHandler.UploadCSVFile(ctx, testInstanceID, isPublished, testReq)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, handler.NewError(
					fmt.Errorf("failed to write key to vault: %w", errVault),
					log.Data{
						"bucket":              testBucket,
						"filename":            fmt.Sprintf("instances/%s.csv", testInstanceID),
						"encryption_disabled": false,
						"is_published":        false,
					},
				))
			})
		})
	})

	Convey("Given an event handler with a successful Vault client, a successful cantabular client, an unsuccessful private S3 client and encryption enabled", t, func() {
		c := cantabularMock(testCsvBody)
		sPrivate := s3UploaderUnhappy(true)
		vaultClient := vaultHappy()
		eventHandler := handler.NewInstanceComplete(cfg, &c, nil, &sPrivate, nil, &vaultClient, nil, generator)

		Convey("When UploadCSVFile is triggered", func() {
			_, _, err := eventHandler.UploadCSVFile(ctx, testInstanceID, isPublished, testReq)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, handler.NewError(
					fmt.Errorf("failed to stream csv data: %w",
						fmt.Errorf("failed to upload encrypted private file to S3: %w", errS3),
					),
					log.Data{
						"bucket":              testBucket,
						"filename":            fmt.Sprintf("instances/%s.csv", testInstanceID),
						"encryption_disabled": false,
						"is_published":        false,
					},
				))
			})
		})
	})

	Convey("Given an event handler with a successful Vault client, an unsuccessful cantabular client and encryption enabled", t, func() {
		c := cantabularUnhappy()
		sPrivate := mock.S3UploaderMock{
			BucketNameFunc: func() string { return testBucket },
		}
		vaultClient := vaultHappy()
		eventHandler := handler.NewInstanceComplete(cfg, &c, nil, &sPrivate, nil, &vaultClient, nil, generator)

		Convey("When UploadCSVFile is triggered", func() {
			_, _, err := eventHandler.UploadCSVFile(ctx, testInstanceID, isPublished, testReq)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, handler.NewError(
					fmt.Errorf("failed to stream csv data: %w", errCantabular),
					log.Data{
						"bucket":              testBucket,
						"filename":            fmt.Sprintf("instances/%s.csv", testInstanceID),
						"encryption_disabled": false,
						"is_published":        false,
					},
				))
			})
		})
	})

	Convey("Given an event handler, a failing createPSK function and encryption enabled", t, func() {
		sPrivate := mock.S3UploaderMock{
			BucketNameFunc: func() string { return testBucket },
		}

		generator.NewPSKFunc = func() ([]byte, error) {
			return nil, errPsk
		}

		eventHandler := handler.NewInstanceComplete(cfg, nil, nil, &sPrivate, nil, nil, nil, generator)

		Convey("When UploadCSVFile is triggered with valid paramters", func() {
			_, _, err := eventHandler.UploadCSVFile(ctx, testInstanceID, isPublished, testReq)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, fmt.Sprintf("failed to generate a PSK for encryption: %s", errPsk.Error()))
			})
		})
	})
}

func TestUploadPublishedCSVFile(t *testing.T) {
	isPublished := true
	expectedS3Key := fmt.Sprintf("instances/%s.csv", testInstanceID)

	Convey("Given an event handler with a successful cantabular client and public S3Uploader", t, func() {
		c := cantabularMock(testCsvBody)
		sPublic := s3UploaderHappy(false)
		eventHandler := handler.NewInstanceComplete(testCfg(), &c, nil, nil, &sPublic, nil, nil, nil)

		Convey("When UploadCSVFile is triggered with valid paramters", func() {
			loc, rowCount, err := eventHandler.UploadCSVFile(ctx, testInstanceID, isPublished, testReq)

			Convey("Then the expected location and rowCount is returned with no error ", func() {
				So(err, ShouldBeNil)
				So(loc, ShouldEqual, testS3Location)
				So(rowCount, ShouldEqual, testRowCount)
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

	Convey("Given an event handler with a successful cantabular client and an unsuccessful public S3Uploader", t, func() {
		c := cantabularMock(testCsvBody)
		publicS3Uploader := s3UploaderUnhappy(false)
		eventHandler := handler.NewInstanceComplete(testCfg(), &c, nil, nil, &publicS3Uploader, nil, nil, nil)

		Convey("When UploadCSVFile is triggered", func() {
			_, _, err := eventHandler.UploadCSVFile(ctx, testInstanceID, isPublished, testReq)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, handler.NewError(
					fmt.Errorf("failed to stream csv data: %w",
						fmt.Errorf("failed to upload published file to S3: %w", errS3),
					),
					log.Data{
						"bucket":       testBucket,
						"filename":     fmt.Sprintf("instances/%s.csv", testInstanceID),
						"is_published": true,
					}),
				)
			})
		})
	})

	Convey("Given an event handler with an unsuccessful cantabular client", t, func() {
		cfg := testCfg()
		c := cantabularUnhappy()
		publicS3Uploader := mock.S3UploaderMock{
			BucketNameFunc: func() string { return testBucket },
		}
		eventHandler := handler.NewInstanceComplete(cfg, &c, nil, nil, &publicS3Uploader, nil, nil, nil)

		Convey("When UploadCSVFile is triggered", func() {
			_, _, err := eventHandler.UploadCSVFile(ctx, testInstanceID, isPublished, testReq)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, handler.NewError(
					fmt.Errorf("failed to stream csv data: %w", errCantabular),
					log.Data{
						"bucket":       testBucket,
						"filename":     fmt.Sprintf("instances/%s.csv", testInstanceID),
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

	Convey("Given an event handler with a successful s3 private uploader", t, func() {
		sPrivate := mock.S3UploaderMock{HeadFunc: headOk}
		eventHandler := handler.NewInstanceComplete(testCfg(), nil, nil, &sPrivate, nil, nil, nil, nil)

		Convey("Then GetS3ContentLength returns the expected size with no error", func() {
			numBytes, err := eventHandler.GetS3ContentLength(ctx, testInstanceID, false)
			So(err, ShouldBeNil)
			So(numBytes, ShouldEqual, testNumBytes)
		})
	})

	Convey("Given an event handler with a failing s3 private uploader", t, func() {
		sPrivate := mock.S3UploaderMock{HeadFunc: headErr}
		eventHandler := handler.NewInstanceComplete(testCfg(), nil, nil, &sPrivate, nil, nil, nil, nil)

		Convey("Then GetS3ContentLength returns the expected error", func() {
			_, err := eventHandler.GetS3ContentLength(ctx, testInstanceID, false)
			So(err, ShouldResemble, fmt.Errorf("private s3 head object error: %w", errS3))
		})
	})

	Convey("Given an event handler with a successful s3 public uploader", t, func() {
		sPublic := mock.S3UploaderMock{HeadFunc: headOk}
		eventHandler := handler.NewInstanceComplete(testCfg(), nil, nil, nil, &sPublic, nil, nil, nil)

		Convey("Then GetS3ContentLength returns the expected size with no error", func() {
			numBytes, err := eventHandler.GetS3ContentLength(ctx, testInstanceID, true)
			So(err, ShouldBeNil)
			So(numBytes, ShouldEqual, testNumBytes)
		})
	})

	Convey("Given an event handler with a failing s3 public uploader", t, func() {
		sPublic := mock.S3UploaderMock{HeadFunc: headErr}
		eventHandler := handler.NewInstanceComplete(testCfg(), nil, nil, nil, &sPublic, nil, nil, nil)

		Convey("Then GetS3ContentLength returns the expected error", func() {
			_, err := eventHandler.GetS3ContentLength(ctx, testInstanceID, true)
			So(err, ShouldResemble, fmt.Errorf("public s3 head object error: %w", errS3))
		})
	})
}

func TestUpdateInstance(t *testing.T) {
	testSize := testCsvBody.Size()

	Convey("Given an event handler with a successful dataset API mock", t, func() {
		datasetAPIMock := datasetAPIClientHappy()
		eventHandler := handler.NewInstanceComplete(testCfg(), nil, &datasetAPIMock, nil, nil, nil, nil, nil)

		Convey("When UpdateInstance is called for a private csv file", func() {
			err := eventHandler.UpdateInstance(ctx, testInstanceID, testSize, false, "")

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the expected UpdateInstance call is executed with the expected paramters", func() {
				expectedURL := fmt.Sprintf("%s/downloads/instances/%s.csv", testDownloadServiceURL, testInstanceID)
				So(datasetAPIMock.GetInstanceCalls(), ShouldHaveLength, 0)
				So(datasetAPIMock.PutInstanceCalls(), ShouldHaveLength, 1)
				So(datasetAPIMock.PutInstanceCalls()[0].InstanceID, ShouldEqual, testInstanceID)
				So(datasetAPIMock.PutInstanceCalls()[0].InstanceUpdate, ShouldResemble, dataset.UpdateInstance{
					Downloads: dataset.DownloadList{
						CSV: &dataset.Download{
							URL:  expectedURL,
							Size: fmt.Sprintf("%d", testSize),
						},
					},
				})
				So(datasetAPIMock.PutInstanceCalls()[0].IfMatch, ShouldEqual, headers.IfMatchAnyETag)
			})
		})

		Convey("When UpdateInstance is called for a public csv file", func() {
			err := eventHandler.UpdateInstance(ctx, testInstanceID, testSize, true, "publicURL")

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the expected UpdateInstance call is executed once to update the public download link and associate it, and once more to publish it", func() {
				expectedURL := "publicURL"
				So(datasetAPIMock.GetInstanceCalls(), ShouldHaveLength, 0)
				So(datasetAPIMock.PutInstanceCalls(), ShouldHaveLength, 1)
				So(datasetAPIMock.PutInstanceCalls()[0].InstanceID, ShouldEqual, testInstanceID)
				So(datasetAPIMock.PutInstanceCalls()[0].InstanceUpdate, ShouldResemble, dataset.UpdateInstance{
					Downloads: dataset.DownloadList{
						CSV: &dataset.Download{
							Public: expectedURL,
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
		eventHandler := handler.NewInstanceComplete(testCfg(), nil, &datasetAPIMock, nil, nil, nil, nil, nil)

		Convey("When UpdateInstance is called", func() {
			err := eventHandler.UpdateInstance(ctx, testInstanceID, testSize, false, "")

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, fmt.Errorf("error during put instance: %w", errDataset))
			})
		})
	})
}

func TestProduceExportCompleteEvent(t *testing.T) {
	Convey("Given an event handler with a successful Kafka Producer", t, func(c C) {
		producer := kafkatest.NewMessageProducer(true)
		eventHandler := handler.NewInstanceComplete(testCfg(), nil, nil, nil, nil, nil, producer, nil)

		Convey("When ProduceExportCompleteEvent is called for a private csv file", func(c C) {
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := eventHandler.ProduceExportCompleteEvent(testInstanceID, false, "", testRowCount)
				c.So(err, ShouldBeNil)
			}()

			expectedEvent := event.CSVCreated{
				FileURL:    fmt.Sprintf("%s/downloads/instances/%s.csv", testDownloadServiceURL, testInstanceID),
				InstanceID: testInstanceID,
				RowCount:   testRowCount,
			}

			Convey("Then the expected message is produced", func() {
				producedBytes := <-producer.Channels().Output
				producedMessage := event.CSVCreated{}
				err := schema.CSVCreated.Unmarshal(producedBytes, &producedMessage)
				So(err, ShouldBeNil)
				So(producedMessage, ShouldResemble, expectedEvent)
			})

			// make sure the go-routine finishes its execution
			wg.Wait()
		})

		Convey("When ProduceExportCompleteEvent is called for a public csv file", func(c C) {
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := eventHandler.ProduceExportCompleteEvent(testInstanceID, true, "publicURL", testRowCount)
				c.So(err, ShouldBeNil)
			}()

			expectedEvent := event.CSVCreated{
				FileURL:    "publicURL",
				InstanceID: testInstanceID,
				RowCount:   testRowCount,
			}

			Convey("Then the expected message is produced", func() {
				producedBytes := <-producer.Channels().Output
				producedMessage := event.CSVCreated{}
				err := schema.CSVCreated.Unmarshal(producedBytes, &producedMessage)
				So(err, ShouldBeNil)
				So(producedMessage, ShouldResemble, expectedEvent)
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

func s3UploaderHappy(encryptionEnabled bool) mock.S3UploaderMock {
	if encryptionEnabled {
		return mock.S3UploaderMock{
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
	return mock.S3UploaderMock{
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

func s3UploaderUnhappy(encryptionEnabled bool) mock.S3UploaderMock {
	if encryptionEnabled {
		return mock.S3UploaderMock{
			UploadWithPSKFunc: func(input *s3manager.UploadInput, psk []byte) (*s3manager.UploadOutput, error) {
				return nil, errS3
			},
			BucketNameFunc: func() string {
				return testBucket
			},
		}
	}
	return mock.S3UploaderMock{
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
		PutInstanceFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, instanceID string, instanceUpdate dataset.UpdateInstance, ifMatch string) (string, error) {
			return testETag, nil
		},
	}
}

func datasetAPIClientUnhappy() mock.DatasetAPIClientMock {
	return mock.DatasetAPIClientMock{
		GetInstanceFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, instanceID string, ifMatch string) (dataset.Instance, string, error) {
			return dataset.Instance{}, "", errDataset
		},
		PutInstanceFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, instanceID string, instanceUpdate dataset.UpdateInstance, ifMatch string) (string, error) {
			return "", errDataset
		},
	}
}
