package handler_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-api-clients-go/v2/headers"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/config"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/event"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/handler"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/handler/mock"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/schema"
	"github.com/ONSdigital/dp-kafka/v2/kafkatest"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	testBucket     = "test-bucket"
	testVaultPath  = "vault-root"
	testInstanceID = "test-instance-id"
	testS3Location = "s3://myBucket/my-file.csv"
	testETag       = "testETag"
)

var (
	testCfg = config.Config{
		UploadBucketName: testBucket,
		VaultPath:        testVaultPath,
	}
	testCsvBody        = bufio.NewReader(bytes.NewReader([]byte("a,b,c,d,e,f,g,h,i,j,k,l")))
	testCsvFileContent = bufio.NewReadWriter(testCsvBody, nil)
	testPsk            = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	errS3              = errors.New("test S3Upload error")
	errVault           = errors.New("test Vault error")
	errPsk             = errors.New("test PSK error")
	errDataset         = errors.New("test DatasetAPI error")
)

var originalCreatePSK = handler.CreatePSK

var ctx = context.Background()

// TODO uncomment and finish implementing test after the whole functionality for the handler is implemented
// func TestInstanceCompleteHandler_Handle(t *testing.T) {

// 	Convey("Given a successful event handler", t, func() {
// 		ctblrClient := cantabularClientHappy()
// 		datasetAPIClient := datasetAPIClientHappy()
// 		s3Uploader := s3UploaderHappy(false)
// 		producer := &kafkatest.IProducerMock{}

// 		eventHandler := handler.NewInstanceComplete(testCfg, &ctblrClient, &datasetAPIClient, &s3Uploader, nil, producer)

// 		Convey("Then when Handle is triggered, the instance is read from dataset api", func() {
// 			err := eventHandler.Handle(ctx, &event.InstanceComplete{
// 				InstanceID: testInstanceID,
// 			})
// 			So(err, ShouldBeNil)

// 			So(datasetAPIClient.GetInstanceCalls(), ShouldHaveLength, 1)
// 			So(datasetAPIClient.GetInstanceCalls()[0].InstanceID, ShouldResemble, testInstanceID)
// 		})
// 	})

// }

func TestUploadCSVFile(t *testing.T) {

	expectedS3Key := fmt.Sprintf("%s-%s.csv", testInstanceID, generateUUID())
	expectedVaultPath := fmt.Sprintf("%s/%s", testVaultPath, expectedS3Key)

	Convey("Given an event handler with a successful S3Uploader", t, func() {
		handler.GenerateUUID = generateUUID
		s3Uploader := s3UploaderHappy(false)
		eventHandler := handler.NewInstanceComplete(testCfg, nil, nil, &s3Uploader, nil, nil)

		Convey("When UploadCSVFile is triggered with valid paramters and encryption disbled", func() {
			loc, err := eventHandler.UploadCSVFile(ctx, testInstanceID, testCsvFileContent, false)

			Convey("Then the expected location is returned with no error ", func() {
				So(err, ShouldBeNil)
				So(loc, ShouldEqual, testS3Location)
			})

			Convey("Then the expected call Upload call is executed", func() {
				So(s3Uploader.UploadCalls(), ShouldHaveLength, 1)
				So(*s3Uploader.UploadCalls()[0].Input.Key, ShouldResemble, expectedS3Key)
				So(*s3Uploader.UploadCalls()[0].Input.Bucket, ShouldResemble, testBucket)
				So(s3Uploader.UploadCalls()[0].Input.Body, ShouldResemble, testCsvBody)
			})
		})
	})

	Convey("Given an event handler with a successful S3Uploader and Vault client", t, func() {
		handler.GenerateUUID = generateUUID
		handler.CreatePSK = createPSK
		s3Uploader := s3UploaderHappy(true)
		vaultClient := vaultHappy()
		eventHandler := handler.NewInstanceComplete(testCfg, nil, nil, &s3Uploader, &vaultClient, nil)

		Convey("When UploadCSVFile is triggered with valid paramters and encryption enabled", func() {
			loc, err := eventHandler.UploadCSVFile(ctx, testInstanceID, testCsvFileContent, true)

			Convey("Then the expected location is returned with no error ", func() {
				So(err, ShouldBeNil)
				So(loc, ShouldEqual, testS3Location)
			})

			Convey("Then the expected key is stored in vault", func() {
				So(vaultClient.WriteKeyCalls(), ShouldHaveLength, 1)
				expectedPsk := hex.EncodeToString(testPsk)
				So(vaultClient.WriteKeyCalls()[0].Path, ShouldResemble, expectedVaultPath)
				So(vaultClient.WriteKeyCalls()[0].Key, ShouldResemble, "key")
				So(vaultClient.WriteKeyCalls()[0].Value, ShouldResemble, expectedPsk)
			})

			Convey("Then the expected call UploadWithPSK call is executed with the expected psk", func() {
				So(s3Uploader.UploadWithPSKCalls(), ShouldHaveLength, 1)
				expectedS3Key := fmt.Sprintf("%s-%s.csv", testInstanceID, generateUUID())
				So(*s3Uploader.UploadWithPSKCalls()[0].Input.Key, ShouldResemble, expectedS3Key)
				So(*s3Uploader.UploadWithPSKCalls()[0].Input.Bucket, ShouldResemble, testBucket)
				So(s3Uploader.UploadWithPSKCalls()[0].Input.Body, ShouldResemble, testCsvBody)
				So(s3Uploader.UploadWithPSKCalls()[0].Psk, ShouldResemble, testPsk)
			})
		})
	})

	Convey("Given an event handler with an unsuccessful S3Uploader", t, func() {
		handler.GenerateUUID = generateUUID
		s3Uploader := s3UploaderUnhappy(false)
		eventHandler := handler.NewInstanceComplete(testCfg, nil, nil, &s3Uploader, nil, nil)

		Convey("When UploadCSVFile is triggered with encryption disabled", func() {
			_, err := eventHandler.UploadCSVFile(ctx, testInstanceID, testCsvFileContent, false)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, handler.NewError(
					fmt.Errorf("failed to upload file to S3: %w", errS3),
					log.Data{
						"bucket":    testBucket,
						"filename":  fmt.Sprintf("%s-%s.csv", testInstanceID, generateUUID()),
						"encrypted": false,
					},
				))
			})
		})
	})

	Convey("Given an event handler with an unsuccessful Vault client", t, func() {
		handler.GenerateUUID = generateUUID
		s3Uploader := mock.S3UploaderMock{
			BucketNameFunc: func() string { return testCfg.UploadBucketName },
		}
		vaultClient := vaultUnhappy()
		eventHandler := handler.NewInstanceComplete(testCfg, nil, nil, &s3Uploader, &vaultClient, nil)

		Convey("When UploadCSVFile is triggered with encryption enabled", func() {
			_, err := eventHandler.UploadCSVFile(ctx, testInstanceID, testCsvFileContent, true)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, handler.NewError(
					fmt.Errorf("failed to write key to vault: %w", errVault),
					log.Data{
						"bucket":    testBucket,
						"filename":  fmt.Sprintf("%s-%s.csv", testInstanceID, generateUUID()),
						"encrypted": true,
					},
				))
			})
		})
	})

	Convey("Given an event handler with a successful Vault client and unsuccessful S3 client", t, func() {
		handler.GenerateUUID = generateUUID
		s3Uploader := s3UploaderUnhappy(true)
		vaultClient := vaultHappy()
		eventHandler := handler.NewInstanceComplete(testCfg, nil, nil, &s3Uploader, &vaultClient, nil)

		Convey("When UploadCSVFile is triggered with encryption enabled", func() {
			_, err := eventHandler.UploadCSVFile(ctx, testInstanceID, testCsvFileContent, true)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, handler.NewError(
					fmt.Errorf("failed to upload file to S3: %w", errS3),
					log.Data{
						"bucket":    testBucket,
						"filename":  fmt.Sprintf("%s-%s.csv", testInstanceID, generateUUID()),
						"encrypted": true,
					},
				))
			})
		})
	})

	Convey("Given an empty event handler", t, func() {
		eventHandler := handler.NewInstanceComplete(testCfg, nil, nil, nil, nil, nil)

		Convey("When UploadCSVFile is triggered with an empty instanceID", func() {
			_, err := eventHandler.UploadCSVFile(ctx, "", testCsvFileContent, false)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, errors.New("empty instance id not allowed"))
			})
		})

		Convey("When UploadCSVFile is triggered with a nil csv reader", func() {
			_, err := eventHandler.UploadCSVFile(ctx, testInstanceID, nil, false)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, errors.New("no file content has been provided"))
			})
		})
	})

	Convey("Given an event handler and a failing createPSK function", t, func() {
		s3Uploader := mock.S3UploaderMock{
			BucketNameFunc: func() string { return testCfg.UploadBucketName },
		}
		handler.CreatePSK = func() ([]byte, error) {
			return nil, errPsk
		}
		defer func() { handler.CreatePSK = createPSK }()
		eventHandler := handler.NewInstanceComplete(testCfg, nil, nil, &s3Uploader, nil, nil)

		Convey("When UploadCSVFile is triggered with valid paramters and encryption enabled", func() {
			_, err := eventHandler.UploadCSVFile(ctx, testInstanceID, testCsvFileContent, true)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, fmt.Sprintf("failed to generate a PSK for encryption: %s", errPsk.Error()))
			})
		})
	})
}

func TestUpdateInstance(t *testing.T) {
	testSize := testCsvFileContent.Reader.Size()

	Convey("Given an event handler with a successful dataset API mock", t, func() {
		datasetAPIMock := datasetAPIClientHappy()
		eventHandler := handler.NewInstanceComplete(testCfg, nil, &datasetAPIMock, nil, nil, nil)

		Convey("When UpdateInstance is called", func() {
			err := eventHandler.UpdateInstance(ctx, testInstanceID, testS3Location, testSize)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the expected UpdateInstance call is executed with the expected paramters", func() {
				So(datasetAPIMock.GetInstanceCalls(), ShouldHaveLength, 0)
				So(datasetAPIMock.PutInstanceCalls(), ShouldHaveLength, 1)
				So(datasetAPIMock.PutInstanceCalls()[0].InstanceID, ShouldEqual, testInstanceID)
				So(datasetAPIMock.PutInstanceCalls()[0].InstanceUpdate, ShouldResemble, dataset.UpdateInstance{
					Downloads: dataset.DownloadList{
						CSV: &dataset.Download{
							URL:  testS3Location,
							Size: fmt.Sprintf("%d", testSize),
						},
					},
				})
				So(datasetAPIMock.PutInstanceCalls()[0].IfMatch, ShouldEqual, headers.IfMatchAnyETag)
			})
		})
	})

	Convey("Given an event handler with a failing dataset API mock", t, func() {
		datasetAPIMock := datasetAPIClientUnhappy()
		eventHandler := handler.NewInstanceComplete(testCfg, nil, &datasetAPIMock, nil, nil, nil)

		Convey("When UpdateInstance is called", func() {
			err := eventHandler.UpdateInstance(ctx, testInstanceID, testS3Location, testSize)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldResemble, errDataset)
			})
		})
	})
}

func TestProduceExportCompleteEvent(t *testing.T) {
	expectedEvent := event.CommonOutputCreated{
		FileURL:    testS3Location,
		InstanceID: testInstanceID,
	}

	Convey("Given an event handler with a successful Kafka Producer", t, func(c C) {
		producer := kafkatest.NewMessageProducer(true)
		eventHandler := handler.NewInstanceComplete(testCfg, nil, nil, nil, nil, producer)

		Convey("When UpdateInstance is called", func(c C) {
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := eventHandler.ProduceExportCompleteEvent(testInstanceID, testS3Location)
				c.So(err, ShouldBeNil)
			}()

			Convey("Then the expected message is produced", func() {
				producedBytes := <-producer.Channels().Output
				producedMessage := event.CommonOutputCreated{}
				err := schema.CommonOutputCreated.Unmarshal(producedBytes, &producedMessage)
				So(err, ShouldBeNil)
				So(producedMessage, ShouldResemble, expectedEvent)
			})

			// make sure the go-routine finishes its execution
			wg.Wait()
		})
	})
}

func TestCreatePSK(t *testing.T) {
	Convey("CreatePSK should return a byte array of size 16 with no error", t, func() {
		psk, err := originalCreatePSK()
		So(err, ShouldBeNil)
		So(psk, ShouldHaveLength, 16)

		Convey("Runing CreatePSK again should return a different value", func() {
			psk1, err1 := originalCreatePSK()
			So(err1, ShouldBeNil)
			So(psk1, ShouldNotResemble, psk)
		})
	})
}

func cantabularClientHappy() mock.CantabularClientMock {
	return mock.CantabularClientMock{}
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
				return testCfg.UploadBucketName
			},
		}
	}
	return mock.S3UploaderMock{
		UploadFunc: func(input *s3manager.UploadInput, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
			return &s3manager.UploadOutput{
				Location: testS3Location,
			}, nil
		},
		BucketNameFunc: func() string {
			return testCfg.UploadBucketName
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
				return testCfg.UploadBucketName
			},
		}
	}
	return mock.S3UploaderMock{
		UploadFunc: func(input *s3manager.UploadInput, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
			return nil, errS3
		},
		BucketNameFunc: func() string {
			return testCfg.UploadBucketName
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

// generateUUID returns a mocked deterministic UUID for testing
var generateUUID = func() string {
	return "00000000-feed-dada-iced-c0ffee000000"
}

// createPSK returns a mocked array of 16 bytes
var createPSK = func() ([]byte, error) {
	return testPsk, nil
}
