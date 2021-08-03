package handler_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/config"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/event"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/handler"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/handler/mock"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	testBucket     = "test-bucket"
	testInstanceID = "test-instance-id"
	testS3Location = "s3://myBucket/my-file.csv"
)

var (
	testCfg = config.Config{
		UploadBucketName: testBucket,
	}
	testCsvBody        = bufio.NewReader(bytes.NewReader([]byte("a,b,c,d,e,f,g,h,i,j,k,l")))
	testCsvFileContent = bufio.NewReadWriter(testCsvBody, nil)
	errS3              = errors.New("test S3Upload error")
)

var ctx = context.Background()

func TestInstanceCompleteHandler_Handle(t *testing.T) {

	Convey("Given a successful event handler", t, func() {
		ctblrClient := cantabularClientHappy()
		datasetAPIClient := datasetAPIClientHappy()
		s3Uploader := s3UploaderHappy()

		eventHandler := handler.NewInstanceComplete(testCfg, &ctblrClient, &datasetAPIClient, &s3Uploader, nil)

		Convey("Then when Handle is triggered, one Post call is performed to Dataset API for each Cantabular variable", func() {
			err := eventHandler.Handle(ctx, &event.InstanceComplete{
				InstanceID: testInstanceID,
			})
			So(err, ShouldBeNil)

			So(datasetAPIClient.GetInstanceCalls(), ShouldHaveLength, 1)
			So(datasetAPIClient.GetInstanceCalls()[0].InstanceID, ShouldResemble, testInstanceID)
		})
	})

}

func TestUploadCSVFile(t *testing.T) {

	Convey("Given an event handler with a successful S3Uploader", t, func() {
		handler.GenerateUUID = generateUUID
		s3Uploader := s3UploaderHappy()
		eventHandler := handler.NewInstanceComplete(testCfg, nil, nil, &s3Uploader, nil)

		Convey("When UploadCSVFile is triggered with valid paramters", func() {
			loc, err := eventHandler.UploadCSVFile(ctx, testInstanceID, testCsvFileContent, false)

			Convey("Then the expected location is returned with no error ", func() {
				So(err, ShouldBeNil)
				So(loc, ShouldEqual, testS3Location)
			})

			Convey("Then the expected call Upload call is executed", func() {
				So(s3Uploader.UploadCalls(), ShouldHaveLength, 1)
				expectedS3Key := fmt.Sprintf("%s-%s.csv", testInstanceID, generateUUID())
				So(*s3Uploader.UploadCalls()[0].Input.Key, ShouldResemble, expectedS3Key)
				So(*s3Uploader.UploadCalls()[0].Input.Bucket, ShouldResemble, testBucket)
				So(s3Uploader.UploadCalls()[0].Input.Body, ShouldResemble, testCsvBody)
			})
		})
	})

	Convey("Given an event handler with an unsuccessful S3Uploader", t, func() {
		handler.GenerateUUID = generateUUID
		s3Uploader := s3UploaderUnhappy()
		eventHandler := handler.NewInstanceComplete(testCfg, nil, nil, &s3Uploader, nil)

		Convey("When UploadCSVFile is triggered with an empty instanceID", func() {
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

	Convey("Given an empty event handler", t, func() {
		eventHandler := handler.NewInstanceComplete(testCfg, nil, nil, nil, nil)

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
}

func cantabularClientHappy() mock.CantabularClientMock {
	return mock.CantabularClientMock{}
}

func s3UploaderHappy() mock.S3UploaderMock {
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

func s3UploaderUnhappy() mock.S3UploaderMock {
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
	}
}

// generateUUID returns a mocked deterministic UUID for testing
var generateUUID = func() string {
	return "00000000-feed-dada-iced-c0ffee000000"
}
