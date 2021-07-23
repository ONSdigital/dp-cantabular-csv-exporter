package event_test

import (
	"context"
	"testing"

	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/config"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/event"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/event/mock"
	. "github.com/smartystreets/goconvey/convey"
)

var testCfg = config.Config{
	ServiceAuthToken: "testToken",
}

var ctx = context.Background()

func TestInstanceCompleteHandler_Handle(t *testing.T) {

	Convey("Given a successful event handler", t, func() {
		ctblrClient := cantabularClientHappy()
		datasetAPIClient := datasetAPIClientHappy()
		s3Client := s3ClientHappy()

		eventHandler := event.NewInstanceCompleteHandler(testCfg, &ctblrClient, &datasetAPIClient, &s3Client)

		Convey("Then when Handle is triggered, one Post call is performed to Dataset API for each Cantabular variable", func() {
			err := eventHandler.Handle(ctx, &testCfg, &event.InstanceComplete{
				InstanceID: "test-instance-id",
			})
			So(err, ShouldBeNil)

			So(datasetAPIClient.GetInstanceCalls(), ShouldHaveLength, 1)
			So(datasetAPIClient.GetInstanceCalls()[0].InstanceID, ShouldResemble, "test-instance-id")

		})
	})

}

func cantabularClientHappy() mock.CantabularClientMock {
	return mock.CantabularClientMock{}
}

func s3ClientHappy() mock.S3ClientMock {
	return mock.S3ClientMock{}
}

func datasetAPIClientHappy() mock.DatasetAPIClientMock {
	return mock.DatasetAPIClientMock{
		GetInstanceFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, instanceID string, ifMatch string) (dataset.Instance, string, error) {
			return dataset.Instance{}, "", nil
		},
	}
}
