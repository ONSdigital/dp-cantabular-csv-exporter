package handler

import (
	"context"

	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

//go:generate moq -out mock/cantabular-client.go -pkg mock . CantabularClient
//go:generate moq -out mock/dataset-api-client.go -pkg mock . DatasetAPIClient
//go:generate moq -out mock/s3-client.go -pkg mock . S3Uploader

// CantabularClient contains the required method for the Cantabular Client
type CantabularClient interface {
}

// S3Uploader contains the required method for the S3 Uploader
type S3Uploader interface {
	Upload(input *s3manager.UploadInput, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error)
	BucketName() string
}

// DatasetAPIClient contains the required method for the Dataset API Client
type DatasetAPIClient interface {
	GetInstance(ctx context.Context, userAuthToken, serviceAuthToken, collectionID, instanceID, ifMatch string) (m dataset.Instance, eTag string, err error)
}
