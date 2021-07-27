package event

import (
	"context"

	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
)

//go:generate moq -out mock/cantabular-client.go -pkg mock . CantabularClient
//go:generate moq -out mock/dataset-api-client.go -pkg mock . DatasetAPIClient
//go:generate moq -out mock/s3-client.go -pkg mock . S3Client
//go:generate moq -out mock/handler.go -pkg mock . Handler

type Handler interface {
	Handle(ctx context.Context, event *InstanceComplete) error
}

// CantabularClient contains the required method for the Cantabular Client
type CantabularClient interface {
}

// CantabularClient contains the required method for the S3 Client
type S3Client interface {
}

// DatasetAPIClient contains the required method for the Dataset API Client
type DatasetAPIClient interface {
	GetInstance(ctx context.Context, userAuthToken, serviceAuthToken, collectionID, instanceID, ifMatch string) (m dataset.Instance, eTag string, err error)
}

type dataLogger interface {
	LogData() map[string]interface{}
}

type coder interface {
	Code() int
}
