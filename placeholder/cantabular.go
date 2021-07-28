// Package placeholder is a temporary package for usage during development
// of the main handler. The idea is to implement placeholder data structures
// which are use for the inputs and outputs of the various steps of the export
// process. This is to help aid development of the various steps in parallel,
// so we can have a rough idea of what he have to use for each step. As each
// step is completed we should replace the placeholder structure with the real
// input/output and the placeholder structure should be deleted. The first
// example is that placeholder.GraphQLQueryResponse will be replaced by the
// real cantabular.GraphQLQueryResponse from the dp-api-clients-go/cantabular
// package once it has been implemented. It can also be used for functions
// which are yet to be implemented in external services, the prime example
// being cantabular.GraphQLQuery()
package placeholder

import (
	"context"
)

type QueryDatasetRequest struct {
	Dataset   string
	Variables []string
}

type QueryDatasetResponse struct {
	Data struct {
		Dataset Dataset
	}
	Errors []ErrorResponse
}

type Dataset struct {
	Table Table
}

type Table struct {
	Dimensions []Dimension
	Values     []int
	Error      string
}

type Variable struct {
	Name, Label string
}

type Category struct {
	Code, Label string
}

type Row struct {
	Categories []Category
	Count      int
}

type Dimension struct {
	Count      int
	Categories []Category
	Variable   Variable
}

type ErrorResponse struct {
	Message string
}

func QueryDataset(ctx context.Context, req QueryDatasetRequest) (*QueryDatasetResponse, error) {
	return &QueryDatasetResponse{}, nil
}
