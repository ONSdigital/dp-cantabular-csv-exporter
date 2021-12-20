// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package mock

import (
	"context"
	"github.com/ONSdigital/dp-api-clients-go/v2/cantabular"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/handler"
	"io"
	"sync"
)

// Ensure, that CantabularClientMock does implement handler.CantabularClient.
// If this is not the case, regenerate this file with moq.
var _ handler.CantabularClient = &CantabularClientMock{}

// CantabularClientMock is a mock implementation of handler.CantabularClient.
//
// 	func TestSomethingThatUsesCantabularClient(t *testing.T) {
//
// 		// make and configure a mocked handler.CantabularClient
// 		mockedCantabularClient := &CantabularClientMock{
// 			StaticDatasetQueryStreamCSVFunc: func(ctx context.Context, req cantabular.StaticDatasetQueryRequest, consume func(ctx context.Context, r io.Reader) error) (int32, error) {
// 				panic("mock out the StaticDatasetQueryStreamCSV method")
// 			},
// 		}
//
// 		// use mockedCantabularClient in code that requires handler.CantabularClient
// 		// and then make assertions.
//
// 	}
type CantabularClientMock struct {
	// StaticDatasetQueryStreamCSVFunc mocks the StaticDatasetQueryStreamCSV method.
	StaticDatasetQueryStreamCSVFunc func(ctx context.Context, req cantabular.StaticDatasetQueryRequest, consume func(ctx context.Context, r io.Reader) error) (int32, error)

	// calls tracks calls to the methods.
	calls struct {
		// StaticDatasetQueryStreamCSV holds details about calls to the StaticDatasetQueryStreamCSV method.
		StaticDatasetQueryStreamCSV []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// Req is the req argument value.
			Req cantabular.StaticDatasetQueryRequest
			// Consume is the consume argument value.
			Consume func(ctx context.Context, r io.Reader) error
		}
	}
	lockStaticDatasetQueryStreamCSV sync.RWMutex
}

// StaticDatasetQueryStreamCSV calls StaticDatasetQueryStreamCSVFunc.
func (mock *CantabularClientMock) StaticDatasetQueryStreamCSV(ctx context.Context, req cantabular.StaticDatasetQueryRequest, consume func(ctx context.Context, r io.Reader) error) (int32, error) {
	if mock.StaticDatasetQueryStreamCSVFunc == nil {
		panic("CantabularClientMock.StaticDatasetQueryStreamCSVFunc: method is nil but CantabularClient.StaticDatasetQueryStreamCSV was just called")
	}
	callInfo := struct {
		Ctx     context.Context
		Req     cantabular.StaticDatasetQueryRequest
		Consume func(ctx context.Context, r io.Reader) error
	}{
		Ctx:     ctx,
		Req:     req,
		Consume: consume,
	}
	mock.lockStaticDatasetQueryStreamCSV.Lock()
	mock.calls.StaticDatasetQueryStreamCSV = append(mock.calls.StaticDatasetQueryStreamCSV, callInfo)
	mock.lockStaticDatasetQueryStreamCSV.Unlock()
	return mock.StaticDatasetQueryStreamCSVFunc(ctx, req, consume)
}

// StaticDatasetQueryStreamCSVCalls gets all the calls that were made to StaticDatasetQueryStreamCSV.
// Check the length with:
//     len(mockedCantabularClient.StaticDatasetQueryStreamCSVCalls())
func (mock *CantabularClientMock) StaticDatasetQueryStreamCSVCalls() []struct {
	Ctx     context.Context
	Req     cantabular.StaticDatasetQueryRequest
	Consume func(ctx context.Context, r io.Reader) error
} {
	var calls []struct {
		Ctx     context.Context
		Req     cantabular.StaticDatasetQueryRequest
		Consume func(ctx context.Context, r io.Reader) error
	}
	mock.lockStaticDatasetQueryStreamCSV.RLock()
	calls = mock.calls.StaticDatasetQueryStreamCSV
	mock.lockStaticDatasetQueryStreamCSV.RUnlock()
	return calls
}
