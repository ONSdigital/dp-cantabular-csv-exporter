// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package mock

import (
	"context"
	"github.com/ONSdigital/dp-api-clients-go/v2/filter"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/service"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"sync"
)

// Ensure, that FilterAPIClientMock does implement service.FilterAPIClient.
// If this is not the case, regenerate this file with moq.
var _ service.FilterAPIClient = &FilterAPIClientMock{}

// FilterAPIClientMock is a mock implementation of service.FilterAPIClient.
//
// 	func TestSomethingThatUsesFilterAPIClient(t *testing.T) {
//
// 		// make and configure a mocked service.FilterAPIClient
// 		mockedFilterAPIClient := &FilterAPIClientMock{
// 			CheckerFunc: func(contextMoqParam context.Context, checkState *healthcheck.CheckState) error {
// 				panic("mock out the Checker method")
// 			},
// 			GetDimensionsFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, filterID string, q *filter.QueryParams) (filter.Dimensions, string, error) {
// 				panic("mock out the GetDimensions method")
// 			},
// 			GetOutputFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, downloadServiceToken string, collectionID string, filterOutputID string) (filter.Model, error) {
// 				panic("mock out the GetOutput method")
// 			},
// 			UpdateFilterOutputFunc: func(ctx context.Context, userAuthToken string, serviceAuthToken string, downloadServiceToken string, filterOutputID string, m *filter.Model) error {
// 				panic("mock out the UpdateFilterOutput method")
// 			},
// 		}
//
// 		// use mockedFilterAPIClient in code that requires service.FilterAPIClient
// 		// and then make assertions.
//
// 	}
type FilterAPIClientMock struct {
	// CheckerFunc mocks the Checker method.
	CheckerFunc func(contextMoqParam context.Context, checkState *healthcheck.CheckState) error

	// GetDimensionsFunc mocks the GetDimensions method.
	GetDimensionsFunc func(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, filterID string, q *filter.QueryParams) (filter.Dimensions, string, error)

	// GetOutputFunc mocks the GetOutput method.
	GetOutputFunc func(ctx context.Context, userAuthToken string, serviceAuthToken string, downloadServiceToken string, collectionID string, filterOutputID string) (filter.Model, error)

	// UpdateFilterOutputFunc mocks the UpdateFilterOutput method.
	UpdateFilterOutputFunc func(ctx context.Context, userAuthToken string, serviceAuthToken string, downloadServiceToken string, filterOutputID string, m *filter.Model) error

	// calls tracks calls to the methods.
	calls struct {
		// Checker holds details about calls to the Checker method.
		Checker []struct {
			// ContextMoqParam is the contextMoqParam argument value.
			ContextMoqParam context.Context
			// CheckState is the checkState argument value.
			CheckState *healthcheck.CheckState
		}
		// GetDimensions holds details about calls to the GetDimensions method.
		GetDimensions []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// UserAuthToken is the userAuthToken argument value.
			UserAuthToken string
			// ServiceAuthToken is the serviceAuthToken argument value.
			ServiceAuthToken string
			// CollectionID is the collectionID argument value.
			CollectionID string
			// FilterID is the filterID argument value.
			FilterID string
			// Q is the q argument value.
			Q *filter.QueryParams
		}
		// GetOutput holds details about calls to the GetOutput method.
		GetOutput []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// UserAuthToken is the userAuthToken argument value.
			UserAuthToken string
			// ServiceAuthToken is the serviceAuthToken argument value.
			ServiceAuthToken string
			// DownloadServiceToken is the downloadServiceToken argument value.
			DownloadServiceToken string
			// CollectionID is the collectionID argument value.
			CollectionID string
			// FilterOutputID is the filterOutputID argument value.
			FilterOutputID string
		}
		// UpdateFilterOutput holds details about calls to the UpdateFilterOutput method.
		UpdateFilterOutput []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// UserAuthToken is the userAuthToken argument value.
			UserAuthToken string
			// ServiceAuthToken is the serviceAuthToken argument value.
			ServiceAuthToken string
			// DownloadServiceToken is the downloadServiceToken argument value.
			DownloadServiceToken string
			// FilterOutputID is the filterOutputID argument value.
			FilterOutputID string
			// M is the m argument value.
			M *filter.Model
		}
	}
	lockChecker            sync.RWMutex
	lockGetDimensions      sync.RWMutex
	lockGetOutput          sync.RWMutex
	lockUpdateFilterOutput sync.RWMutex
}

// Checker calls CheckerFunc.
func (mock *FilterAPIClientMock) Checker(contextMoqParam context.Context, checkState *healthcheck.CheckState) error {
	if mock.CheckerFunc == nil {
		panic("FilterAPIClientMock.CheckerFunc: method is nil but FilterAPIClient.Checker was just called")
	}
	callInfo := struct {
		ContextMoqParam context.Context
		CheckState      *healthcheck.CheckState
	}{
		ContextMoqParam: contextMoqParam,
		CheckState:      checkState,
	}
	mock.lockChecker.Lock()
	mock.calls.Checker = append(mock.calls.Checker, callInfo)
	mock.lockChecker.Unlock()
	return mock.CheckerFunc(contextMoqParam, checkState)
}

// CheckerCalls gets all the calls that were made to Checker.
// Check the length with:
//     len(mockedFilterAPIClient.CheckerCalls())
func (mock *FilterAPIClientMock) CheckerCalls() []struct {
	ContextMoqParam context.Context
	CheckState      *healthcheck.CheckState
} {
	var calls []struct {
		ContextMoqParam context.Context
		CheckState      *healthcheck.CheckState
	}
	mock.lockChecker.RLock()
	calls = mock.calls.Checker
	mock.lockChecker.RUnlock()
	return calls
}

// GetDimensions calls GetDimensionsFunc.
func (mock *FilterAPIClientMock) GetDimensions(ctx context.Context, userAuthToken string, serviceAuthToken string, collectionID string, filterID string, q *filter.QueryParams) (filter.Dimensions, string, error) {
	if mock.GetDimensionsFunc == nil {
		panic("FilterAPIClientMock.GetDimensionsFunc: method is nil but FilterAPIClient.GetDimensions was just called")
	}
	callInfo := struct {
		Ctx              context.Context
		UserAuthToken    string
		ServiceAuthToken string
		CollectionID     string
		FilterID         string
		Q                *filter.QueryParams
	}{
		Ctx:              ctx,
		UserAuthToken:    userAuthToken,
		ServiceAuthToken: serviceAuthToken,
		CollectionID:     collectionID,
		FilterID:         filterID,
		Q:                q,
	}
	mock.lockGetDimensions.Lock()
	mock.calls.GetDimensions = append(mock.calls.GetDimensions, callInfo)
	mock.lockGetDimensions.Unlock()
	return mock.GetDimensionsFunc(ctx, userAuthToken, serviceAuthToken, collectionID, filterID, q)
}

// GetDimensionsCalls gets all the calls that were made to GetDimensions.
// Check the length with:
//     len(mockedFilterAPIClient.GetDimensionsCalls())
func (mock *FilterAPIClientMock) GetDimensionsCalls() []struct {
	Ctx              context.Context
	UserAuthToken    string
	ServiceAuthToken string
	CollectionID     string
	FilterID         string
	Q                *filter.QueryParams
} {
	var calls []struct {
		Ctx              context.Context
		UserAuthToken    string
		ServiceAuthToken string
		CollectionID     string
		FilterID         string
		Q                *filter.QueryParams
	}
	mock.lockGetDimensions.RLock()
	calls = mock.calls.GetDimensions
	mock.lockGetDimensions.RUnlock()
	return calls
}

// GetOutput calls GetOutputFunc.
func (mock *FilterAPIClientMock) GetOutput(ctx context.Context, userAuthToken string, serviceAuthToken string, downloadServiceToken string, collectionID string, filterOutputID string) (filter.Model, error) {
	if mock.GetOutputFunc == nil {
		panic("FilterAPIClientMock.GetOutputFunc: method is nil but FilterAPIClient.GetOutput was just called")
	}
	callInfo := struct {
		Ctx                  context.Context
		UserAuthToken        string
		ServiceAuthToken     string
		DownloadServiceToken string
		CollectionID         string
		FilterOutputID       string
	}{
		Ctx:                  ctx,
		UserAuthToken:        userAuthToken,
		ServiceAuthToken:     serviceAuthToken,
		DownloadServiceToken: downloadServiceToken,
		CollectionID:         collectionID,
		FilterOutputID:       filterOutputID,
	}
	mock.lockGetOutput.Lock()
	mock.calls.GetOutput = append(mock.calls.GetOutput, callInfo)
	mock.lockGetOutput.Unlock()
	return mock.GetOutputFunc(ctx, userAuthToken, serviceAuthToken, downloadServiceToken, collectionID, filterOutputID)
}

// GetOutputCalls gets all the calls that were made to GetOutput.
// Check the length with:
//     len(mockedFilterAPIClient.GetOutputCalls())
func (mock *FilterAPIClientMock) GetOutputCalls() []struct {
	Ctx                  context.Context
	UserAuthToken        string
	ServiceAuthToken     string
	DownloadServiceToken string
	CollectionID         string
	FilterOutputID       string
} {
	var calls []struct {
		Ctx                  context.Context
		UserAuthToken        string
		ServiceAuthToken     string
		DownloadServiceToken string
		CollectionID         string
		FilterOutputID       string
	}
	mock.lockGetOutput.RLock()
	calls = mock.calls.GetOutput
	mock.lockGetOutput.RUnlock()
	return calls
}

// UpdateFilterOutput calls UpdateFilterOutputFunc.
func (mock *FilterAPIClientMock) UpdateFilterOutput(ctx context.Context, userAuthToken string, serviceAuthToken string, downloadServiceToken string, filterOutputID string, m *filter.Model) error {
	if mock.UpdateFilterOutputFunc == nil {
		panic("FilterAPIClientMock.UpdateFilterOutputFunc: method is nil but FilterAPIClient.UpdateFilterOutput was just called")
	}
	callInfo := struct {
		Ctx                  context.Context
		UserAuthToken        string
		ServiceAuthToken     string
		DownloadServiceToken string
		FilterOutputID       string
		M                    *filter.Model
	}{
		Ctx:                  ctx,
		UserAuthToken:        userAuthToken,
		ServiceAuthToken:     serviceAuthToken,
		DownloadServiceToken: downloadServiceToken,
		FilterOutputID:       filterOutputID,
		M:                    m,
	}
	mock.lockUpdateFilterOutput.Lock()
	mock.calls.UpdateFilterOutput = append(mock.calls.UpdateFilterOutput, callInfo)
	mock.lockUpdateFilterOutput.Unlock()
	return mock.UpdateFilterOutputFunc(ctx, userAuthToken, serviceAuthToken, downloadServiceToken, filterOutputID, m)
}

// UpdateFilterOutputCalls gets all the calls that were made to UpdateFilterOutput.
// Check the length with:
//     len(mockedFilterAPIClient.UpdateFilterOutputCalls())
func (mock *FilterAPIClientMock) UpdateFilterOutputCalls() []struct {
	Ctx                  context.Context
	UserAuthToken        string
	ServiceAuthToken     string
	DownloadServiceToken string
	FilterOutputID       string
	M                    *filter.Model
} {
	var calls []struct {
		Ctx                  context.Context
		UserAuthToken        string
		ServiceAuthToken     string
		DownloadServiceToken string
		FilterOutputID       string
		M                    *filter.Model
	}
	mock.lockUpdateFilterOutput.RLock()
	calls = mock.calls.UpdateFilterOutput
	mock.lockUpdateFilterOutput.RUnlock()
	return calls
}
