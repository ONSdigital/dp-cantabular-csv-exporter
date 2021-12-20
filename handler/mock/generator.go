// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package mock

import (
	"github.com/ONSdigital/dp-cantabular-csv-exporter/handler"
	"sync"
)

// Ensure, that GeneratorMock does implement handler.Generator.
// If this is not the case, regenerate this file with moq.
var _ handler.Generator = &GeneratorMock{}

// GeneratorMock is a mock implementation of handler.Generator.
//
// 	func TestSomethingThatUsesGenerator(t *testing.T) {
//
// 		// make and configure a mocked handler.Generator
// 		mockedGenerator := &GeneratorMock{
// 			NewPSKFunc: func() ([]byte, error) {
// 				panic("mock out the NewPSK method")
// 			},
// 		}
//
// 		// use mockedGenerator in code that requires handler.Generator
// 		// and then make assertions.
//
// 	}
type GeneratorMock struct {
	// NewPSKFunc mocks the NewPSK method.
	NewPSKFunc func() ([]byte, error)

	// calls tracks calls to the methods.
	calls struct {
		// NewPSK holds details about calls to the NewPSK method.
		NewPSK []struct {
		}
	}
	lockNewPSK sync.RWMutex
}

// NewPSK calls NewPSKFunc.
func (mock *GeneratorMock) NewPSK() ([]byte, error) {
	if mock.NewPSKFunc == nil {
		panic("GeneratorMock.NewPSKFunc: method is nil but Generator.NewPSK was just called")
	}
	callInfo := struct {
	}{}
	mock.lockNewPSK.Lock()
	mock.calls.NewPSK = append(mock.calls.NewPSK, callInfo)
	mock.lockNewPSK.Unlock()
	return mock.NewPSKFunc()
}

// NewPSKCalls gets all the calls that were made to NewPSK.
// Check the length with:
//     len(mockedGenerator.NewPSKCalls())
func (mock *GeneratorMock) NewPSKCalls() []struct {
} {
	var calls []struct {
	}
	mock.lockNewPSK.RLock()
	calls = mock.calls.NewPSK
	mock.lockNewPSK.RUnlock()
	return calls
}
