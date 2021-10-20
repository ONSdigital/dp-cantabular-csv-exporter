// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package mock

import (
	"context"
	"github.com/ONSdigital/dp-healthcheck/v2/healthcheck"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"sync"
)

var (
	lockS3UploaderMockBucketName    sync.RWMutex
	lockS3UploaderMockChecker       sync.RWMutex
	lockS3UploaderMockGet           sync.RWMutex
	lockS3UploaderMockUpload        sync.RWMutex
	lockS3UploaderMockUploadWithPSK sync.RWMutex
)

// S3UploaderMock is a mock implementation of service.S3Uploader.
//
//     func TestSomethingThatUsesS3Uploader(t *testing.T) {
//
//         // make and configure a mocked service.S3Uploader
//         mockedS3Uploader := &S3UploaderMock{
//             BucketNameFunc: func() string {
// 	               panic("mock out the BucketName method")
//             },
//             CheckerFunc: func(in1 context.Context, in2 *healthcheck.CheckState) error {
// 	               panic("mock out the Checker method")
//             },
//             GetFunc: func(key string) (io.ReadCloser, *int64, error) {
// 	               panic("mock out the Get method")
//             },
//             UploadFunc: func(input *s3manager.UploadInput, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
// 	               panic("mock out the Upload method")
//             },
//             UploadWithPSKFunc: func(input *s3manager.UploadInput, psk []byte) (*s3manager.UploadOutput, error) {
// 	               panic("mock out the UploadWithPSK method")
//             },
//         }
//
//         // use mockedS3Uploader in code that requires service.S3Uploader
//         // and then make assertions.
//
//     }
type S3UploaderMock struct {
	// BucketNameFunc mocks the BucketName method.
	BucketNameFunc func() string

	// CheckerFunc mocks the Checker method.
	CheckerFunc func(in1 context.Context, in2 *healthcheck.CheckState) error

	// GetFunc mocks the Get method.
	GetFunc func(key string) (io.ReadCloser, *int64, error)

	// UploadFunc mocks the Upload method.
	UploadFunc func(input *s3manager.UploadInput, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error)

	// UploadWithPSKFunc mocks the UploadWithPSK method.
	UploadWithPSKFunc func(input *s3manager.UploadInput, psk []byte) (*s3manager.UploadOutput, error)

	// calls tracks calls to the methods.
	calls struct {
		// BucketName holds details about calls to the BucketName method.
		BucketName []struct {
		}
		// Checker holds details about calls to the Checker method.
		Checker []struct {
			// In1 is the in1 argument value.
			In1 context.Context
			// In2 is the in2 argument value.
			In2 *healthcheck.CheckState
		}
		// Get holds details about calls to the Get method.
		Get []struct {
			// Key is the key argument value.
			Key string
		}
		// Upload holds details about calls to the Upload method.
		Upload []struct {
			// Input is the input argument value.
			Input *s3manager.UploadInput
			// Options is the options argument value.
			Options []func(*s3manager.Uploader)
		}
		// UploadWithPSK holds details about calls to the UploadWithPSK method.
		UploadWithPSK []struct {
			// Input is the input argument value.
			Input *s3manager.UploadInput
			// Psk is the psk argument value.
			Psk []byte
		}
	}
}

// BucketName calls BucketNameFunc.
func (mock *S3UploaderMock) BucketName() string {
	if mock.BucketNameFunc == nil {
		panic("S3UploaderMock.BucketNameFunc: method is nil but S3Uploader.BucketName was just called")
	}
	callInfo := struct {
	}{}
	lockS3UploaderMockBucketName.Lock()
	mock.calls.BucketName = append(mock.calls.BucketName, callInfo)
	lockS3UploaderMockBucketName.Unlock()
	return mock.BucketNameFunc()
}

// BucketNameCalls gets all the calls that were made to BucketName.
// Check the length with:
//     len(mockedS3Uploader.BucketNameCalls())
func (mock *S3UploaderMock) BucketNameCalls() []struct {
} {
	var calls []struct {
	}
	lockS3UploaderMockBucketName.RLock()
	calls = mock.calls.BucketName
	lockS3UploaderMockBucketName.RUnlock()
	return calls
}

// Checker calls CheckerFunc.
func (mock *S3UploaderMock) Checker(in1 context.Context, in2 *healthcheck.CheckState) error {
	if mock.CheckerFunc == nil {
		panic("S3UploaderMock.CheckerFunc: method is nil but S3Uploader.Checker was just called")
	}
	callInfo := struct {
		In1 context.Context
		In2 *healthcheck.CheckState
	}{
		In1: in1,
		In2: in2,
	}
	lockS3UploaderMockChecker.Lock()
	mock.calls.Checker = append(mock.calls.Checker, callInfo)
	lockS3UploaderMockChecker.Unlock()
	return mock.CheckerFunc(in1, in2)
}

// CheckerCalls gets all the calls that were made to Checker.
// Check the length with:
//     len(mockedS3Uploader.CheckerCalls())
func (mock *S3UploaderMock) CheckerCalls() []struct {
	In1 context.Context
	In2 *healthcheck.CheckState
} {
	var calls []struct {
		In1 context.Context
		In2 *healthcheck.CheckState
	}
	lockS3UploaderMockChecker.RLock()
	calls = mock.calls.Checker
	lockS3UploaderMockChecker.RUnlock()
	return calls
}

// Get calls GetFunc.
func (mock *S3UploaderMock) Get(key string) (io.ReadCloser, *int64, error) {
	if mock.GetFunc == nil {
		panic("S3UploaderMock.GetFunc: method is nil but S3Uploader.Get was just called")
	}
	callInfo := struct {
		Key string
	}{
		Key: key,
	}
	lockS3UploaderMockGet.Lock()
	mock.calls.Get = append(mock.calls.Get, callInfo)
	lockS3UploaderMockGet.Unlock()
	return mock.GetFunc(key)
}

// GetCalls gets all the calls that were made to Get.
// Check the length with:
//     len(mockedS3Uploader.GetCalls())
func (mock *S3UploaderMock) GetCalls() []struct {
	Key string
} {
	var calls []struct {
		Key string
	}
	lockS3UploaderMockGet.RLock()
	calls = mock.calls.Get
	lockS3UploaderMockGet.RUnlock()
	return calls
}

// Upload calls UploadFunc.
func (mock *S3UploaderMock) Upload(input *s3manager.UploadInput, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
	if mock.UploadFunc == nil {
		panic("S3UploaderMock.UploadFunc: method is nil but S3Uploader.Upload was just called")
	}
	callInfo := struct {
		Input   *s3manager.UploadInput
		Options []func(*s3manager.Uploader)
	}{
		Input:   input,
		Options: options,
	}
	lockS3UploaderMockUpload.Lock()
	mock.calls.Upload = append(mock.calls.Upload, callInfo)
	lockS3UploaderMockUpload.Unlock()
	return mock.UploadFunc(input, options...)
}

// UploadCalls gets all the calls that were made to Upload.
// Check the length with:
//     len(mockedS3Uploader.UploadCalls())
func (mock *S3UploaderMock) UploadCalls() []struct {
	Input   *s3manager.UploadInput
	Options []func(*s3manager.Uploader)
} {
	var calls []struct {
		Input   *s3manager.UploadInput
		Options []func(*s3manager.Uploader)
	}
	lockS3UploaderMockUpload.RLock()
	calls = mock.calls.Upload
	lockS3UploaderMockUpload.RUnlock()
	return calls
}

// UploadWithPSK calls UploadWithPSKFunc.
func (mock *S3UploaderMock) UploadWithPSK(input *s3manager.UploadInput, psk []byte) (*s3manager.UploadOutput, error) {
	if mock.UploadWithPSKFunc == nil {
		panic("S3UploaderMock.UploadWithPSKFunc: method is nil but S3Uploader.UploadWithPSK was just called")
	}
	callInfo := struct {
		Input *s3manager.UploadInput
		Psk   []byte
	}{
		Input: input,
		Psk:   psk,
	}
	lockS3UploaderMockUploadWithPSK.Lock()
	mock.calls.UploadWithPSK = append(mock.calls.UploadWithPSK, callInfo)
	lockS3UploaderMockUploadWithPSK.Unlock()
	return mock.UploadWithPSKFunc(input, psk)
}

// UploadWithPSKCalls gets all the calls that were made to UploadWithPSK.
// Check the length with:
//     len(mockedS3Uploader.UploadWithPSKCalls())
func (mock *S3UploaderMock) UploadWithPSKCalls() []struct {
	Input *s3manager.UploadInput
	Psk   []byte
} {
	var calls []struct {
		Input *s3manager.UploadInput
		Psk   []byte
	}
	lockS3UploaderMockUploadWithPSK.RLock()
	calls = mock.calls.UploadWithPSK
	lockS3UploaderMockUploadWithPSK.RUnlock()
	return calls
}
