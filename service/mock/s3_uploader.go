// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package mock

import (
	"context"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/service"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"sync"
)

var (
	lockS3UploaderMockBucketName        sync.RWMutex
	lockS3UploaderMockChecker           sync.RWMutex
	lockS3UploaderMockHead              sync.RWMutex
	lockS3UploaderMockSession           sync.RWMutex
	lockS3UploaderMockUploadWithContext sync.RWMutex
	lockS3UploaderMockUploadWithPSK     sync.RWMutex
)

// Ensure, that S3UploaderMock does implement service.S3Uploader.
// If this is not the case, regenerate this file with moq.
var _ service.S3Uploader = &S3UploaderMock{}

// Example of how to instantiate a mock for testing - this code is automatically generated
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
//             HeadFunc: func(key string) (*s3.HeadObjectOutput, error) {
// 	               panic("mock out the Head method")
//             },
//             SessionFunc: func() *session.Session {
// 	               panic("mock out the Session method")
//             },
//             UploadWithContextFunc: func(ctx context.Context, input *s3manager.UploadInput, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
// 	               panic("mock out the UploadWithContext method")
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

	// HeadFunc mocks the Head method.
	HeadFunc func(key string) (*s3.HeadObjectOutput, error)

	// SessionFunc mocks the Session method.
	SessionFunc func() *session.Session

	// UploadWithContextFunc mocks the UploadWithContext method.
	UploadWithContextFunc func(ctx context.Context, input *s3manager.UploadInput, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error)

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
		// Head holds details about calls to the Head method.
		Head []struct {
			// Key is the key argument value.
			Key string
		}
		// Session holds details about calls to the Session method.
		Session []struct {
		}
		// UploadWithContext holds details about calls to the UploadWithContext method.
		UploadWithContext []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
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

// Head calls HeadFunc.
func (mock *S3UploaderMock) Head(key string) (*s3.HeadObjectOutput, error) {
	if mock.HeadFunc == nil {
		panic("S3UploaderMock.HeadFunc: method is nil but S3Uploader.Head was just called")
	}
	callInfo := struct {
		Key string
	}{
		Key: key,
	}
	lockS3UploaderMockHead.Lock()
	mock.calls.Head = append(mock.calls.Head, callInfo)
	lockS3UploaderMockHead.Unlock()
	return mock.HeadFunc(key)
}

// HeadCalls gets all the calls that were made to Head.
// Check the length with:
//     len(mockedS3Uploader.HeadCalls())
func (mock *S3UploaderMock) HeadCalls() []struct {
	Key string
} {
	var calls []struct {
		Key string
	}
	lockS3UploaderMockHead.RLock()
	calls = mock.calls.Head
	lockS3UploaderMockHead.RUnlock()
	return calls
}

// Session calls SessionFunc.
func (mock *S3UploaderMock) Session() *session.Session {
	if mock.SessionFunc == nil {
		panic("S3UploaderMock.SessionFunc: method is nil but S3Uploader.Session was just called")
	}
	callInfo := struct {
	}{}
	lockS3UploaderMockSession.Lock()
	mock.calls.Session = append(mock.calls.Session, callInfo)
	lockS3UploaderMockSession.Unlock()
	return mock.SessionFunc()
}

// SessionCalls gets all the calls that were made to Session.
// Check the length with:
//     len(mockedS3Uploader.SessionCalls())
func (mock *S3UploaderMock) SessionCalls() []struct {
} {
	var calls []struct {
	}
	lockS3UploaderMockSession.RLock()
	calls = mock.calls.Session
	lockS3UploaderMockSession.RUnlock()
	return calls
}

// UploadWithContext calls UploadWithContextFunc.
func (mock *S3UploaderMock) UploadWithContext(ctx context.Context, input *s3manager.UploadInput, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
	if mock.UploadWithContextFunc == nil {
		panic("S3UploaderMock.UploadWithContextFunc: method is nil but S3Uploader.UploadWithContext was just called")
	}
	callInfo := struct {
		Ctx     context.Context
		Input   *s3manager.UploadInput
		Options []func(*s3manager.Uploader)
	}{
		Ctx:     ctx,
		Input:   input,
		Options: options,
	}
	lockS3UploaderMockUploadWithContext.Lock()
	mock.calls.UploadWithContext = append(mock.calls.UploadWithContext, callInfo)
	lockS3UploaderMockUploadWithContext.Unlock()
	return mock.UploadWithContextFunc(ctx, input, options...)
}

// UploadWithContextCalls gets all the calls that were made to UploadWithContext.
// Check the length with:
//     len(mockedS3Uploader.UploadWithContextCalls())
func (mock *S3UploaderMock) UploadWithContextCalls() []struct {
	Ctx     context.Context
	Input   *s3manager.UploadInput
	Options []func(*s3manager.Uploader)
} {
	var calls []struct {
		Ctx     context.Context
		Input   *s3manager.UploadInput
		Options []func(*s3manager.Uploader)
	}
	lockS3UploaderMockUploadWithContext.RLock()
	calls = mock.calls.UploadWithContext
	lockS3UploaderMockUploadWithContext.RUnlock()
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
