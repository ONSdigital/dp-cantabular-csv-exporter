package handlers

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	dprequest "github.com/ONSdigital/dp-net/request"
	"github.com/ONSdigital/log.go/log"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	testUserIdentifier = "fred@ons.gov.uk"
)

func TestCheckIdentityNoHeaders(t *testing.T) {

	Convey("Given a http request without auth", t, func() {
		req := httptest.NewRequest("GET", url, bytes.NewBufferString("some body content"))
		responseRecorder := httptest.NewRecorder()

		handlerCalled := false
		httpHandler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			handlerCalled = true
		})

		identityHandler := CheckIdentity(httpHandler)

		Convey("When ServeHTTP is called", func() {

			identityHandler.ServeHTTP(responseRecorder, req)

			Convey("Then the downstream HTTP handler is not called", func() {
				So(handlerCalled, ShouldBeFalse)
			})

			Convey("Then the http response should have a 401 status", func() {
				So(responseRecorder.Result().StatusCode, ShouldEqual, http.StatusUnauthorized)
			})

			Convey("Then the request body has been drained", func() {
				_, err := req.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})
	})
}

func TestCheckIdentity(t *testing.T) {

	Convey("Given a request with caller identity in context", t, func() {
		req := httptest.NewRequest("GET", url, bytes.NewBufferString("some body content"))
		req = req.WithContext(context.WithValue(req.Context(), dprequest.CallerIdentityKey, testUserIdentifier))
		responseRecorder := httptest.NewRecorder()

		handlerCalled := false
		httpHandler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusAccepted)
			handlerCalled = true
		})

		identityHandler := CheckIdentity(httpHandler)

		Convey("When ServeHTTP is called", func() {

			identityHandler.ServeHTTP(responseRecorder, req)

			Convey("Then the downstream HTTP handler is called", func() {
				So(handlerCalled, ShouldBeTrue)
			})

			Convey("Then the response code is set to the value returned by the downstream handler (202 Accepted)", func() {
				So(responseRecorder.Code, ShouldEqual, http.StatusAccepted)
			})

			Convey("Then the request body has not been drained", func() {
				_, err := req.Body.Read(make([]byte, 1))
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestGetLogData(t *testing.T) {
	ctx := context.Background()

	Convey("Given a context with caller identity", t, func() {
		ctx := context.WithValue(ctx, dprequest.CallerIdentityKey, testUserIdentifier)
		Convey("Then getLogData correctly populates the user identity field in log Data", func() {
			logData := getLogData(ctx, url, nil)
			So(logData, ShouldResemble, log.Data{
				"caller_identity": testUserIdentifier,
			})
		})
	})

	Convey("Given a hierarchies URL", t, func() {
		testPath := "/hierarchies/myInstance/myDimension/myCode"
		Convey("Then getLogData correctly parses the path and populates the expected fields", func() {
			logData := getLogData(ctx, testPath, nil)
			So(logData, ShouldResemble, log.Data{
				"code":        "myCode",
				"dimension":   "myDimension",
				"instance_id": "myInstance",
			})
		})
	})

	Convey("Given a set of vars for jobs, instances and datasets", t, func() {
		vars := map[string]string{
			"jobs":      "myJob",
			"datasets":  "myDataset",
			"instances": "myInstance",
		}
		Convey("Then getLogData populates the expected fields from vars", func() {
			logData := getLogData(ctx, url, vars)
			So(logData, ShouldResemble, log.Data{
				"jobs":      "myJob",
				"datasets":  "myDataset",
				"instances": "myInstance",
			})
		})
	})

	Convey("Given a vars map only with id", t, func() {
		vars := map[string]string{
			"id": "myID",
		}

		Convey("Then getLogData for a '/jobs' path populates the job_id field", func() {
			logData := getLogData(ctx, "/jobs/123", vars)
			So(logData, ShouldResemble, log.Data{
				"job_id": "myID",
			})
		})

		Convey("Then getLogData for a '/search/jobs' path populates the job_id field", func() {
			logData := getLogData(ctx, "/search/jobs/123", vars)
			So(logData, ShouldResemble, log.Data{
				"job_id": "myID",
			})
		})

		Convey("Then getLogData for a '/datasets' path populates the dataset_id field", func() {
			logData := getLogData(ctx, "/datasets/123", vars)
			So(logData, ShouldResemble, log.Data{
				"dataset_id": "myID",
			})
		})

		Convey("Then getLogData for a '/datasets/jobs' path populates the dataset_id field", func() {
			logData := getLogData(ctx, "/search/datasets/123", vars)
			So(logData, ShouldResemble, log.Data{
				"dataset_id": "myID",
			})
		})

		Convey("Then getLogData for a '/instances' path populates the instance_id field", func() {
			logData := getLogData(ctx, "/instances/123", vars)
			So(logData, ShouldResemble, log.Data{
				"instance_id": "myID",
			})
		})

		Convey("Then getLogData for a '/search/instances' path populates the instance_id field", func() {
			logData := getLogData(ctx, "/search/instances/123", vars)
			So(logData, ShouldResemble, log.Data{
				"instance_id": "myID",
			})
		})
	})
}
