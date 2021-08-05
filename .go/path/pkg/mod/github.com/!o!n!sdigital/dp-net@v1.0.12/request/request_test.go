package request

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAddRequestIdHeader(t *testing.T) {

	Convey("Given a request", t, func() {

		r, _ := http.NewRequest("POST", "http://localhost:21800/jobs", nil)

		Convey("When AddRequestIdHeader is called", func() {

			reqId := "123"
			AddRequestIdHeader(r, reqId)

			Convey("Then the request has the correct header set", func() {
				So(r.Header.Get(RequestHeaderKey), ShouldEqual, reqId)
			})
		})
	})
}

func TestNewRequestID(t *testing.T) {
	Convey("create a requestID with length of 12", t, func() {
		requestID := NewRequestID(12)
		So(len(requestID), ShouldEqual, 12)

		Convey("create a second requestID with length of 12", func() {
			secondRequestID := NewRequestID(12)
			So(len(secondRequestID), ShouldEqual, 12)
			So(secondRequestID, ShouldNotEqual, requestID)
		})
	})
}

func TestGetRequestId(t *testing.T) {
	Convey("should return requestID if it exists in the provided context", t, func() {
		ctx := WithRequestId(context.Background(), "666")
		So(ctx.Value(ContextKey("request-id")).(string), ShouldEqual, "666")
	})

	Convey("should return empty value if requestID is not in the provided context", t, func() {
		id := GetRequestId(context.Background())
		So(id, ShouldBeBlank)
	})
}

func TestSetRequestId(t *testing.T) {
	Convey("set request id in empty context", t, func() {
		ctx := WithRequestId(context.Background(), "123")
		So(ctx.Value(ContextKey("request-id")), ShouldEqual, "123")

		Convey("overwrite context request id with new value", func() {
			newCtx := WithRequestId(ctx, "456")
			So(newCtx.Value(ContextKey("request-id")), ShouldEqual, "456")
		})
	})
}

func TestHandler(t *testing.T) {
	Convey("requestID handler should wrap another handler", t, func() {
		handler := HandlerRequestID(20)
		wrapped := handler(dummyHandler)
		So(wrapped, ShouldHaveSameTypeAs, dummyHandler)
	})

	Convey("requestID should create a request ID if it doesn't exist", t, func() {
		req, err := http.NewRequest("GET", "/", nil)
		if err != nil {
			t.Fail()
		}
		w := httptest.NewRecorder()

		So(req.Header.Get(RequestHeaderKey), ShouldBeEmpty)

		handler := HandlerRequestID(20)
		wrapped := handler(dummyHandler)

		wrapped.ServeHTTP(w, req)
		So(w.Code, ShouldEqual, 200)

		header := req.Header.Get(RequestHeaderKey)
		So(header, ShouldNotBeEmpty)
		So(header, ShouldHaveLength, 20)
	})

	Convey("Existing request ID should be used if present", t, func() {
		req, err := http.NewRequest("GET", "/", nil)
		if err != nil {
			t.Fail()
		}
		w := httptest.NewRecorder()

		req.Header.Set(RequestHeaderKey, "test")
		So(req.Header.Get(RequestHeaderKey), ShouldNotBeEmpty)

		handler := HandlerRequestID(20)
		wrapped := handler(dummyHandler)

		wrapped.ServeHTTP(w, req)
		So(w.Code, ShouldEqual, 200)

		header := req.Header.Get(RequestHeaderKey)
		So(header, ShouldNotBeEmpty)
		So(header, ShouldHaveLength, 4)
		So(header, ShouldEqual, "test")
	})

	Convey("Length of requestID should be configurable", t, func() {
		req, err := http.NewRequest("GET", "/", nil)
		if err != nil {
			t.Fail()
		}
		w := httptest.NewRecorder()

		handler := HandlerRequestID(30)
		wrapped := handler(dummyHandler)

		wrapped.ServeHTTP(w, req)
		So(w.Code, ShouldEqual, 200)

		header := req.Header.Get(RequestHeaderKey)
		So(header, ShouldNotBeEmpty)
		So(header, ShouldHaveLength, 30)
	})

	Convey("generated requestIDs should be added to the request context", t, func() {
		req, err := http.NewRequest("GET", "/", nil)
		if err != nil {
			t.Fail()
		}

		var reqCtx context.Context
		var captureContextHandler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			reqCtx = req.Context()
		})

		w := httptest.NewRecorder()

		handler := HandlerRequestID(30)
		wrapped := handler(captureContextHandler)

		wrapped.ServeHTTP(w, req)
		So(w.Code, ShouldEqual, 200)
		So(reqCtx, ShouldNotBeNil)

		id := GetRequestId(reqCtx)
		So(id, ShouldNotBeEmpty)
		So(len(id), ShouldEqual, 30)
	})

	Convey("existing requestIDs should be added to the request context", t, func() {
		req, err := http.NewRequest("GET", "/", nil)
		if err != nil {
			t.Fail()
		}
		req.Header.Set(RequestHeaderKey, "666")

		var reqCtx context.Context
		var captureContextHandler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			reqCtx = req.Context()
		})

		w := httptest.NewRecorder()

		handler := HandlerRequestID(30)
		wrapped := handler(captureContextHandler)

		wrapped.ServeHTTP(w, req)
		So(w.Code, ShouldEqual, 200)
		So(reqCtx, ShouldNotBeNil)

		id := GetRequestId(reqCtx)
		So(id, ShouldNotBeEmpty)
		So(id, ShouldEqual, "666")
	})
}
