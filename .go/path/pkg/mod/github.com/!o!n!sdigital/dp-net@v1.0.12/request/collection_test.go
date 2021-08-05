package request

import (
	"github.com/ONSdigital/dp-api-clients-go/headers"
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestGetCollectionID(t *testing.T) {

	expectedToken := "foo"

	Convey("should return collectionID from request header", t, func() {
		req := httptest.NewRequest("GET", "http://localhost:8080", nil)
		req.Header.Set(CollectionIDHeaderKey, expectedToken)

		actual, err := GetCollectionID(req)

		So(actual, ShouldEqual, expectedToken)
		So(err, ShouldBeNil)
	})

	Convey("should return collection id from request cookie", t, func() {
		req := httptest.NewRequest("GET", "http://localhost:8080", nil)
		req.AddCookie(&http.Cookie{Name: CollectionIDCookieKey, Value: expectedToken})

		actual, err := GetCollectionID(req)

		So(actual, ShouldEqual, expectedToken)
		So(err, ShouldBeNil)
	})

	Convey("should return empty token if no header or cookie is set", t, func() {
		req := httptest.NewRequest("GET", "http://localhost:8080", nil)

		actual, err := GetCollectionID(req)

		So(actual, ShouldBeEmpty)
		So(err, ShouldBeNil)
	})

	Convey("should return empty token and error if get header returns an error that is not ErrHeaderNotFound", t, func() {
		actual, err := GetCollectionID(nil)

		So(actual, ShouldBeEmpty)
		So(err, ShouldResemble, headers.ErrRequestNil)
	})
}

func TestGetCollectionIDFromCookie(t *testing.T) {
	expectedToken := "foo"

	Convey("should return collection id from request cookie", t, func() {
		req := httptest.NewRequest("GET", "http://localhost:8080", nil)
		req.AddCookie(&http.Cookie{Name: CollectionIDCookieKey, Value: expectedToken})

		actual, err := getCollectionIDFromCookie(req)

		So(actual, ShouldEqual, expectedToken)
		So(err, ShouldBeNil)
	})

	Convey("should return empty id if collection id cookie not found", t, func() {
		req := httptest.NewRequest("GET", "http://localhost:8080", nil)

		actual, err := getCollectionIDFromCookie(req)

		So(actual, ShouldBeEmpty)
		So(err, ShouldBeNil)
	})
}
