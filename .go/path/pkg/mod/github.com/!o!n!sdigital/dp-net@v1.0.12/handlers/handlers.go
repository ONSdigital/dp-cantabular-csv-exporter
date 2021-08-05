package handlers

import (
	"context"
	"github.com/ONSdigital/dp-net/request"
	"github.com/ONSdigital/log.go/log"
	"net/http"
)

// ControllerHandlerFunc is a function type that accepts arguments required for logical flow in handlers Implementation of function should set headers as needed
type ControllerHandlerFunc func(w http.ResponseWriter, r *http.Request, lang, collectionID, accessToken string)

// CheckHeader is a wrapper which adds a value from the request header (if found) to the request context.
// This function complies with alice middleware Constructor type: func(http.Handler) -> (http.Handler)
func CheckHeader(key Key) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return DoCheckHeader(h, key)
	}
}

// DoCheckHeader returns a handler that performs the CheckHeader logic
func DoCheckHeader(h http.Handler, key Key) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if headerValue := req.Header.Get(key.Header()); headerValue != "" {
			req = req.WithContext(context.WithValue(req.Context(), key.Context(), headerValue))
		}
		h.ServeHTTP(w, req)
	})
}

// CheckCookie is a wrapper which adds a cookie value (if found) to the request context.
// This function complies with alice middleware Constructor type: func(http.Handler) -> (http.Handler))
func CheckCookie(key Key) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return DoCheckCookie(h, key)
	}
}

// DoCheckCookie returns a handler that performs the CheckCookie logic
func DoCheckCookie(h http.Handler, key Key) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		cookieValue, err := req.Cookie(key.Cookie())
		if err != nil {
			if err != http.ErrNoCookie {
				log.Event(req.Context(), "unexpected error while extracting value from cookie", log.ERROR, log.Error(err),
					log.Data{"cookie_key": key.Cookie()})
			}
		} else {
			req = req.WithContext(context.WithValue(req.Context(), key.Context(), cookieValue.Value))
		}
		h.ServeHTTP(w, req)
	})
}

// ControllerHandler is a middleware handler that ensures all required logical arguments are being passed along and then returns a generic http.HandlerFunc
func ControllerHandler(controllerHandlerFunc ControllerHandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		locale := request.GetLocaleCode(r)
		collectionID, err := request.GetCollectionID(r)
		if err != nil {
			log.Event(ctx, "unexpected error when getting collection id", log.ERROR, log.Error(err))
		}
		accessToken, err := GetFlorenceToken(ctx, r)
		if err != nil {
			log.Event(ctx, "unexpected error when getting access token", log.ERROR, log.Error(err))
		}
		controllerHandlerFunc(w, r, locale, collectionID, accessToken)
	}
}
