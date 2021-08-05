package request

import (
	"context"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

// Header constants
const (
	RequestHeaderKey = "X-Request-Id"
)

// GetRequestId gets the correlation id on the context
func GetRequestId(ctx context.Context) string {
	correlationId, _ := ctx.Value(RequestIdKey).(string)
	return correlationId
}

// WithRequestId sets the correlation id on the context
func WithRequestId(ctx context.Context, correlationId string) context.Context {
	return context.WithValue(ctx, RequestIdKey, correlationId)
}

// AddRequestIdHeader add header for given correlation ID
func AddRequestIdHeader(r *http.Request, token string) {
	if len(token) > 0 {
		r.Header.Add(RequestHeaderKey, token)
	}
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var requestIDRandom = rand.New(rand.NewSource(time.Now().UnixNano()))
var randMutex sync.Mutex

// NewRequestID generates a random string of requested length
func NewRequestID(size int) string {
	b := make([]rune, size)
	randMutex.Lock()
	for i := range b {
		b[i] = letters[requestIDRandom.Intn(len(letters))]
	}
	randMutex.Unlock()
	return string(b)
}

// HandlerRequestID is a wrapper which adds an X-Request-Id header if one does not yet exist
func HandlerRequestID(size int) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {

		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			requestID := req.Header.Get(RequestHeaderKey)

			if len(requestID) == 0 {
				requestID = NewRequestID(size)
				AddRequestIdHeader(req, requestID)
			}

			h.ServeHTTP(w, req.WithContext(WithRequestId(req.Context(), requestID)))
		})
	}
}
