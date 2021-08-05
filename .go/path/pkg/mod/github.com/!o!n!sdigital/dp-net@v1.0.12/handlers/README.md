# Handlers

This package contains handlers to manage header and cookie values, and identity validation.

## Context values middleware
===================

CheckHeader and CheckCookie handlers forward values coming from a request header or a cookie, to a request context.

The mapping is done using enumeration of possible keys, and their mappings to header, context or cookie keys.

### Usage

- Read a header from a request and add its value to the underlying request context:

```go
    // Access token
    handler := handlers.CheckHeader(handlers.UserAccess)

    // Locale
    handler := handlers.CheckHeader(handlers.Locale)

    // CollectionID
    handler := handlers.CheckHeader(handlers.CollectionID)
```

- Read a cookie and add its value to the output request context:

```go
    // Access token
    handler := handlers.CheckCookie(handlers.UserAccess)

    // Locale
    handler := handlers.CheckCookie(handlers.Locale)

    // CollectionID
    handler := handlers.CheckCookie(handlers.CollectionID)
```

- Access the context value:

```go
    // Access token
    accessToken := ctx.Value(handlers.UserAccess.Context())

    // Locale
    locale := ctx.Value(handlers.Locale.Context())

    // CollectionID
    collectionID := ctx.Value(handlers.CollectionID.Context())
```

## Identity middleware
===================

Middleware component that authenticates requests against zebedee.

The identity and permissions returned from the identity endpoint are added to the request context.

### Getting started

Initialise the identity middleware and add it into the HTTP handler chain using `alice`:

```go
    router := mux.NewRouter()
    alice := alice.New(handlers.Identity(<zebedeeURL>)).Then(router)
    httpServer := server.New(config.BindAddr, alice)
```

Wrap authenticated endpoints using the `handlers.CheckIdentity(handler)` function to check that a request identity exists.

```go
    router.Path("/jobs").Methods("POST").HandlerFunc(handlers.CheckIdentity(api.addJob))
```

Add required headers to outbound requests to other services

```go
    import "github.com/ONSdigital/dp-net/request"

    request.AddServiceTokenHeader(req, api.AuthToken)
    request.AddUserHeader(req, "UserA")
```

or, put less portably:

```go
    req.Header.Add("Authorization", api.AuthToken)
    req.Header.Add("User-Identity", "UserA")
```

But most of this should be done by `dp-net/http` and `dp-api-clients-go/...`.

### Testing

If you need to use the middleware component in unit tests you can call the constructor function that allows injection of the HTTP client

```go
import (
    clientsidentity "github.com/ONSdigital/dp-api-clients-go/identity"
    dphttp "github.com/ONSdigital/dp-net/http"
    dphandlers "github.com/ONSdigital/dp-net/handlers"
)

httpClient := &dphttp.ClienterMock{
    DoFunc: func(ctx context.Context, req *http.Request) (*http.Response, error) {
        return &http.Response{
            StatusCode: http.StatusOK,
        }, nil
    },
}
// set last argument to secretKey if you want to support legacy headers
clientsidentity.NewAPIClient(httpClient, zebedeeURL, "")

identityHandler := dphandlers.IdentityWithHTTPClient(doAuth, httpClient)
```
