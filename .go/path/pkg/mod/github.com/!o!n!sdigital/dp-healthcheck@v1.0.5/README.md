dp-healthcheck
==============

A health check git repository for Digital Publishing that implements the [Health Check Specification](https://github.com/ONSdigital/dp/blob/master/standards/HEALTH_CHECK_SPECIFICATION.md).  All Digital Publishing apps must implement a health check using this library.  Functions that implement the `Checker` type are registered with the library which will check internal and external measures of the apps health.  The library will then call these functions periodically to determine the overall health of the app and report this back using the included handler.

Getting started
---------------

* [Add health check to an app](#adding-a-health-check-to-an-app)
* [Implementing a `Checker` function](#implementing-a-checker)

Adding a health check to an app
-------------------------------

1. Import the library and your HTTP server dependencies:

    ```
    package main

    import (
        ...
        health "github.com/ONSdigital/dp-healthcheck/healthcheck"
        dphttp "github.com/ONSdigital/dp-net/http"
        "github.com/gorilla/mux"
        ...
    )
    ```

2. Create a new version object defining the version of your app:

    ```
    ...

    var BuildTime, GitCommit, Version string

    ...

    func main() {
        ctx := context.Context(context.Background())

        ...

        // Likely these values would come from your app's config
        criticalTimeout := 90 * time.Second // defined by env var HEALTHCHECK_CRITICAL_TIMEOUT
        interval := 30 * time.Second // defined by env var HEALTHCHECK_INTERVAL

        ...

        versionInfo := health.NewVersionInfo(
            BuildTime,
            GitCommit,
            Version,
        )

        ...
    ```

2. Initialise any clients that have `Checker` type functions you wish to use

3. Instantiate the health check library:

    ```
        ...

        hc, err := health.New(versionInfo criticalTimeout, interval)
        if err != nil {
            ...
        }

        ...
    ```

4. Register your `Checker` functions providing a short human readable name for each (it is best to try to keep the name consistent between apps where possible):

    ```
        ...

        if err = hc.AddCheck("check 1", CheckFunc1); err != nil {
            ...
        }
        if err = hc.AddCheck("mongoDB", &mongoClient.Checker); err != nil {
            ...
        }

        ...
    ```

5. Register the health handler:

    ```
        ...

        r := mux.NewRouter()
        r.HandleFunc("/health", hc.Handler)

        ...
    ```

6. Start the health check library:

    ```
        ...

        hc.Start(ctx)

        ...
    ```

7. Start the HTTP server:

    ```
        ...

        s := dphttp.NewServer(":8080", r
        if err := s.ListenAndServe(); err != nil {
            ...
        }

        ...
    ```

8. Then gracefully shutdown the health check library:

    ```
        ...

        hc.Stop()

        ...
    }
    ```

9. Set the `BuildTime`, `GitCommit` and `Version` during compile:

    Command line:

    ```
    BUILD_TIME="$(date +%s)" GIT_COMMIT="$(git rev-parse HEAD)" VERSION="$(git tag --points-at HEAD | grep ^v | head -n 1)" go build -ldflags="-X 'main.BuildTime=$BUILD_TIME' -X 'main.GitCommit=$GIT_COMMIT' -X 'main.Version=$VERSION'"
    ```

    Makefile:

    ```
    ...
    APP_NAME = app-name

    BIN_DIR    ?=.
    VERSION    ?= $(shell git tag --points-at HEAD | grep ^v | head -n 1)

    BUILD_ARCH  = $(BUILD)/$(GOOS)-$(GOARCH)
    BUILD_TIME  = $(shell date +%s)
    GIT_COMMIT  = $(shell git rev-parse HEAD)

    export GOOS   ?= $(shell go env GOOS)
    export GOARCH ?= $(shell go env GOARCH)

    ...

    build:
            @mkdir -p $(BUILD_ARCH)/$(BIN_DIR)
            go build -o $(BUILD_ARCH)/$(BIN_DIR)/$(MAIN) -ldflags="-X 'main.BuildTime=$(BUILD_TIME)' -X 'main.GitCommit=$(GIT_COMMIT)' -X 'main.Version=$(VERSION)'" cmd/$(MAIN)/main.go

    debug:
            HUMAN_LOG=1 go run -race -ldflags="-X 'main.BuildTime=$(BUILD_TIME)' -X 'main.GitCommit=$(GIT_COMMIT)' -X 'main.Version=$(VERSION)'" cmd/$(MAIN)/main.go

    ...
    ```

Implementing a checker
----------------------

Each checker measures the health of something that is required for an app to function.  This could be something internal to the app (e.g. latency, error rate, saturation, etc.) or something external (e.g. the health of an upstream app, connection to a data store, etc.).  Each checker is a function that gets the current state of whatever it is responsible for checking.

Implement a checker by creating a function (with or without a receiver) that is of the `healthcheck.Checker` type.

For example:

```
func Check(ctx context.Context, state *CheckState) error {
	success := rand.Float32() < 0.5
	warn := rand.Float32() < 0.5

	if success {
        state.Update(health.StatusOK, "I'm OK", 200)
	} else if warn {
        state.Update(health.StatusWarning, "degraded function of ...", 0)
	} else {
        state.Update(health.StatusWarning, "failed to ...", 503)
	}
	return nil
}
```

Note that the `statusCode` argument (last argument) to `CheckState.Update()` is only used for HTTP based checks.  If you do not have a status code then pass `0` as seen in the example above (degraded state/warning block).

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright © 2019-2020, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.
