package handlers

import (
	"context"
	"net/http"
	"strings"

	dphttp "github.com/ONSdigital/dp-net/http"
	request "github.com/ONSdigital/dp-net/request"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
)

// pathIDs relate the possible string matches in path with the respective
// parameter name as key values pairs
var pathIDs = map[string]string{
	"jobs":      "job_id",
	"datasets":  "dataset_id",
	"instances": "instance_id",
}

// CheckIdentity wraps a HTTP handler function. It validates that the request context contains a Caller,
// snf only calls the provided HTTP handler if the Caller is available, else it returns an error code and drains the http body
func CheckIdentity(handle func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		vars := mux.Vars(r)
		logData := getLogData(ctx, r.URL.EscapedPath(), vars)

		// just checking if an identity exists until permissions are being provided.
		if !request.IsCallerPresent(ctx) {
			log.Event(ctx, "no identity found in context of request", log.HTTP(r, 0, 0, nil, nil), logData)
			http.Error(w, "unauthenticated request", http.StatusUnauthorized)
			dphttp.DrainBody(r)
			return
		}

		// The request has been authenticated, now run the clients request
		log.Event(ctx, "identity found in request context, calling downstream handler", log.HTTP(r, 0, 0, nil, nil), logData)
		handle(w, r)
	})
}

// getLogData populates a new log.Data with path variable values
func getLogData(ctx context.Context, path string, vars map[string]string) log.Data {
	logData := log.Data{}

	callerIdentity := request.Caller(ctx)
	if callerIdentity != "" {
		logData["caller_identity"] = callerIdentity
	}

	pathSegments := strings.Split(path, "/")
	// Remove initial segment if empty
	if pathSegments[0] == "" {
		pathSegments = pathSegments[1:]
	}
	numberOfSegments := len(pathSegments)

	if pathSegments[0] == "hierarchies" {
		if numberOfSegments > 1 {
			logData["instance_id"] = pathSegments[1]

			if numberOfSegments > 2 {
				logData["dimension"] = pathSegments[2]

				if numberOfSegments > 3 {
					logData["code"] = pathSegments[3]
				}
			}
		}
		return logData
	}

	if pathSegments[0] == "search" {
		pathSegments = pathSegments[1:]
	}

	for key, value := range vars {
		if key == "id" {
			logData[pathIDs[pathSegments[0]]] = value
		} else {
			logData[key] = value
		}
	}

	return logData
}
