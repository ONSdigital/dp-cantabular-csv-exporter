package response

import (
	"encoding/json"
	"net/http"
)

// ContentType header possible values
const (
	ContentTypeHeader = "Content-Type"
	ContentTypeJSON   = "application/json"
)

// JSONEncoder interface defining a JSON encoder.
type JSONEncoder interface {
	WriteResponseJSON(w http.ResponseWriter, value interface{}, status int) error
}

// OnsJSONEncoder is a JSON encoder
type OnsJSONEncoder struct{}

var JsonResponseEncoder JSONEncoder = &OnsJSONEncoder{}

// WriteJSON set the content type header to JSON, writes the response object as json and sets the http status code.
func WriteJSON(w http.ResponseWriter, value interface{}, status int) error {
	return JsonResponseEncoder.WriteResponseJSON(w, value, status)
}

// WriteResponseJSON marshals the provided value as json body, and sets the status code to the provided value
func (j *OnsJSONEncoder) WriteResponseJSON(w http.ResponseWriter, value interface{}, status int) error {
	w.Header().Set(ContentTypeHeader, ContentTypeJSON)

	b, err := json.Marshal(value)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return err
	}
	w.WriteHeader(status)
	w.Write(b)
	return nil
}
