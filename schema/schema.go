package schema

import (
	"github.com/ONSdigital/go-ns/avro"
)

// TODO: remove or replace hello called structure and model with app specific
var instanceCompleteEvent = `{
  "type": "record",
  "name": "hello-called",
  "fields": [
    {"name": "instance_id", "type": "string", "default": ""}
  ]
}`

// HelloCalledEvent is the Avro schema for Hello Called messages.
var InstanceCompleteEvent = &avro.Schema{
	Definition: instanceCompleteEvent,
}
