package schema

import (
	"github.com/ONSdigital/dp-kafka/v2/avro"
)

var instanceCompleteEvent = `{
  "type": "record",
  "name": "cantabular-dataset-instance-complete",
  "fields": [
    {"name": "instance_id", "type": "string", "default": ""}
  ]
}`

// InstanceCompleteEvent is the Avro schema for Instance Complete messages.
var InstanceCompleteEvent = &avro.Schema{
	Definition: instanceCompleteEvent,
}
