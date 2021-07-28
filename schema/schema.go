package schema

import (
	"github.com/ONSdigital/dp-kafka/v2/avro"
)

var instanceComplete = `{
  "type": "record",
  "name": "cantabular-dataset-instance-complete",
  "fields": [
    {"name": "instance_id", "type": "string", "default": ""}
  ]
}`

// InstanceComplete is the Avro schema for Instance Complete messages.
var InstanceComplete = &avro.Schema{
	Definition: instanceComplete,
}
