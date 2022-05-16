package schema

import (
	"github.com/ONSdigital/dp-kafka/v3/avro"
)

/*
   Filter ID could be null, but opted for empty string
   representing part of publishing journey.
   the avro unmarshal lib does not support pointers to
   strings
*/
var exportStart = `{
  "type": "record",
  "name": "cantabular-export-start",
  "fields": [
    {"name": "instance_id", 		"type": "string", "default": ""},
    {"name": "dataset_id",  		"type": "string", "default": ""},
    {"name": "edition",     		"type": "string", "default": ""},
    {"name": "version",     		"type": "string", "default": ""},
    {"name": "filter_output_id","type":"string",  "default": ""},
    {"name": "dimensions",   "type": { "type": "array", "items": "string"},"default": [] }
  ]
}`

// ExportStart is the Avro schema for Instance Complete messages.
var ExportStart = &avro.Schema{
	Definition: exportStart,
}

var csvCreated = `{
  "type": "record",
  "name": "cantabular-csv-created",
  "fields": [
    {"name": "instance_id",   "type": "string", "default": ""},
    {"name": "dataset_id",    "type": "string", "default": ""},
    {"name": "edition",       "type": "string", "default": ""},
    {"name": "version",       "type": "string", "default": ""},
    {"name": "row_count",     "type": "int",    "default": 0 },
    {"name": "dimensions", "type": { "type": "array", "items": "string"}, "default": [] }
  ]
}`

// CSVCreated the Avro schema for CSV exported messages.
var CSVCreated = &avro.Schema{
	Definition: csvCreated,
}
