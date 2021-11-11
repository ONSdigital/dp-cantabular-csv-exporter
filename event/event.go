package event

// ExportStart provides an avro structure for a Export Start event
type ExportStart struct {
	InstanceID string `avro:"instance_id"`
	DatasetID  string `avro:"dataset_id"`
	Edition    string `avro:"edition"`
	Version    string `avro:"version"`
}

// CSVCreated provides an avro structure for a CSV Created event
type CSVCreated struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
	DatasetID  string `avro:"dataset_id"`
	Edition    string `avro:"edition"`
	Version    string `avro:"version"`
	Filename   string `avro:"filename"`
	RowCount   int32  `avro:"row_count"`
}
