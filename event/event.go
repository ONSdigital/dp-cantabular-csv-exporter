package event

// InstanceComplete provides an avro structure for a Instance Complete event
type InstanceComplete struct {
	InstanceID     string `avro:"instance_id"`
	CantabularBlob string `avro:"cantabular_blob"`
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
