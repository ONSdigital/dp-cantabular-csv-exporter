package event

// InstanceComplete provides an avro structure for a Instance Complete event
type InstanceComplete struct {
	InstanceID     string `avro:"instance_id"`
	CantabularBlob string `avro:"cantabular_blob"`
}

// CommonOutputCreated provides an avro structure for an Output Created event
type CommonOutputCreated struct {
	FilterID   string `avro:"filter_output_id"`
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
	DatasetID  string `avro:"dataset_id"`
	Edition    string `avro:"edition"`
	Version    string `avro:"version"`
	Filename   string `avro:"filename"`
	RowCount   int32  `avro:"row_count"`
}
