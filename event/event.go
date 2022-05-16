package event

// ExportStart provides an avro structure for a Export Start event
type ExportStart struct {
	InstanceID     string   `avro:"instance_id"`
	DatasetID      string   `avro:"dataset_id"`
	Edition        string   `avro:"edition"`
	Version        string   `avro:"version"`
	FilterOutputID string   `avro:"filter_output_id"`
	DimensionIDs   []string `avro:"dimension_ids"`
}

// CSVCreated provides an avro structure for a CSV Created event
type CSVCreated struct {
	InstanceID   string   `avro:"instance_id"`
	DatasetID    string   `avro:"dataset_id"`
	Edition      string   `avro:"edition"`
	Version      string   `avro:"version"`
	RowCount     int32    `avro:"row_count"`
	DimensionIDs []string `avro:"dimension_ids"`
}
