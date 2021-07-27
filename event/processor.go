package event

import (
	"github.com/ONSdigital/dp-cantabular-csv-exporter/config"
)

// Processor handles consuming and processing Kafka messages
type Processor struct {
	datasetAPI DatasetAPIClient
	cfg        config.Config
}

// NewProcessor returns a new Processor
func NewProcessor(cfg config.Config, d DatasetAPIClient) *Processor {
	return &Processor{
		cfg:        cfg,
		datasetAPI: d,
	}
}