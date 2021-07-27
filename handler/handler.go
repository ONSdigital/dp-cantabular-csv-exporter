package handler

import (
	"context"
	"fmt"


	"github.com/ONSdigital/dp-cantabular-csv-exporter/config"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/event"

	"github.com/ONSdigital/log.go/v2/log"
	"github.com/ONSdigital/dp-api-clients-go/v2/headers"
)

// InstanceCompleteHandler is the handle for the InstanceCompleteHandler event
type InstanceComplete struct {
	cfg      config.Config
	ctblr    CantabularClient
	datasets DatasetAPIClient
	s3       S3Client
}

// NewInstanceCompleteHandler creates a new InstanceCompleteHandler
func NewInstanceComplete(cfg config.Config, c CantabularClient, d DatasetAPIClient, s S3Client) *InstanceComplete {
	return &InstanceComplete{
		cfg:      cfg,
		ctblr:    c,
		datasets: d,
		s3:       s,
	}
}

// Handle takes a single event.
func (h *InstanceComplete) Handle(ctx context.Context, e *event.InstanceComplete) error {
	logData := log.Data{
		"event": e,
	}
	log.Info(ctx, "event handler called", logData)

	instance, _, err := h.datasets.GetInstance(ctx, "", h.cfg.ServiceAuthToken, "", e.InstanceID, headers.IfMatchAnyETag)
	if err != nil {
		return fmt.Errorf("failed to get instance: %w", err)
	}

	log.Info(ctx, "instance obtained from dataset API", log.Data{
		"instance": instance,
	})

	return nil
}
