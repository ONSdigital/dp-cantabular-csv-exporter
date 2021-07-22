package event

import (
	"context"

	"github.com/ONSdigital/dp-api-clients-go/v2/headers"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/config"
	"github.com/ONSdigital/log.go/v2/log"
)

// InstanceCompleteHandler is the handle for the InstanceCompleteHandler event
type InstanceCompleteHandler struct {
	cfg      config.Config
	ctblr    CantabularClient
	datasets DatasetAPIClient
	s3       S3Client
}

// NewInstanceCompleteHandler creates a new InstanceCompleteHandler
func NewInstanceCompleteHandler(cfg config.Config, c CantabularClient, d DatasetAPIClient, s S3Client) *InstanceCompleteHandler {
	return &InstanceCompleteHandler{
		cfg:      cfg,
		ctblr:    c,
		datasets: d,
		s3:       s,
	}
}

// Handle takes a single event.
func (h *InstanceCompleteHandler) Handle(ctx context.Context, cfg *config.Config, event *InstanceComplete) (err error) {
	logData := log.Data{
		"event": event,
	}
	log.Info(ctx, "event handler called", logData)

	instance, _, err := h.datasets.GetInstance(ctx, "", h.cfg.ServiceAuthToken, "", event.InstanceID, headers.IfMatchAnyETag)
	if err != nil {
		return err
	}

	log.Info(ctx, "instance obtained from dataset API", log.Data{
		"instance": instance,
	})

	return nil
}
