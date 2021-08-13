package handler

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"path"

	"github.com/ONSdigital/dp-cantabular-csv-exporter/config"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/event"
	"github.com/ONSdigital/dp-api-clients-go/v2/cantabular"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/schema"
	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-api-clients-go/v2/headers"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/log.go/v2/log"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/uuid"
)

// Encrypted determines if files need to be encrypted with a newly generated key when they are stored in S3
const Encrypted = false

// InstanceComplete is the handle for the InstanceCompleteHandler event
type InstanceComplete struct {
	cfg         config.Config
	ctblr       CantabularClient
	datasets    DatasetAPIClient
	s3          S3Uploader
	vaultClient VaultClient
	producer    kafka.IProducer
}

// NewInstanceComplete creates a new InstanceCompleteHandler
func NewInstanceComplete(cfg config.Config, c CantabularClient, d DatasetAPIClient, s S3Uploader, v VaultClient, p kafka.IProducer) *InstanceComplete {
	return &InstanceComplete{
		cfg:         cfg,
		ctblr:       c,
		datasets:    d,
		s3:          s,
		vaultClient: v,
		producer:    p,
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
		return &Error{
			err:     fmt.Errorf("failed to get instance: %w", err),
			logData: logData,
		}
	}

	log.Info(ctx, "instance obtained from dataset API", log.Data{
		"instance_id": instance.ID,
	})

	if err := h.ValidateInstance(instance); err != nil{
		return fmt.Errorf("failed to validate instance: %w", err)
	}

	req := cantabular.StaticDatasetQueryRequest{
		Dataset:   e.CantabularBlob,
		Variables: instance.CSVHeader[1:],
	}

	logData["request"] = req

	resp, err := h.ctblr.StaticDatasetQuery(ctx, req)
	if err != nil {
		return &Error{
			err:     fmt.Errorf("failed to query dataset: %w", err),
			logData: logData,
		}
	}

	log.Info(ctx, "response from Cantabular Extended API GraphQL query", log.Data{
		"resp": resp,
	})

	if err := h.ValidateQueryResponse(resp); err != nil {
		return &Error{
			err:     fmt.Errorf("failed to validate query response: %w", err),
			logData: logData,
		}
	}

	// ========================================================================
	// Ticket #5178
	// Convert Cantabular Response To CSV file
	csv, err := h.ParseQueryResponse(resp)
	if err != nil {
		return fmt.Errorf("failed to generate table from query response: %w", err)
	}
	// ========================================================================

	// When planning the tickets we thought there would be another conversion
	// step here but it turns out the sensible code example is for directly
	// creating a CSV file, not just parsing the response into a generic struct.

	// Upload CSV file to S3
	uploadedUrl, err := h.UploadCSVFile(ctx, e.InstanceID, &csv, Encrypted)
	if err != nil {
		return &Error{
			err:     fmt.Errorf("failed to upload .csv file to S3 bucket: %w", err),
			logData: log.Data{
				"bucket":      h.s3.BucketName(),
				"instance_id": e.InstanceID,
			},
		}
	}

	// ========================================================================
	// Ticket #5181
	// Update instance with link to file
	if err := h.UpdateInstance(ctx, e.InstanceID, uploadedUrl, csv.Reader.Size()); err != nil {
		return fmt.Errorf("failed to update instance: %w", err)
	}

	// Generate output kafka message
	if err := h.ProduceExportCompleteEvent(e.InstanceID, uploadedUrl); err != nil {
		return fmt.Errorf("failed to produce export complete kafka message: %w", err)
	}
	// ========================================================================
	return nil
}

// ValidateInstance validates the instance returned from dp-dataset-api
func (h *InstanceComplete) ValidateInstance(i dataset.Instance) error {
	if len(i.CSVHeader) < 2{
		return &Error{
			err: errors.New("no dimensions in headers"),
			logData: log.Data{
				"headers": i.CSVHeader,
			},
		}
	}

	return nil
}

// ValidateQueryResponse validates the query response returned from Cantabular
func (h *InstanceComplete) ValidateQueryResponse(resp *cantabular.StaticDatasetQuery) error {
	if resp == nil {
		return errors.New("nil response")
	}

	return nil
}

func (h *InstanceComplete) ParseQueryResponse(resp *cantabular.StaticDatasetQuery) (bufio.ReadWriter, error) {
	// Here is where we implement the example set out by Sensible Code here:
	// https://github.com/cantabular/examples/blob/master/golang/main.go
	// and referenced in the high level design doc

	// Using a bufio.ReadWriter we should be able to use an object that can be written
	// to by the csv package and then uploaded directly with the S3 package.
	var csv bufio.ReadWriter
	return csv, nil
}

// UploadCSVFile uploads the provided file content to AWS S3
// The file name is the instance ID and a uuid
func (h *InstanceComplete) UploadCSVFile(ctx context.Context, instanceID string, file *bufio.ReadWriter, encrypted bool) (string, error) {
	if instanceID == "" {
		return "", errors.New("empty instance id not allowed")
	}
	if file == nil {
		return "", errors.New("no file content has been provided")
	}

	bucketName := h.s3.BucketName()
	filename := fmt.Sprintf("%s-%s.csv", instanceID, GenerateUUID())

	logData := log.Data{
		"bucket":    bucketName,
		"filename":  filename,
		"encrypted": encrypted,
	}

	if encrypted {
		log.Event(ctx, "uploading private file to S3", log.INFO, logData)

		psk, err := CreatePSK()
		if err != nil {
			return "", NewError(
				fmt.Errorf("failed to generate a PSK for encryption: %w", err),
				logData,
			)
		}

		vaultPath := fmt.Sprintf("%s/%s", h.cfg.VaultPath, path.Base(filename))
		vaultKey := "key"

		log.Event(ctx, "writing key to vault", log.INFO, log.Data{"vault_path": vaultPath})

		if err := h.vaultClient.WriteKey(vaultPath, vaultKey, hex.EncodeToString(psk)); err != nil {
			return "", NewError(
				fmt.Errorf("failed to write key to vault: %w", err), 
				logData,
			)
		}

		result, err := h.s3.UploadWithPSK(&s3manager.UploadInput{
			Body:   file.Reader,
			Bucket: &bucketName,
			Key:    &filename,
		}, psk)
		if err != nil {
			return "", NewError(
				fmt.Errorf("failed to upload file to S3: %w", err),
				logData,
			)
		}

		return url.PathUnescape(result.Location)
	}

	log.Event(ctx, "uploading public file to S3", log.INFO, logData)

	result, err := h.s3.Upload(&s3manager.UploadInput{
		Body:   file.Reader,
		Bucket: &bucketName,
		Key:    &filename,
	})
	if err != nil {
		return "", NewError(
			fmt.Errorf("failed to upload file to S3: %w", err),
			logData,
		)
	}

	return url.PathUnescape(result.Location)
}

// UpdateInstance updates the instance downlad CSV link using dataset API PUT /instances/{id} endpoint
func (h *InstanceComplete) UpdateInstance(ctx context.Context, instanceID, url string, size int) error {
	update := dataset.UpdateInstance{
		Downloads: dataset.DownloadList{
			CSV: &dataset.Download{
				URL:  url,                     // URL of the uploaded CSV file to S3
				Size: fmt.Sprintf("%d", size), // size of the file in bytes
			},
		},
	}
	if _, err := h.datasets.PutInstance(ctx, "", h.cfg.ServiceAuthToken, "", instanceID, update, headers.IfMatchAnyETag); err != nil {
		return fmt.Errorf("error during put instance: %w", err)
	}
	return nil
}

// ProduceExportCompleteEvent sends the final kafka message signifying the export complete
func (h *InstanceComplete) ProduceExportCompleteEvent(instanceID, url string) error {
	// create InstanceComplete event and Marshal it
	bytes, err := schema.CommonOutputCreated.Marshal(&event.CommonOutputCreated{
		InstanceID: instanceID,
		FileURL:    url,
	})
	if err != nil {
		return fmt.Errorf("error marshalling instance complete event: %w", err)
	}

	// Send bytes to kafka producer output channel
	h.producer.Channels().Output <- bytes

	return nil
}

// GenerateUUID returns a new V4 unique ID
var GenerateUUID = func() string {
	return uuid.NewString()
}

// CreatePSK returns a new random array of 16 bytes
var CreatePSK = func() ([]byte, error) {
	key := make([]byte, 16)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	return key, nil
}
