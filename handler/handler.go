package handler

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/ONSdigital/dp-api-clients-go/v2/cantabular"
	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-api-clients-go/v2/headers"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/config"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/event"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/schema"
	kafka "github.com/ONSdigital/dp-kafka/v3"
	"github.com/ONSdigital/log.go/v2/log"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// InstanceComplete is the handle for the InstanceCompleteHandler event
type InstanceComplete struct {
	cfg         config.Config
	ctblr       CantabularClient
	datasets    DatasetAPIClient
	filters     FilterAPIClient
	s3Private   S3Client
	s3Public    S3Client
	vaultClient VaultClient
	producer    kafka.IProducer
	generator   Generator
}

// NewInstanceComplete creates a new InstanceCompleteHandler
func NewInstanceComplete(cfg config.Config, c CantabularClient, d DatasetAPIClient, f FilterAPIClient, sPrivate, sPublic S3Client, v VaultClient, p kafka.IProducer, g Generator) *InstanceComplete {
	return &InstanceComplete{
		cfg:         cfg,
		ctblr:       c,
		filters:     f,
		datasets:    d,
		s3Private:   sPrivate,
		s3Public:    sPublic,
		vaultClient: v,
		producer:    p,
		generator:   g,
	}
}

// Handle takes a single event.
func (h *InstanceComplete) Handle(ctx context.Context, workerID int, msg kafka.Message) error {

	e := &event.ExportStart{}
	s := schema.ExportStart

	if err := s.Unmarshal(msg.GetData(), e); err != nil {
		return &Error{
			err: fmt.Errorf("failed to unmarshal event: %w", err),
			logData: map[string]interface{}{
				"msg_data": msg.GetData(),
			},
		}
	}

	logData := log.Data{"event": e}
	log.Info(ctx, "event received", logData)

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

	// validate the instance and determine wether it is published or not
	isPublished, err := h.ValidateInstance(instance)
	if err != nil {
		return fmt.Errorf("failed to validate instance: %w", err)
	}

	req := cantabular.StaticDatasetQueryRequest{
		Dataset:   instance.IsBasedOn.ID, // This value corresponds to the CantabularBlob that was used in import process
		Variables: instance.CSVHeader[1:],
	}

	if e.FilterID != "" {
		dimensionNames, populationType, err := h.getFilterInfo(ctx, e.FilterID, logData)
		if err != nil {
			return err
		}
		req.Dataset = populationType
		req.Variables = dimensionNames
	}

	logData["request"] = req

	// Stream data from Cantabular to S3 bucket in CSV format
	s3Url, rowCount, err := h.UploadCSVFile(ctx, e, isPublished, req)
	if err != nil {
		return &Error{
			err:     fmt.Errorf("failed to upload .csv file to S3 bucket: %w", err),
			logData: logData,
		}
	}

	numBytes, err := h.GetS3ContentLength(ctx, e, isPublished)
	if err != nil {
		return &Error{
			err:     fmt.Errorf("failed to get S3 content length: %w", err),
			logData: logData,
		}
	}

	// Update instance with link to file
	if err := h.UpdateInstance(ctx, e, numBytes, isPublished, s3Url); err != nil {
		return fmt.Errorf("failed to update instance: %w", err)
	}

	log.Event(ctx, "producing  event", log.INFO, log.Data{})

	// Generate output kafka message
	if err := h.ProduceExportCompleteEvent(e, rowCount); err != nil {
		return fmt.Errorf("failed to produce export complete kafka message: %w", err)
	}
	return nil
}

func (h *InstanceComplete) getFilterInfo(ctx context.Context, filterID string, logData log.Data) ([]string, string, error) {
	dimensions, _, err := h.filters.GetDimensions(ctx, "", h.cfg.ServiceAuthToken, "", filterID, nil)
	if err != nil {
		return nil, "", &Error{
			err:     fmt.Errorf("failed to get dimensions: %w", err),
			logData: logData,
		}
	}

	dimensionNames := make([]string, 0)
	for _, d := range dimensions.Items {
		dimensionNames = append(dimensionNames, d.Name)
	}

	model, _, _ := h.filters.GetJobState(ctx, "", h.cfg.ServiceAuthToken, "", "", filterID)
	if err != nil {
		return nil, "", &Error{
			err:     fmt.Errorf("failed to get filter: %w", err),
			logData: logData,
		}
	}
	return dimensionNames, model.PopulationType, nil
}

// ValidateInstance validates the instance returned from dp-dataset-api
// Returns isPublished bool value and any validation error
func (h *InstanceComplete) ValidateInstance(i dataset.Instance) (bool, error) {
	if len(i.CSVHeader) < 2 {
		return false, &Error{
			err: errors.New("no dimensions in headers"),
			logData: log.Data{
				"headers": i.CSVHeader,
			},
		}
	}

	if i.IsBasedOn == nil || len(i.IsBasedOn.ID) == 0 {
		return false, &Error{
			err: errors.New("missing instance isBasedOn.ID"),
			logData: log.Data{
				"is_based_on": i.IsBasedOn,
			},
		}
	}

	return i.State == dataset.StatePublished.String(), nil
}

// UploadCSVFile queries a static dataset to cantabular using the provided request,
// transforms the response to a CSV format and streams the data to S3.
// If the data is published, the S3 file will be stored in the public bucket
// If the data is private, the S3 file will be stored in the private bucket (encrypted or un-encrypted depending on EncryptionDisabled flag)
// Returns S3 file URL, file size [bytes], number of rows, and any error that happens.
func (h *InstanceComplete) UploadCSVFile(ctx context.Context, e *event.ExportStart, isPublished bool, req cantabular.StaticDatasetQueryRequest) (string, int32, error) {
	if e.InstanceID == "" {
		return "", 0, errors.New("empty instance id not allowed")
	}
	var s3Location string
	var consume cantabular.Consumer

	filename := generateS3Filename(e)
	logData := log.Data{
		"filename":     filename,
		"is_published": isPublished,
	}

	if isPublished {
		bucketName := h.s3Public.BucketName()
		logData["bucket"] = bucketName

		// stream consumer/uploader for public files
		consume = func(ctx context.Context, file io.Reader) error {
			if file == nil {
				return errors.New("no file content has been provided")
			}
			log.Info(ctx, "uploading published file to S3", logData)

			result, err := h.s3Public.UploadWithContext(ctx, &s3manager.UploadInput{
				Body:   file,
				Bucket: &bucketName,
				Key:    &filename,
			})
			if err != nil {
				return fmt.Errorf("failed to upload published file to S3: %w", err)
			}

			s3Location, err = url.PathUnescape(result.Location)
			if err != nil {
				logData["location"] = result.Location
				return NewError(
					fmt.Errorf("failed to unescape S3 path location: %w", err),
					logData,
				)
			}
			return nil
		}
	} else {
		bucketName := h.s3Private.BucketName()
		logData["bucket"] = bucketName
		logData["encryption_disabled"] = h.cfg.EncryptionDisabled

		if h.cfg.EncryptionDisabled {
			// stream consumer/uploader for un-encrypted private files
			consume = func(ctx context.Context, file io.Reader) error {
				if file == nil {
					return errors.New("no file content has been provided")
				}
				log.Info(ctx, "uploading un-encrypted private file to S3", logData)

				result, err := h.s3Private.UploadWithContext(ctx, &s3manager.UploadInput{
					Body:   file,
					Bucket: &bucketName,
					Key:    &filename,
				})
				if err != nil {
					return fmt.Errorf("failed to upload un-encrypted private file to S3: %w", err)
				}

				s3Location, err = url.PathUnescape(result.Location)
				if err != nil {
					logData["location"] = result.Location
					return NewError(
						fmt.Errorf("failed to unescape S3 path location: %w", err),
						logData,
					)
				}
				return nil
			}
		} else {
			psk, err := h.generator.NewPSK()
			if err != nil {
				return "", 0, NewError(
					fmt.Errorf("failed to generate a PSK for encryption: %w", err),
					logData,
				)
			}

			vaultPath := generateVaultPathForFile(h.cfg.VaultPath, e)
			vaultKey := "key"
			log.Info(ctx, "writing key to vault", log.Data{"vault_path": vaultPath})

			if err := h.vaultClient.WriteKey(vaultPath, vaultKey, hex.EncodeToString(psk)); err != nil {
				return "", 0, NewError(
					fmt.Errorf("failed to write key to vault: %w", err),
					logData,
				)
			}

			// stream consumer/uploader for encrypted private files
			consume = func(ctx context.Context, file io.Reader) error {
				if file == nil {
					return errors.New("no file content has been provided")
				}
				log.Info(ctx, "uploading encrypted private file to S3", logData)

				result, err := h.s3Private.UploadWithPSK(&s3manager.UploadInput{
					Body:   file,
					Bucket: &bucketName,
					Key:    &filename,
				}, psk)
				if err != nil {
					return fmt.Errorf("failed to upload encrypted private file to S3: %w", err)
				}

				s3Location, err = url.PathUnescape(result.Location)
				if err != nil {
					logData["location"] = result.Location
					return NewError(
						fmt.Errorf("failed to unescape S3 path location: %w", err),
						logData,
					)
				}
				return nil
			}
		}
	}

	rowCount, err := h.ctblr.StaticDatasetQueryStreamCSV(ctx, req, consume)
	if err != nil {
		return "", 0, &Error{
			err:     fmt.Errorf("failed to stream csv data: %w", err),
			logData: logData,
		}
	}

	return s3Location, rowCount, nil
}

// GetS3ContentLength obtains an S3 file size (in number of bytes) by calling Head Object
func (h *InstanceComplete) GetS3ContentLength(ctx context.Context, e *event.ExportStart, isPublished bool) (int, error) {
	filename := generateS3Filename(e)
	if isPublished {
		headOutput, err := h.s3Public.Head(filename)
		if err != nil {
			return 0, fmt.Errorf("public s3 head object error: %w", err)
		}
		return int(*headOutput.ContentLength), nil
	}
	headOutput, err := h.s3Private.Head(filename)
	if err != nil {
		return 0, fmt.Errorf("private s3 head object error: %w", err)
	}
	return int(*headOutput.ContentLength), nil
}

// UpdateInstance updates the instance downlad CSV link using dataset API PUT /instances/{id} endpoint
// if the instance is published, then the s3Url will be set as public link and the instance state will be set to published
// otherwise, a private url will be generated and the state will not be changed
func (h *InstanceComplete) UpdateInstance(ctx context.Context, e *event.ExportStart, size int, isPublished bool, s3Url string) error {
	csvDownload := dataset.Download{
		Size: fmt.Sprintf("%d", size),
		URL: fmt.Sprintf("%s/downloads/datasets/%s/editions/%s/versions/%s.csv",
			h.cfg.DownloadServiceURL,
			e.DatasetID,
			e.Edition,
			e.Version,
		),
	}

	if isPublished {
		csvDownload.Public = s3Url
	} else {
		csvDownload.Private = s3Url
	}

	log.Info(ctx, "updating dataset api with download link", log.Data{
		"is_published": isPublished,
		"csv_download": csvDownload,
	})

	versionUpdate := dataset.Version{
		Downloads: map[string]dataset.Download{
			"CSV": csvDownload,
		},
	}

	err := h.datasets.PutVersion(
		ctx, "", h.cfg.ServiceAuthToken, "", e.DatasetID, e.Edition, e.Version, versionUpdate)
	if err != nil {
		return fmt.Errorf("error while attempting update version downloads: %w", err)
	}

	return nil
}

// ProduceExportCompleteEvent sends the final kafka message signifying the export complete
func (h *InstanceComplete) ProduceExportCompleteEvent(e *event.ExportStart, rowCount int32) error {
	if err := h.producer.Send(schema.CSVCreated, &event.CSVCreated{
		InstanceID: e.InstanceID,
		DatasetID:  e.DatasetID,
		Edition:    e.Edition,
		Version:    e.Version,
		RowCount:   rowCount,
	}); err != nil {
		return fmt.Errorf("error sending csv-created event: %w", err)
	}
	return nil
}

// generateS3Filename generates the S3 key (filename including `subpaths` after the bucket) for the provided instanceID
func generateS3Filename(e *event.ExportStart) string {
	if e.FilterID != "" {
		return fmt.Sprintf("datasets/%s-%s-%s-filtered-%s.csv", e.DatasetID, e.Edition, e.Version, time.Now().Format(time.RFC3339))
	}
	return fmt.Sprintf("datasets/%s-%s-%s.csv", e.DatasetID, e.Edition, e.Version)
}

// generateVaultPathForFile generates the vault path for the provided root and filename
func generateVaultPathForFile(vaultPathRoot string, e *event.ExportStart) string {
	if e.FilterID != "" {
		return fmt.Sprintf("%s/%s-%s-%s-filtered-%s.csv", vaultPathRoot, e.DatasetID, e.Edition, e.Version, time.Now().Format(time.RFC3339))
	}
	return fmt.Sprintf("%s/%s-%s-%s.csv", vaultPathRoot, e.DatasetID, e.Edition, e.Version)
}
