package handler

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/ONSdigital/dp-api-clients-go/v2/cantabular"
	"github.com/ONSdigital/dp-api-clients-go/v2/dataset"
	"github.com/ONSdigital/dp-api-clients-go/v2/filter"
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
	var isPublished bool
	e := &event.ExportStart{}
	s := schema.ExportStart

	if err := s.Unmarshal(msg.GetData(), e); err != nil {
		return &Error{
			err: errors.Wrap(err, "failed to unmarshal event"),
			logData: map[string]interface{}{
				"msg_data": string(msg.GetData()),
			},
		}
	}

	logData := log.Data{"event": e}
	log.Info(ctx, "event received", logData)

	var err error
	req := cantabular.StaticDatasetQueryRequest{}
	isFilterJob := e.FilterOutputID != ""

	if isFilterJob {
		req.Variables, req.Filters, req.Dataset, isPublished, err = h.getFilterInfo(ctx, e.FilterOutputID, logData)
		if err != nil {
			return errors.Wrap(err, "failed to get filter info")
		}
	} else {
		req.Dataset, req.Variables, isPublished, err = h.getInstanceInfo(ctx, e.InstanceID, logData)
		if err != nil {
			return errors.Wrap(err, "failed to get instance info")
		}
	}

	logData["request"] = req

	// Stream data from Cantabular to S3 bucket in CSV format
	s3Url, rowCount, filename, err := h.UploadCSVFile(ctx, e, isPublished, req)
	if err != nil {
		return &Error{
			err:     fmt.Errorf("failed to upload .csv file to S3 bucket: %w", err),
			logData: logData,
		}
	}

	numBytes, err := h.GetS3ContentLength(ctx, e, isPublished, filename)
	if err != nil {
		return &Error{
			err:     fmt.Errorf("failed to get S3 content length: %w", err),
			logData: logData,
		}
	}

	if isFilterJob {
		if err := h.UpdateFilterOutput(ctx, e, numBytes, isPublished, s3Url); err != nil {
			return errors.Wrap(err, "failed to update filter output")
		}
	} else {
		if err := h.UpdateInstance(ctx, e, numBytes, isPublished, s3Url); err != nil {
			return errors.Wrap(err, "failed to update instance")
		}
	}

	log.Info(ctx, "producing event")

	//just pass the file name
	f := strings.Replace(filename, "datasets/", "", 1)
	if err := h.ProduceExportCompleteEvent(e, rowCount, f); err != nil {
		return fmt.Errorf("failed to produce export complete kafka message: %w", err)
	}

	return nil
}

func (h *InstanceComplete) getFilterInfo(ctx context.Context, filterOutputID string, logData log.Data) ([]string, []cantabular.Filter, string, bool, error) {
	model, err := h.filters.GetOutput(ctx, "", h.cfg.ServiceAuthToken, "", "", filterOutputID)
	if err != nil {
		return nil, nil, "", false, &Error{
			err:     errors.Wrap(err, "failed to get filter"),
			logData: logData,
		}
	}

	dimensions := model.Dimensions

	dimensionIds := make([]string, 0)
	filters := make([]cantabular.Filter, 0)
	for _, d := range dimensions {
		dimensionIds = append(dimensionIds, d.ID)
		if len(d.Options) > 0 {
			v := d.ID
			if len(d.FilterByParent) != 0 {
				v = d.FilterByParent
			}
			filters = append(filters, cantabular.Filter{
				Codes:    d.Options,
				Variable: v,
			})
		}
	}

	isPublished := model.IsPublished

	return dimensionIds, filters, model.PopulationType, isPublished, nil
}

func (h *InstanceComplete) getInstanceInfo(ctx context.Context, instanceID string, logData log.Data) (string, []string, bool, error) {
	instance, _, err := h.datasets.GetInstance(ctx, "", h.cfg.ServiceAuthToken, "", instanceID, headers.IfMatchAnyETag)
	if err != nil {
		return "", nil, false, &Error{
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
		return "", nil, false, fmt.Errorf("failed to validate instance: %w", err)
	}

	return instance.IsBasedOn.ID, instance.CSVHeader[1:], isPublished, err
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
func (h *InstanceComplete) UploadCSVFile(ctx context.Context, e *event.ExportStart, isPublished bool, req cantabular.StaticDatasetQueryRequest) (string, int32, string, error) {
	if e.InstanceID == "" {
		return "", 0, "", errors.New("empty instance id not allowed")
	}
	var s3Location string
	var consume cantabular.Consumer

	filename, err := h.generateS3Filename(e)
	if err != nil {
		return "", 0, "", &Error{
			err: fmt.Errorf("failed to generate S3 filename: %w", err),
		}
	}

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
				return "", 0, "", NewError(
					fmt.Errorf("failed to generate a PSK for encryption: %w", err),
					logData,
				)
			}

			vaultPath, err := h.generateVaultPathForFile(h.cfg.VaultPath, e)
			if err != nil {
				return "", 0, "", NewError(
					fmt.Errorf("failed to generate a vault path for file: %w", err),
					logData,
				)
			}

			vaultKey := "key"
			log.Info(ctx, "writing key to vault", log.Data{"vault_path": vaultPath})

			if err := h.vaultClient.WriteKey(vaultPath, vaultKey, hex.EncodeToString(psk)); err != nil {
				return "", 0, "", NewError(
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
		return "", 0, "", &Error{
			err:     fmt.Errorf("failed to stream csv data: %w", err),
			logData: logData,
		}
	}

	return s3Location, rowCount, filename, nil
}

// GetS3ContentLength obtains an S3 file size (in number of bytes) by calling Head Object
func (h *InstanceComplete) GetS3ContentLength(ctx context.Context, e *event.ExportStart, isPublished bool, filename string) (int, error) {
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
		ctx,
		"",
		h.cfg.ServiceAuthToken,
		"",
		e.DatasetID,
		e.Edition,
		e.Version,
		versionUpdate,
	)
	if err != nil {
		return fmt.Errorf("error while attempting update version downloads: %w", err)
	}

	return nil
}

// ProduceExportCompleteEvent sends the final kafka message signifying the export complete
func (h *InstanceComplete) ProduceExportCompleteEvent(e *event.ExportStart, rowCount int32, fileName string) error {
	if err := h.producer.Send(schema.CSVCreated, &event.CSVCreated{
		InstanceID:     e.InstanceID,
		DatasetID:      e.DatasetID,
		Edition:        e.Edition,
		Version:        e.Version,
		RowCount:       rowCount,
		FileName:       fileName,
		FilterOutputID: e.FilterOutputID,
		Dimensions:     e.Dimensions,
	}); err != nil {
		return fmt.Errorf("error sending csv-created event: %w", err)
	}
	return nil
}

func (h *InstanceComplete) UpdateFilterOutput(ctx context.Context, e *event.ExportStart, size int, isPublished bool, s3Url string) error {
	log.Info(ctx, "Updating filter output with download link")

	download := filter.Download{
		URL:     fmt.Sprintf("%s/downloads/filter-outputs/%s.csv", h.cfg.DownloadServiceURL, e.FilterOutputID),
		Size:    fmt.Sprintf("%d", size),
		Skipped: false,
	}

	if isPublished {
		download.Public = s3Url
	} else {
		download.Private = s3Url
	}

	m := filter.Model{
		Downloads: map[string]filter.Download{
			"CSV": download,
		},
	}

	if err := h.filters.UpdateFilterOutput(ctx, "", h.cfg.ServiceAuthToken, "", e.FilterOutputID, &m); err != nil {
		return errors.Wrap(err, "failed to update filter output")
	}

	return nil
}

// generateS3Filename generates the S3 key (filename including `subpaths` after the bucket) for the provided instanceID
func (h *InstanceComplete) generateS3Filename(e *event.ExportStart) (string, error) {
	if e.FilterOutputID != "" {
		generateUUID, err := h.generator.UniqueID()
		if err != nil {
			return "", errors.Wrap(err, "failed to generate an UUID for the S3 filename")
		}
		return fmt.Sprintf("datasets/%s-%s-%s-filtered-%s-%s.csv", e.DatasetID, e.Edition, e.Version,
			h.generator.Timestamp().Format(time.RFC3339), generateUUID), nil
	}
	return fmt.Sprintf("datasets/%s-%s-%s.csv", e.DatasetID, e.Edition, e.Version), nil
}

// generateVaultPathForFile generates the vault path for the provided root and filename
func (h *InstanceComplete) generateVaultPathForFile(vaultPathRoot string, e *event.ExportStart) (string, error) {
	if e.FilterOutputID != "" {
		generateUUID, err := h.generator.UniqueID()
		if err != nil {
			return "", errors.Wrap(err, "failed to generate an UUID for the S3 filename")
		}
		return fmt.Sprintf("%s/%s-%s-%s-filtered-%s-%s.csv", vaultPathRoot, e.DatasetID, e.Edition, e.Version,
			h.generator.Timestamp().Format(time.RFC3339), generateUUID), nil
	}
	return fmt.Sprintf("%s/%s-%s-%s.csv", vaultPathRoot, e.DatasetID, e.Edition, e.Version), nil
}
