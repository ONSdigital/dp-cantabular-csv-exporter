package handler

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"

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
	s3Private   S3Uploader
	s3Public    S3Uploader
	vaultClient VaultClient
	producer    kafka.IProducer
	generator   Generator
}

// NewInstanceComplete creates a new InstanceCompleteHandler
func NewInstanceComplete(cfg config.Config, c CantabularClient, d DatasetAPIClient, sPrivate, sPublic S3Uploader, v VaultClient, p kafka.IProducer, g Generator) *InstanceComplete {
	return &InstanceComplete{
		cfg:         cfg,
		ctblr:       c,
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
	e := &event.InstanceComplete{}
	s := schema.InstanceComplete

	log.Info(ctx, "message data", log.Data{"msg_data": msg.GetData()})

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

	isPublished, err := h.ValidateInstance(instance)
	if err != nil {
		return fmt.Errorf("failed to validate instance: %w", err)
	}

	req := cantabular.StaticDatasetQueryRequest{
		Dataset:   e.CantabularBlob,
		Variables: instance.CSVHeader[1:],
	}
	logData["request"] = req

	// Upload csv file to S3 bucket
	// TODO: The S3 file location returned by the Uploader should be ignored and we should use Download Service instead, which will:
	// - decrypt private files uploaded to the private bucket (for authorised users only)
	// - or it will redirect to the public S3 URL for public files.
	// This change will be possible once the Cantabular CSV exporter is triggered by the Dataset API on Dataset 'Associated' (private) or 'Published' (public) events
	s3Url, numBytes, rowCount, err := h.UploadCSVFile(ctx, e.InstanceID, isPublished, req)
	if err != nil {
		return &Error{
			err:     fmt.Errorf("failed to upload .csv file to S3 bucket: %w", err),
			logData: logData,
		}
	}

	// Update instance with link to file
	if err := h.UpdateInstance(ctx, e.InstanceID, numBytes, isPublished, s3Url); err != nil {
		return fmt.Errorf("failed to update instance: %w", err)
	}

	log.Event(ctx, "producing  event", log.INFO, log.Data{})

	// Generate output kafka message
	if err := h.ProduceExportCompleteEvent(e.InstanceID, isPublished, s3Url, rowCount); err != nil {
		return fmt.Errorf("failed to produce export complete kafka message: %w", err)
	}
	return nil
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

	// TODO check if instance is published
	return true, nil
}

// UploadCSVFile queries a static dataset to cantabular using the provided request,
// transforms the response to a CSV format and streams the data to S3.
// If the data is published, the S3 file will be stored in the public bucket
// If the data is private, the S3 file will be stored in the private bucket (encrypted or un-encrypted depending on EncryptionDisabled flag)
// Returns S3 file URL, file size [bytes], number of rows, and any error that happens.
func (h *InstanceComplete) UploadCSVFile(ctx context.Context, instanceID string, isPublished bool, req cantabular.StaticDatasetQueryRequest) (string, int, int32, error) {
	if instanceID == "" {
		return "", 0, 0, errors.New("empty instance id not allowed")
	}
	var s3Location string
	var consume cantabular.Consumer

	filename := generateS3Filename(instanceID)
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

			result, err := h.s3Public.Upload(&s3manager.UploadInput{
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
				log.Info(ctx, "uploading private file to S3", logData)

				result, err := h.s3Private.Upload(&s3manager.UploadInput{
					Body:   file,
					Bucket: &bucketName,
					Key:    &filename,
				})
				if err != nil {
					return fmt.Errorf("failed to upload private file to S3: %w", err)
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
				return "", 0, 0, NewError(
					fmt.Errorf("failed to generate a PSK for encryption: %w", err),
					logData,
				)
			}

			vaultPath := generateVaultPathForFile(h.cfg.VaultPath, instanceID)
			vaultKey := "key"
			log.Info(ctx, "writing key to vault", log.Data{"vault_path": vaultPath})

			if err := h.vaultClient.WriteKey(vaultPath, vaultKey, hex.EncodeToString(psk)); err != nil {
				return "", 0, 0, NewError(
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

	// TODO check file size with an S3 HEAD operation
	numBytes := 123

	rowCount, err := h.ctblr.StaticDatasetQueryStreamCSV(ctx, req, consume)
	if err != nil {
		return "", 0, 0, &Error{
			err:     fmt.Errorf("failed to stream csv data: %w", err),
			logData: logData,
		}
	}

	return s3Location, numBytes, rowCount, nil
}

// UpdateInstance updates the instance downlad CSV link using dataset API PUT /instances/{id} endpoint
// if the instance is published, then the s3Url will be set as public link and the instance state will be set to published
// otherwise, a private url will be generated and the state will not be changed
func (h *InstanceComplete) UpdateInstance(ctx context.Context, instanceID string, size int, isPublished bool, s3Url string) error {
	if isPublished {
		update := dataset.UpdateInstance{
			Downloads: dataset.DownloadList{
				CSV: &dataset.Download{
					URL:    s3Url,                   // Public URL, as returned by the S3 uploader
					Public: s3Url,                   // Public URL, as returned by the S3 uploader
					Size:   fmt.Sprintf("%d", size), // size of the file in number of bytes
				},
			},
		}
		if _, err := h.datasets.PutInstance(ctx, "", h.cfg.ServiceAuthToken, "", instanceID, update, headers.IfMatchAnyETag); err != nil {
			return fmt.Errorf("error during put instance: %w", err)
		}
		return nil
	}

	update := dataset.UpdateInstance{
		Downloads: dataset.DownloadList{
			CSV: &dataset.Download{
				Size: fmt.Sprintf("%d", size),                                  // size of the file in number of bytes
				URL:  generatePrivateURL(h.cfg.DownloadServiceURL, instanceID), // Private downloadService URL
			},
		},
	}
	if _, err := h.datasets.PutInstance(ctx, "", h.cfg.ServiceAuthToken, "", instanceID, update, headers.IfMatchAnyETag); err != nil {
		return fmt.Errorf("error during put instance: %w", err)
	}
	return nil
}

// ProduceExportCompleteEvent sends the final kafka message signifying the export complete
func (h *InstanceComplete) ProduceExportCompleteEvent(instanceID string, isPublished bool, s3Url string, rowCount int32) error {
	var downloadURL string
	if isPublished {
		downloadURL = s3Url
	} else {
		downloadURL = generatePrivateURL(h.cfg.DownloadServiceURL, instanceID)
	}

	if err := h.producer.Send(schema.CSVCreated, &event.CSVCreated{
		InstanceID: instanceID,
		FileURL:    downloadURL, // download service URL for the CSV file
		RowCount:   rowCount,
	}); err != nil {
		return fmt.Errorf("error sending csv-created event: %w", err)
	}
	return nil
}

// generatePrivateURL generates the download service private URL for the provided instanceID CSV file
func generatePrivateURL(downloadServiceURL, instanceID string) string {
	return fmt.Sprintf("%s/downloads/instances/%s.csv",
		downloadServiceURL,
		instanceID,
	)
}

// generateS3Filename generates the S3 key (filename including `subpaths` after the bucket) for the provided instanceID
// TODO filename should be datasets/<dataset_name>_<version>.csv to match CMD naming
func generateS3Filename(instanceID string) string {
	return fmt.Sprintf("instances/%s.csv", instanceID)
}

// generateVaultPathForFile generates the vault path for the provided root and filename
func generateVaultPathForFile(vaultPathRoot, instanceID string) string {
	return fmt.Sprintf("%s/%s.csv", vaultPathRoot, instanceID)
}
