package handler

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/csv"
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
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/log.go/v2/log"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

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

	if err := h.ValidateInstance(instance); err != nil {
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

	if err := h.ValidateQueryResponse(resp); err != nil {
		return &Error{
			err:     fmt.Errorf("failed to validate query response: %w", err),
			logData: logData,
		}
	}

	// Convert Cantabular Response To CSV file
	file, len, err := h.ParseQueryResponse(resp)
	if err != nil {
		return fmt.Errorf("failed to generate table from query response: %w", err)
	}

	// When planning the tickets we thought there would be another conversion
	// step here but it turns out the sensible code example is for directly
	// creating a CSV file, not just parsing the response into a generic struct.

	// Upload CSV file to S3, note that the S3 file location is ignored
	// because we will use download service to access the file
	_, err = h.UploadCSVFile(ctx, e.InstanceID, file)
	if err != nil {
		return &Error{
			err: fmt.Errorf("failed to upload .csv file to S3 bucket: %w", err),
			logData: log.Data{
				"bucket":      h.s3.BucketName(),
				"instance_id": e.InstanceID,
			},
		}
	}

	// Update instance with link to file
	if err := h.UpdateInstance(ctx, e.InstanceID, len); err != nil {
		return fmt.Errorf("failed to update instance: %w", err)
	}

	// Generate output kafka message
	if err := h.ProduceExportCompleteEvent(e.InstanceID); err != nil {
		return fmt.Errorf("failed to produce export complete kafka message: %w", err)
	}
	return nil
}

// ValidateInstance validates the instance returned from dp-dataset-api
func (h *InstanceComplete) ValidateInstance(i dataset.Instance) error {
	if len(i.CSVHeader) < 2 {
		return &Error{
			err: errors.New("no dimensions in headers"),
			logData: log.Data{
				"headers": i.CSVHeader,
			},
		}
	}

	return nil
}

// ValidateQueryResponse validates the query response returned from Cantabular:
// - Is not nil
// - Contains at least one dimension
// - Each dimension count matches the number of categories for that dimension
// - Each dimension variable contains a non-empty label
// - Each dimension category contains a non-empty label
// - The total number of values corresponds to all the permutations of possible dimension categories
func (h *InstanceComplete) ValidateQueryResponse(resp *cantabular.StaticDatasetQuery) error {
	if resp == nil {
		return errors.New("nil response")
	}
	if len(resp.Dataset.Table.Dimensions) == 0 {
		return errors.New("no dimension in response")
	}

	expectedNumValues := 1
	for _, dim := range resp.Dataset.Table.Dimensions {
		expectedNumValues = expectedNumValues * dim.Count

		if dim.Variable.Label == "" {
			return errors.New("empty variable label in cantabular response")
		}

		if len(dim.Categories) != dim.Count {
			return NewError(
				errors.New("wrong number of categories for a dimensions in response"),
				log.Data{
					"dimension":         dim.Variable.Label,
					"dimension_count":   dim.Count,
					"categories_length": len(dim.Categories),
				},
			)
		}

		for _, category := range dim.Categories {
			if category.Label == "" {
				return errors.New("empty category label in cantabular response")
			}
		}
	}

	if len(resp.Dataset.Table.Values) != expectedNumValues {
		return NewError(
			errors.New("wrong number of values in response"),
			log.Data{
				"expected_values": expectedNumValues,
				"values_length":   len(resp.Dataset.Table.Values),
			},
		)
	}

	return nil
}

// ParseQueryResponse parses the provided cantabular response into a CSV bufio.Reader,
// where the first row corresponds to the dimension names header (including a count)
// and each subsequent row corresponds to a unique combination of dimension values and their count.
//
// Example by Sensible Code here: https://github.com/cantabular/examples/blob/master/golang/main.go
func (h *InstanceComplete) ParseQueryResponse(resp *cantabular.StaticDatasetQuery) (*bufio.Reader, int, error) {
	// Create CSV writer with underlying buffer
	b := new(bytes.Buffer)
	w := csv.NewWriter(b)

	// Obtain the CSV header
	header := createCSVHeader(resp.Dataset.Table.Dimensions)
	w.Write(header)
	if err := w.Error(); err != nil {
		return nil, 0, fmt.Errorf("error writing the csv header: %w", err)
	}

	// Obtain the CSV rows according to the cantabular dimensions and counts
	for i, count := range resp.Dataset.Table.Values {
		row := createCSVRow(resp.Dataset.Table.Dimensions, i, count)
		w.Write(row)
		if err := w.Error(); err != nil {
			return nil, 0, fmt.Errorf("error writing a csv row: %w", err)
		}
	}

	// Flush to make sure all data is present in the byte buffer
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, 0, fmt.Errorf("error flushing the csv writer: %w", err)
	}

	// Return a reader with the same underlying Byte buffer as written by the csv writter
	return bufio.NewReader(b), b.Len(), nil
}

// createCSVHeader creates an array of strings corresponding to a csv header
// where each column contains the value of the corresponding dimension, with the last column being the 'count'
func createCSVHeader(dims []cantabular.Dimension) []string {
	header := make([]string, len(dims)+1)
	for i, dim := range dims {
		header[i] = dim.Variable.Label
	}
	header[len(dims)] = "count"
	return header
}

// createCSVRow creates an array of strings corresponding to a csv row
// for the provided array of dimension, index and count
// it assumes that the values are sorted with lower weight for the last dimension and higher weight for the first dimension.
func createCSVRow(dims []cantabular.Dimension, index, count int) []string {
	row := make([]string, len(dims)+1)
	// Iterate dimensions starting from the last one (lower weight)
	for i := len(dims) - 1; i >= 0; i-- {
		catIndex := index % dims[i].Count           // Index of the category for the current dimension
		row[i] = dims[i].Categories[catIndex].Label // The CSV column corresponds to the label of the Category
		index = index / dims[i].Count               // Modify index for next iteration
	}
	row[len(dims)] = fmt.Sprintf("%d", count)
	return row
}

// UploadCSVFile uploads the provided file content to AWS S3
func (h *InstanceComplete) UploadCSVFile(ctx context.Context, instanceID string, file io.Reader) (string, error) {
	if instanceID == "" {
		return "", errors.New("empty instance id not allowed")
	}
	if file == nil {
		return "", errors.New("no file content has been provided")
	}

	bucketName := h.s3.BucketName()
	filename := generateS3Filename(instanceID)

	logData := log.Data{
		"bucket":              bucketName,
		"filename":            filename,
		"encryption_disabled": h.cfg.EncryptionDisabled,
	}

	if h.cfg.EncryptionDisabled {
		log.Info(ctx, "uploading unencrypted file to S3", logData)

		result, err := h.s3.Upload(&s3manager.UploadInput{
			Body:   file,
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

	log.Info(ctx, "uploading encrypted file to S3", logData)

	psk, err := CreatePSK()
	if err != nil {
		return "", NewError(
			fmt.Errorf("failed to generate a PSK for encryption: %w", err),
			logData,
		)
	}

	vaultPath := generateVaultPathForFile(h.cfg.VaultPath, filename)
	vaultKey := "key"

	log.Info(ctx, "writing key to vault", log.Data{"vault_path": vaultPath})

	if err := h.vaultClient.WriteKey(vaultPath, vaultKey, hex.EncodeToString(psk)); err != nil {
		return "", NewError(
			fmt.Errorf("failed to write key to vault: %w", err),
			logData,
		)
	}

	result, err := h.s3.UploadWithPSK(&s3manager.UploadInput{
		Body:   file,
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

// UpdateInstance updates the instance downlad CSV link using dataset API PUT /instances/{id} endpoint
// note that the URL refers to the download service (it is not the URL returned by the S3 client directly)
func (h *InstanceComplete) UpdateInstance(ctx context.Context, instanceID string, size int) error {
	downloadURL := generateURL(h.cfg.DownloadServiceURL, instanceID)
	update := dataset.UpdateInstance{
		Downloads: dataset.DownloadList{
			CSV: &dataset.Download{
				URL:  downloadURL,             // download service URL for the CSV file
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
func (h *InstanceComplete) ProduceExportCompleteEvent(instanceID string) error {
	downloadURL := generateURL(h.cfg.DownloadServiceURL, instanceID)

	// create InstanceComplete event and Marshal it
	bytes, err := schema.CommonOutputCreated.Marshal(&event.CommonOutputCreated{
		InstanceID: instanceID,
		FileURL:    downloadURL, // download service URL for the CSV file
	})
	if err != nil {
		return fmt.Errorf("error marshalling instance complete event: %w", err)
	}

	// Send bytes to kafka producer output channel
	h.producer.Channels().Output <- bytes

	return nil
}

// CreatePSK returns a new random array of 16 bytes
var CreatePSK = func() ([]byte, error) {
	key := make([]byte, 16)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	return key, nil
}

// generateURL generates the download service URL for the provided instanceID CSV file
func generateURL(downloadServiceURL, instanceID string) string {
	return fmt.Sprintf("%s/downloads/instances/%s.csv",
		downloadServiceURL,
		instanceID,
	)
}

// generateS3Filename generates the S3 key (filename including `subpaths` after the bucket) for the provided instanceID
func generateS3Filename(instanceID string) string {
	return fmt.Sprintf("instances/%s.csv", instanceID)
}

// generateVaultPathForFile generates the vault path for the provided root and filename
func generateVaultPathForFile(vaultPathRoot, filename string) string {
	return fmt.Sprintf("%s/%s", vaultPathRoot, filename)
}
