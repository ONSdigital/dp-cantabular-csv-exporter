package steps

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ONSdigital/dp-cantabular-csv-exporter/event"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/schema"
	"github.com/ONSdigital/log.go/v2/log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"

	assistdog "github.com/ONSdigital/dp-assistdog"
	"github.com/cucumber/godog"
	"github.com/google/go-cmp/cmp"
)

// RegisterSteps maps the human-readable regular expressions to their corresponding funcs
func (c *Component) RegisterSteps(ctx *godog.ScenarioContext) {
	ctx.Step(`^the following response is available from Cantabular from the codebook "([^"]*)" using the GraphQL endpoint:$`, c.theFollowingQueryResponseIsAvailable)
	ctx.Step(`^the service starts`, c.theServiceStarts)
	ctx.Step(`^dp-dataset-api is healthy`, c.datasetAPIIsHealthy)
	ctx.Step(`^dp-dataset-api is unhealthy`, c.datasetAPIIsUnhealthy)
	ctx.Step(`^filter API is healthy`, c.filterAPIIsHealthy)
	ctx.Step(`^filter API is unhealthy`, c.filterAPIIsUnhealthy)
	ctx.Step(`^cantabular server is healthy`, c.cantabularServerIsHealthy)
	ctx.Step(`^cantabular api extension is healthy`, c.cantabularApiExtensionIsHealthy)
	ctx.Step(`^the following instance with id "([^"]*)" is available from dp-dataset-api:$`, c.theFollowingInstanceIsAvailable)
	ctx.Step(`^a dataset version with dataset-id "([^"]*)", edition "([^"]*)" and version "([^"]*)" is updated by an API call to dp-dataset-api`, c.theFollowingVersionIsUpdated)
	ctx.Step(`^for the following filter "([^"]*)" these dimensions are available:$`, c.theFollowingFilterDimensionsExist)
	ctx.Step(`^the following job state is returned for the filter "([^"]*)":$`, c.theFollowingJobStateIsReturned)
	ctx.Step(`^this cantabular-export-start event is queued, to be consumed:$`, c.thisExportStartEventIsQueued)
	ctx.Step(`^one event with the following fields are in the produced kafka topic cantabular-csv-created:$`, c.theseCsvCreatedEventsAreProduced)
	ctx.Step(`^no cantabular-csv-created events are produced`, c.noCsvCreatedEventsAreProduced)
	ctx.Step(`^a public file with filename "([^"]*)" can be seen in minio`, c.theFollowingPublicFileCanBeSeenInMinio)
	ctx.Step(`^a public filtered file, that should contain "([^"]*)" on the filename can be seen in minio`, c.theFollowingPublicFilteredFileCanBeSeenInMinio)
	ctx.Step(`^a private file with filename "([^"]*)" can be seen in minio`, c.theFollowingPrivateFileCanBeSeenInMinio)
}

// theServiceStarts starts the service under test in a new go-routine
// note that this step should be called only after all dependencies have been setup,
// to prevent any race condition, specially during the first healthcheck iteration.
func (c *Component) theServiceStarts() error {
	return c.startService(c.ctx)
}

// datasetAPIIsHealthy generates a mocked healthy response for dataset API healthecheck
func (c *Component) datasetAPIIsHealthy() error {
	const res = `{"status": "OK"}`
	c.DatasetAPI.NewHandler().
		Get("/health").
		Reply(http.StatusOK).
		BodyString(res)
	return nil
}

// datasetAPIIsUnhealthy generates a mocked unhealthy response for dataset API healthcheck
func (c *Component) datasetAPIIsUnhealthy() error {
	const res = `{"status": "CRITICAL"}`
	c.DatasetAPI.NewHandler().
		Get("/health").
		Reply(http.StatusInternalServerError).
		BodyString(res)
	return nil
}

// filterAPIIsHealthy generates a mocked healthy response for the filter API
func (c *Component) filterAPIIsHealthy() error {
	const res = `{"status": "OK"}`
	c.FilterAPI.NewHandler().
		Get("/health").
		Reply(http.StatusOK).
		BodyString(res)
	return nil
}

// filterAPIIsUnhealthy generates a mocked unhealthy response for the filter API
func (c *Component) filterAPIIsUnhealthy() error {
	const res = `{"status": "CRITICAL"}`
	c.DatasetAPI.NewHandler().
		Get("/health").
		Reply(http.StatusInternalServerError).
		BodyString(res)
	return nil
}

// cantabularServerIsHealthy generates a mocked healthy response for cantabular server
func (c *Component) cantabularServerIsHealthy() error {
	const res = `{"status": "OK"}`
	c.CantabularSrv.NewHandler().
		Get("/v9/datasets").
		Reply(http.StatusOK).
		BodyString(res)
	return nil
}

// cantabularApiExtensionIsHealthy generates a mocked healthy response for cantabular api extension
func (c *Component) cantabularApiExtensionIsHealthy() error {
	const res = `{"status": "OK"}`
	c.CantabularAPIExt.NewHandler().
		Get("/graphql?query={}").
		Reply(http.StatusOK).
		BodyString(res)
	return nil
}

// theFollowingInstanceIsAvailable generate a mocked response for dataset API
// GET /instances/{id} with the provided instance response
func (c *Component) theFollowingInstanceIsAvailable(id string, instance *godog.DocString) error {
	c.DatasetAPI.NewHandler().
		Get("/instances/"+id).
		Reply(http.StatusOK).
		BodyString(instance.Content).
		AddHeader("Etag", c.testETag)

	return nil
}

// theFollowingVersionIsUpdated generate a mocked response for dataset API
// PUT /datasets/{dataset_id}/editions/{edition}/versions/{version}
func (c *Component) theFollowingVersionIsUpdated(datasetID, edition, version string) error {
	c.DatasetAPI.NewHandler().
		Put("/datasets/" + datasetID + "/editions/" + edition + "/versions/" + version).
		Reply(http.StatusOK)

	return nil
}

// theFollowingFilterDimensionsExist mocks filter api response for
// GET /filters/{filter_id}/dimensions
func (c *Component) theFollowingFilterDimensionsExist(filterID string, instance *godog.DocString) error {
	uri := fmt.Sprintf("/filters/%s/dimensions", filterID)
	c.FilterAPI.NewHandler().
		Get(uri).
		Reply(http.StatusOK).
		BodyString(instance.Content)

	return nil
}

// theFollowingJobStateIsReturned mocks filter api response for
// GET /filters/{filter_id}
func (c *Component) theFollowingJobStateIsReturned(filterID string, instance *godog.DocString) error {
	uri := fmt.Sprintf("/filters/%s", filterID)
	c.FilterAPI.NewHandler().
		Get(uri).
		Reply(http.StatusOK).
		BodyString(instance.Content)

	return nil
}

// theFollowingQueryResponseIsAvailable generates a mocked response for Cantabular Server
// POST /graphql?query with the provided query
func (c *Component) theFollowingQueryResponseIsAvailable(name string, cb *godog.DocString) error {
	const urlQuery = `{
		dataset(name: "Example") {
		 table(variables: ["city", "siblings"]) {
		  dimensions {
		   count
		   variable {
			name
			label
		   }
		   categories {
			code
			label
		   }
		  }
		  values
		  error
		 }
		}
	   }`

	c.CantabularAPIExt.NewHandler().
		Post("/graphql?query=" + urlQuery).
		Reply(http.StatusOK).
		BodyString(cb.Content)

	return nil
}

// theseCsvCreatedEventsAreProduced consumes kafka messages that are expected to be produced by the service under test
// and validates that they match the expected values in the test
func (c *Component) theseCsvCreatedEventsAreProduced(events *godog.Table) error {
	expected, err := assistdog.NewDefault().CreateSlice(new(event.CSVCreated), events)
	if err != nil {
		return fmt.Errorf("failed to create slice from godog table: %w", err)
	}

	var got []*event.CSVCreated
	listen := true

	for listen {
		select {
		case <-time.After(c.waitEventTimeout):
			listen = false
		case <-c.consumer.Channels().Closer:
			return errors.New("closer channel closed")
		case msg, ok := <-c.consumer.Channels().Upstream:
			if !ok {
				return errors.New("upstream channel closed")
			}

			var e event.CSVCreated
			var s = schema.CSVCreated

			if err := s.Unmarshal(msg.GetData(), &e); err != nil {
				msg.Commit()
				msg.Release()
				return fmt.Errorf("error unmarshalling message: %w", err)
			}

			msg.Commit()
			msg.Release()

			got = append(got, &e)
		}
	}

	if diff := cmp.Diff(got, expected); diff != "" {
		return fmt.Errorf("-got +expected)\n%s\n", diff)
	}

	return nil
}

// noCsvCreatedEventsAreProduced listens to the kafka topic that is produced by the service under test
// and validates that nothing is received, within a time window of duration waitEventTimeout
func (c *Component) noCsvCreatedEventsAreProduced() error {
	select {
	case <-time.After(c.waitEventTimeout):
		return nil
	case <-c.consumer.Channels().Closer:
		return errors.New("closer channel closed")
	case msg, ok := <-c.consumer.Channels().Upstream:
		if !ok {
			return errors.New("upstream channel closed")
		}

		var e event.CSVCreated
		var s = schema.CSVCreated

		if err := s.Unmarshal(msg.GetData(), &e); err != nil {
			msg.Commit()
			msg.Release()
			return fmt.Errorf("error unmarshalling message: %w", err)
		}

		msg.Commit()
		msg.Release()

		return fmt.Errorf("kafka event received in csv-created topic: %v", e)
	}
}

// thisExportStartEventIsQueued produces a new ExportStart event with the contents defined by the input
func (c *Component) thisExportStartEventIsQueued(input *godog.DocString) error {
	var testEvent event.ExportStart
	if err := json.Unmarshal([]byte(input.Content), &testEvent); err != nil {
		return fmt.Errorf("error unmarshaling input to event: %w body: %s", err, input.Content)
	}

	log.Info(c.ctx, "event to send for testing: ", log.Data{
		"event": testEvent,
	})

	if err := c.producer.Send(schema.ExportStart, testEvent); err != nil {
		return fmt.Errorf("failed to send event for testing: %w", err)
	}
	return nil
}

// theFollowingPublicFileCanBeSeenInMinio checks that the provided fileName is available in the public bucket.
// If it is not available it keeps checking following an exponential backoff up to MinioCheckRetries times.
func (c *Component) theFollowingPublicFileCanBeSeenInMinio(fileName string) error {
	return c.theFollowingFileCanBeSeenInMinio(fileName, c.cfg.PublicUploadBucketName)
}

// theFollowingPublicFilteredFileCanBeSeenInMinio checks that the provided fileName is available in the public bucket.
// If it is not available it keeps checking following an exponential backoff up to MinioCheckRetries times.
func (c *Component) theFollowingPublicFilteredFileCanBeSeenInMinio(fileName string) error {
	return c.theFollowingFilteredFileCanBeSeenInMinio(fileName, c.cfg.PublicUploadBucketName)
}

// theFollowingPrivateFileCanBeSeenInMinio checks that the provided fileName is available in the private bucket.
// If it is not available it keeps checking following an exponential backoff up to MinioCheckRetries times.
func (c *Component) theFollowingPrivateFileCanBeSeenInMinio(fileName string) error {
	return c.theFollowingFileCanBeSeenInMinio(fileName, c.cfg.PrivateUploadBucketName)
}

// theFollowingFileCanBeSeenInMinio checks that the provided fileName is available in the provided bucket.
// If it is not available it keeps checking following an exponential backoff up to MinioCheckRetries times.
func (c *Component) theFollowingFileCanBeSeenInMinio(fileName, bucketName string) error {
	var b []byte
	f := aws.NewWriteAtBuffer(b)

	// probe bucket with backoff to give time for event to be processed
	retries := MinioCheckRetries
	timeout := time.Second
	var numBytes int64
	var err error

	for {
		if numBytes, err = c.S3Downloader.Download(f, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(fileName),
		}); err == nil || retries <= 0 {
			break
		}

		retries--

		log.Info(c.ctx, "error obtaining file from minio. Retrying.", log.Data{
			"error":        err,
			"retries_left": retries,
		})

		time.Sleep(timeout)
		timeout *= 2
	}
	if err != nil {
		return fmt.Errorf(
			"error obtaining file from minio. Last error: %w",
			err,
		)
	}

	if numBytes < 1 {
		return errors.New("file length zero")
	}

	log.Info(c.ctx, "got file contents", log.Data{
		"contents": string(f.Bytes()),
	})

	return nil
}

func (c *Component) theFollowingFilteredFileCanBeSeenInMinio(fileName string, bucketName string) error {
	var exists bool

	listObjectOutput, err := c.s3Client.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	})

	if err != nil {
		return fmt.Errorf(
			"error obtaining list of files from minio. Last error: %w",
			err,
		)
	}

	log.Info(c.ctx, "filename to match", log.Data{
		"filename": fileName,
	})

	for _, content := range listObjectOutput.Contents {
		log.Info(c.ctx, "file in minio", log.Data{
			"filename": fileName,
		})
		if strings.Contains(*content.Key, fileName) {
			exists = true
		}
	}

	if !exists {
		return fmt.Errorf(
			"file with '%s' does not exist in minio. Last error: %w", fileName, err)
	}

	return nil
}
