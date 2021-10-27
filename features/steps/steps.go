package steps

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/ONSdigital/dp-cantabular-csv-exporter/event"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/schema"
	kafka "github.com/ONSdigital/dp-kafka/v3"
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
	ctx.Step(`^dp-dataset-api is healthy`, c.datasetAPIIsHealthy)
	ctx.Step(`^the following instance with id "([^"]*)" is available from dp-dataset-api:$`, c.theFollowingInstanceIsAvailable)
	ctx.Step(`^an instance with id "([^"]*)" is updated to dp-dataset-api`, c.theFollowingInstanceIsUpdated)
	ctx.Step(`^this instance-complete event is consumed:$`, c.thisInstanceCompleteEventIsConsumed)
	ctx.Step(`^these cantabular-csv-created events are produced:$`, c.theseCsvCreatedEventsAreProduced)
	ctx.Step(`^a file with filename "([^"]*)" can be seen in minio`, c.theFollowingFileCanBeSeenInMinio)
}

func (c *Component) datasetAPIIsHealthy() error {
	c.DatasetAPI.NewHandler().
		Get("/health").
		Reply(http.StatusOK)
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

// theFollowingInstanceIsUpdated generate a mocked response for dataset API
// PUT /instances/{id} with the provided instance response
func (c *Component) theFollowingInstanceIsUpdated(id string) error {
	c.DatasetAPI.NewHandler().
		Put("/instances/"+id).
		Reply(http.StatusOK).
		AddHeader("Etag", c.testETag)

	return nil
}

// theFollowingQueryResposneIsAvailable generates a mocked response for Cantabular Server
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
	expected, err := assistdog.NewDefault().CreateSlice(new(event.CsvCreated), events)
	if err != nil {
		return fmt.Errorf("failed to create slice from godog table: %w", err)
	}

	var got []*event.CsvCreated
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

			var e event.CsvCreated
			var s = schema.CsvCreated

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

func (c *Component) thisInstanceCompleteEventIsConsumed(input *godog.DocString) error {
	ctx := context.Background()

	// testing kafka message that will be produced
	var testEvent event.InstanceComplete
	if err := json.Unmarshal([]byte(input.Content), &testEvent); err != nil {
		return fmt.Errorf("error unmarshaling input to event: %w body: %s", err, input.Content)
	}

	log.Info(ctx, "event to marshal: ", log.Data{
		"event": testEvent,
	})

	// marshal and send message
	b, err := schema.InstanceComplete.Marshal(testEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal event from schema: %w", err)
	}

	log.Info(ctx, "marshalled event: ", log.Data{
		"event": b,
	})

	time.Sleep(time.Second)

	c.producer.Channels().Output <- b

	// this go-routine retries to send the kafka message on error (we observe the error if the Sarama session is not established)
	// TODO this will probably need to be moved to dp-kafka/v3
	go func() {
		retries := 10
		timeout := time.Second

		for {
			select {
			case err := <-c.producer.Channels().Errors:
				retries--
				logData := kafka.UnwrapLogData(err)
				logData["retries_left"] = retries

				if retries == 0 {
					log.Error(ctx, "received error in component test kafka producer and no retries left, assuming unrecoverable error", err, logData)
					c.FailNow() // no retries left - assume error not recoverable
					return
				}

				logData["err"] = err
				log.Info(ctx, "received error in component test kafka producer. Retrying.", logData)

				time.Sleep(timeout)
				timeout *= 2
				c.producer.Channels().Output <- b
			case <-time.After(c.waitEventTimeout):
				return // assume there is no error
			}
		}
	}()

	return nil
}

func (c *Component) theFollowingFileCanBeSeenInMinio(fileName string) error {
	ctx := context.Background()

	var b []byte
	f := aws.NewWriteAtBuffer(b)

	// probe bucket with backoff to give time for event to be processed
	retries := 10
	timeout := time.Second
	var numBytes int64
	var err error

	for {
		if numBytes, err = c.S3Downloader.Download(f, &s3.GetObjectInput{
			Bucket: aws.String(c.cfg.UploadBucketName),
			Key:    aws.String(fileName),
		}); err == nil || retries <= 0 {
			break
		}

		retries--

		log.Info(ctx, "error obtaining file from minio. Retrying.", log.Data{
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

	log.Info(ctx, "got file contents", log.Data{
		"contents": string(f.Bytes()),
	})

	return nil
}
