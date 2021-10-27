package steps

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ONSdigital/dp-cantabular-csv-exporter/config"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/service"
	componenttest "github.com/ONSdigital/dp-component-test"
	kafka "github.com/ONSdigital/dp-kafka/v3"
	"github.com/ONSdigital/log.go/v2/log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/maxcnunes/httpfake"
)

var (
	BuildTime string = "1625046891"
	GitCommit string = "7434fe334d9f51b7239f978094ea29d10ac33b16"
	Version   string = ""
)

type Component struct {
	componenttest.ErrorFeature
	apiFeature       *componenttest.APIFeature
	DatasetAPI       *httpfake.HTTPFake
	CantabularSrv    *httpfake.HTTPFake
	CantabularAPIExt *httpfake.HTTPFake
	S3Downloader     *s3manager.Downloader
	producer         kafka.IProducer
	consumer         kafka.IConsumerGroup
	errorChan        chan error
	svc              *service.Service
	cfg              *config.Config
	wg               *sync.WaitGroup
	signals          chan os.Signal
	waitEventTimeout time.Duration
	testETag         string
}

func NewComponent() *Component {
	return &Component{
		errorChan:        make(chan error),
		DatasetAPI:       httpfake.New(),
		CantabularSrv:    httpfake.New(),
		CantabularAPIExt: httpfake.New(),
		wg:               &sync.WaitGroup{},
		waitEventTimeout: time.Second * 5,
		testETag:         "13c7791bafdbaaf5e6660754feb1a58cd6aaa892",
	}
}

// initService initialises the server, the mocks and waits for the dependencies to be ready
func (c *Component) initService(ctx context.Context) error {
	// register interrupt signals
	c.signals = make(chan os.Signal, 1)
	signal.Notify(c.signals, os.Interrupt, syscall.SIGTERM)

	// Read config
	cfg, err := config.Get()
	if err != nil {
		return fmt.Errorf("failed to get config: %w", err)
	}

	log.Info(ctx, "config read", log.Data{"cfg": cfg})

	cfg.EncryptionDisabled = true
	cfg.StopConsumingOnUnhealthy = false
	cfg.DatasetAPIURL = c.DatasetAPI.ResolveURL("")
	cfg.CantabularURL = c.CantabularSrv.ResolveURL("")
	cfg.CantabularExtURL = c.CantabularAPIExt.ResolveURL("")

	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(cfg.MinioAccessKey, cfg.MinioSecretKey, ""),
		Endpoint:         aws.String(cfg.LocalObjectStore),
		Region:           aws.String(cfg.AWSRegion),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}

	s := session.New(s3Config)
	c.S3Downloader = s3manager.NewDownloader(s)

	// producer for triggering test events
	if c.producer, err = kafka.NewProducer(
		ctx,
		&kafka.ProducerConfig{
			BrokerAddrs:     cfg.KafkaConfig.Addr,
			Topic:           cfg.KafkaConfig.InstanceCompleteTopic,
			KafkaVersion:    &cfg.KafkaConfig.Version,
			MaxMessageBytes: &cfg.KafkaConfig.MaxBytes,
		},
	); err != nil {
		return fmt.Errorf("error creating kafka producer: %w", err)
	}

	// consumer for receiving csv-created events
	// (expected to be generated by the service under test)
	// use kafkaOldest to make sure we consume all the messages
	kafkaOffset := kafka.OffsetOldest
	if c.consumer, err = kafka.NewConsumerGroup(
		ctx,
		&kafka.ConsumerGroupConfig{
			BrokerAddrs:  cfg.KafkaConfig.Addr,
			Topic:        cfg.KafkaConfig.CsvCreatedTopic,
			GroupName:    "category-dimension-import-group",
			KafkaVersion: &cfg.KafkaConfig.Version,
			Offset:       &kafkaOffset,
		},
	); err != nil {
		return fmt.Errorf("error creating kafka consumer: %w", err)
	}

	// start consumer group
	c.consumer.Start()

	// start kafka logging go-routines
	c.producer.LogErrors(ctx)
	c.consumer.LogErrors(ctx)

	service.GetGenerator = func() service.Generator {
		return &generator{}
	}

	// wait for producer and consumer to be initialised
	<-c.producer.Channels().Initialised
	log.Info(ctx, "component-test kafka producer initialised")
	<-c.consumer.Channels().Initialised
	log.Info(ctx, "component-test kafka consumer initialised")

	// Create service and initialise it
	c.svc = service.New()
	if err = c.svc.Init(context.Background(), cfg, BuildTime, GitCommit, Version); err != nil {
		return fmt.Errorf("unexpected service Init error in NewComponent: %w", err)
	}

	c.cfg = cfg

	// Wait 500ms to make sure that the session has been established and the producer can actually send messages
	// TODO - this should be fixed in dp-kafka!
	time.Sleep(500 * time.Millisecond)

	return nil
}

func (c *Component) startService(ctx context.Context) {
	defer c.wg.Done()
	c.svc.Start(context.Background(), c.errorChan)

	// blocks until an os interrupt or a fatal error occurs
	select {
	case err := <-c.errorChan:
		err = fmt.Errorf("service error received: %w", err)
		c.svc.Close(ctx)
		panic(fmt.Errorf("unexpected error received from errorChan: %w", err))
	case sig := <-c.signals:
		log.Info(ctx, "os signal received", log.Data{"signal": sig})
	}
	if err := c.svc.Close(ctx); err != nil {
		panic(fmt.Errorf("unexpected error during service graceful shutdown: %w", err))
	}
}

// drainTopic drains the topic of any residual messages between scenarios.
// Prevents future tests failing if previous tests fail unexpectedly and
// leave messages in the queue.
func (c *Component) drainTopic(ctx context.Context) error {
	var msgs []interface{}

	defer func() {
		log.Info(ctx, "drained topic", log.Data{
			"len":      len(msgs),
			"messages": msgs,
		})
	}()

	for {
		select {
		case <-time.After(time.Second * 1):
			return nil
		case msg, ok := <-c.consumer.Channels().Upstream:
			if !ok {
				return errors.New("upstream channel closed")
			}

			msgs = append(msgs, msg)
			msg.Commit()
			msg.Release()
		}
	}
}

// Close kills the application under test, and then it shuts down the testing consumer and producer.
func (c *Component) Close() {
	ctx := context.Background()

	// stop listening to consumer, waiting for any in-flight message to be committed
	c.consumer.StopAndWait()

	// drain topic so that next test case starts from a known state
	if err := c.drainTopic(ctx); err != nil {
		log.Error(ctx, "error draining topic", err)
	}

	// close producer
	if err := c.producer.Close(ctx); err != nil {
		log.Error(ctx, "error closing kafka producer", err)
	}

	// close consumer
	if err := c.consumer.Close(ctx); err != nil {
		log.Error(ctx, "error closing kafka consumer", err)
	}

	// kill application
	c.signals <- os.Interrupt

	// wait for graceful shutdown to finish (or timeout)
	c.wg.Wait()
}

// Reset initialises the service under test, the api mocks and then starts the service
func (c *Component) Reset() error {
	ctx := context.Background()

	if err := c.initService(ctx); err != nil {
		return fmt.Errorf("failed to initialise service: %w", err)
	}

	c.DatasetAPI.Reset()
	c.CantabularSrv.Reset()
	c.CantabularAPIExt.Reset()

	// run application in separate goroutine
	c.wg.Add(1)
	go c.startService(ctx)

	return nil
}
