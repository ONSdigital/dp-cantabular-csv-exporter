package steps

import (
	"context"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ONSdigital/dp-cantabular-csv-exporter/event"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/schema"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/service"
	"github.com/ONSdigital/dp-kafka/v2/kafkatest"
	"github.com/cucumber/godog"
	"github.com/rdumont/assistdog"
	"github.com/stretchr/testify/assert"
)

func (c *Component) RegisterSteps(ctx *godog.ScenarioContext) {
}

func (c *Component) iShouldReceiveAHelloworldResponse() error {
	content, err := ioutil.ReadFile(c.cfg.OutputFilePath)
	if err != nil {
		return err
	}

	assert.Equal(c, "Hello, Tim!", string(content))

	return c.StepError()
}

func (c *Component) theseHelloEventsAreConsumed(table *godog.Table) error {

	observationEvents, err := c.convertToHelloEvents(table)
	if err != nil {
		return err
	}

	signals := registerInterrupt()

	// run application in separate goroutine
	go func() {
		c.svc, err = service.Run(context.Background(), c.serviceList, "", "", "", c.errorChan)
	}()

	// consume extracted observations
	for _, e := range observationEvents {
		if err := c.sendToConsumer(e); err != nil {
			return err
		}
	}

	time.Sleep(300 * time.Millisecond)

	// kill application
	signals <- os.Interrupt

	return nil
}

func (c *Component) convertToHelloEvents(table *godog.Table) ([]*event.InstanceComplete, error) {
	assist := assistdog.NewDefault()
	events, err := assist.CreateSlice(&event.InstanceComplete{}, table)
	if err != nil {
		return nil, err
	}
	return events.([]*event.InstanceComplete), nil
}

func (c *Component) sendToConsumer(e *event.InstanceComplete) error {
	bytes, err := schema.InstanceCompleteEvent.Marshal(e)
	if err != nil {
		return err
	}

	c.KafkaConsumer.Channels().Upstream <- kafkatest.NewMessage(bytes, 0)
	return nil

}

func registerInterrupt() chan os.Signal {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	return signals
}
