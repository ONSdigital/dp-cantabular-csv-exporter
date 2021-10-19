package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ONSdigital/dp-cantabular-csv-exporter/config"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/event"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/schema"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/log.go/v2/log"
)

const serviceName = "dp-cantabular-csv-exporter"

func main() {
	log.Namespace = serviceName
	ctx := context.Background()

	// Get Config
	cfg, err := config.Get()
	if err != nil {
		log.Fatal(ctx, "error getting config", err)
		os.Exit(1)
	}

	// Create Kafka Producer
	pChannels := kafka.CreateProducerChannels()
	kafkaProducer, err := kafka.NewProducer(ctx, cfg.KafkaConfig.Addr, cfg.KafkaConfig.InstanceCompleteTopic, pChannels, &kafka.ProducerConfig{
		KafkaVersion: &cfg.KafkaConfig.Version,
	})
	if err != nil {
		log.Fatal(ctx, "fatal error trying to create kafka producer", err, log.Data{"topic": cfg.KafkaConfig.InstanceCompleteTopic})
		os.Exit(1)
	}

	// kafka error logging go-routines
	kafkaProducer.Channels().LogErrors(ctx, "kafka producer")

	time.Sleep(500 * time.Millisecond)
	scanner := bufio.NewScanner(os.Stdin)
	for {
		e := scanEvent(scanner)
		log.Info(ctx, "sending hello-called event", log.Data{"helloCalledEvent": e})

		bytes, err := schema.InstanceComplete.Marshal(e)
		if err != nil {
			log.Fatal(ctx, "hello-called event error", err)
			os.Exit(1)
		}

		// Send bytes to Output channel, after calling Initialise just in case it is not initialised.
		// Wait for producer to be initialised
		<-kafkaProducer.Channels().Ready
		kafkaProducer.Channels().Output <- bytes
	}

}

// scanEvent creates a HelloCalled event according to the user input
func scanEvent(scanner *bufio.Scanner) *event.InstanceComplete {
	fmt.Println("--- [Send Kafka InstanceComplete] ---")

	fmt.Println("Please type the instance_id")
	fmt.Printf("$ ")
	scanner.Scan()
	name := scanner.Text()

	fmt.Println("Please type the cantabular_blob")
	fmt.Printf("$ ")
	scanner.Scan()
	blob := scanner.Text()

	return &event.InstanceComplete{
		InstanceID:     name,
		CantabularBlob: blob,
	}
}
