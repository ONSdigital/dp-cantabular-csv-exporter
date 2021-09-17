dp-cantabular-csv-exporter
================
Consumes a Kafka message (cantabular-dataset-instance-complete) to begin the process of retrieving the counts from Cantabular.

### Getting started

#### Dependencies

Ensure you have vault running.

`brew install vault`
`vault server -dev`

* Setup AWS credentials. The app uses the default provider chain. When running locally this typically means they are provided by the `~/.aws/credentials` file.  Alternatively you can inject the credentials via environment variables as described in the configuration section.

#### Run the service

* Run `make debug`

The service runs in the background consuming messages from Kafka.
An example event can be created using the helper script, `make produce`.

### Dependencies

* Requires running…
  * [kafka](https://github.com/ONSdigital/dp/blob/main/guides/INSTALLING.md#prerequisites)
* No further dependencies other than those defined in `go.mod`

### Configuration

| Environment variable         | Default                              | Description
| ---------------------------- | ------------------------------------ | -----------
| BIND_ADDR                    | localhost:26300                      | The host and port to bind to
| GRACEFUL_SHUTDOWN_TIMEOUT    | 5s                                   | The graceful shutdown timeout in seconds (`time.Duration` format)
| HEALTHCHECK_INTERVAL         | 30s                                  | Time between self-healthchecks (`time.Duration` format)
| HEALTHCHECK_CRITICAL_TIMEOUT | 90s                                  | Time to wait until an unhealthy dependent propagates its state to make this app unhealthy (`time.Duration` format)
| KAFKA_ADDR                   | "localhost:9092"                     | The address of Kafka (accepts list)
| KAFKA_VERSION                | 1.0.2                                | Kafka version running in the environment
| KAFKA_OFFSET_OLDEST          | true                                 | Start processing Kafka messages in order from the oldest in the queue
| KAFKA_NUM_WORKERS            | 1                                    | The maximum number of parallel kafka consumers
| INSTANCE_COMPLETE_GROUP      | dp-cantabular-csv-exporter           | The consumer group this application to consume ImageUploaded messages
| INSTANCE_COMPLETE_TOPIC      | cantabular-dataset-instance-complete | The name of the topic to consume messages from
| SERVICE_AUTH_TOKEN           |                                      | The service token for this app
| CANTABULAR_URL               | localhost:8491                       | The Cantabular server URL
| DATASET_API_URL              | localhost:22000                      | The Dataset API URL
| AWS_REGION                   | eu-west-1                            | The AWS region to use
| UPLOAD_BUCKET_NAME           | dp-cantabular-csv-exporter           | The name of the S3 bucket to store csv files
| VAULT_ADDR                   | http://localhost:8200                | The address of vault
| VAULT_TOKEN                  | -                                    | Use `make debug` to set a vault token
| VAULT_PATH                   | secret/shared/psk                    | The vault path to store psks


### Healthcheck

 The `/health` endpoint returns the current status of the service. Dependent services are health checked on an interval defined by the `HEALTHCHECK_INTERVAL` environment variable.

 On a development machine a request to the health check endpoint can be made by:

 `curl localhost:8125/health`

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright © 2021, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details

