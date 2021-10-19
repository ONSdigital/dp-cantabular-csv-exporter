module github.com/ONSdigital/dp-cantabular-csv-exporter

go 1.16

replace github.com/coreos/etcd => github.com/coreos/etcd v3.3.24+incompatible

require (
	github.com/ONSdigital/dp-api-clients-go/v2 v2.1.10
	github.com/ONSdigital/dp-component-test v0.5.0
	github.com/ONSdigital/dp-healthcheck v1.1.0
	github.com/ONSdigital/dp-kafka/v2 v2.4.2
	github.com/ONSdigital/dp-net v1.0.12
	github.com/ONSdigital/dp-s3 v1.7.0
	github.com/ONSdigital/dp-vault v1.2.0
	github.com/ONSdigital/log.go/v2 v2.0.7
	github.com/aws/aws-sdk-go v1.40.24
	github.com/cucumber/godog v0.11.0
	github.com/google/go-cmp v0.5.5
	github.com/gorilla/mux v1.8.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/maxcnunes/httpfake v1.2.1
	github.com/rdumont/assistdog v0.0.0-20201106100018-168b06230d14
	github.com/smartystreets/goconvey v1.6.4
)
