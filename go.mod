module github.com/ONSdigital/dp-cantabular-csv-exporter

go 1.16

replace (
	github.com/ONSdigital/dp-api-clients-go/v2 => /home/jpm/go-modules/onsdigital/dp-api-clients-go
	github.com/coreos/etcd => github.com/coreos/etcd v3.3.24+incompatible
)

require (
	github.com/ONSdigital/dp-api-clients-go v1.41.1 // indirect
	github.com/ONSdigital/dp-api-clients-go/v2 v2.0.2-beta
	github.com/ONSdigital/dp-component-test v0.3.1
	github.com/ONSdigital/dp-healthcheck v1.0.5
	github.com/ONSdigital/dp-kafka/v2 v2.2.1
	github.com/ONSdigital/dp-net v1.0.12
	github.com/ONSdigital/dp-s3 v1.5.1
	github.com/ONSdigital/log.go v1.0.1
	github.com/ONSdigital/log.go/v2 v2.0.5
	github.com/cucumber/godog v0.11.0
	github.com/gorilla/mux v1.8.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/rdumont/assistdog v0.0.0-20201106100018-168b06230d14
	github.com/shurcooL/graphql v0.0.0-20200928012149-18c5c3165e3a // indirect
	github.com/smartystreets/goconvey v1.6.4
	github.com/stretchr/testify v1.7.0
	golang.org/x/crypto v0.0.0-20210314154223-e6e6c4f2bb5b // indirect
)
