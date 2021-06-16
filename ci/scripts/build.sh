#!/bin/bash -eux

pushd dp-cantabular-csv-exporter
  make build
  cp build/dp-cantabular-csv-exporter Dockerfile.concourse ../build
popd
