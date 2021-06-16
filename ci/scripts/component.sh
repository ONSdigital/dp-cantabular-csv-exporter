#!/bin/bash -eux

pushd dp-cantabular-csv-exporter
  make test-component
popd
