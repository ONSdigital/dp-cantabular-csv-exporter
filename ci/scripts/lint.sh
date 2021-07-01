#!/bin/bash -eux

cwd=$(pwd)

pushd $cwd/dp-cantabular-csv-exporter
  make lint
popd