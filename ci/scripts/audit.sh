#!/bin/bash -eux

export cwd=$(pwd)

pushd $cwd/dp-cantabular-csv-exporter
  make audit
popd