#!/bin/bash -eux

echo "=== DEBUG start of component.sh"

# Run component tests in docker compose defined in features/compose folder
pushd dp-cantabular-csv-exporter/features/compose
  echo "=== DEBUG going to run docker-compose up"
  COMPONENT_TEST_USE_LOG_FILE=true docker-compose up --abort-on-container-exit
  e=$?
  echo "=== DEBUG done running docker-compose up"
popd

# Cat the component-test output file and remove it so log output can
# be seen in Concourse
pushd dp-cantabular-csv-exporter
  echo "=== DEBUG going to run ls"
  ls
  echo "=== DEBUG going to cat and remove component-output.txt"
  cat component-output.txt && rm component-output.txt
  echo "=== DEBUG done catting and removing component-output.txt"
popd

# Show message to prevent any confusion by 'ERROR 0' outpout
echo "please ignore error codes 0, like so: ERRO[xxxx] 0, as error code 0 means that there was no error"

# exit with the same code returned by docker compose
exit $e
