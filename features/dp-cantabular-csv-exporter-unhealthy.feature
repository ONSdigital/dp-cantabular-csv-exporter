Feature: Cantabular-Csv-Exporter-Unhealthy

  Background:
    Given dp-dataset-api is unhealthy
    And cantabular server is healthy
    And cantabular api extension is healthy

    Scenario: Not consuming cantabular-export-start events

    When the service starts
    
    And this cantabular-export-start event is queued:
      """
      {
        "InstanceID": "instance-happy-01",
        "DatasetID":  "dataset-happy-01",
        "Edition":    "edition-happy-01",
	      "Version":    "version-happy-01"
      }
      """  

    Then no cantabular-csv-created events are produced
