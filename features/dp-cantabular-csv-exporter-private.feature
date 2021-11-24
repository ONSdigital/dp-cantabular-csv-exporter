Feature: Cantabular-Csv-Exporter-Private

  Background:
    Given the following response is available from Cantabular from the codebook "Example" using the GraphQL endpoint:
      """
      {
        "data": {
            "dataset": {
                "table": {
                    "dimensions": [
                        {
                            "categories": [
                                {
                                    "code": "0",
                                    "label": "London"
                                },
                                {
                                    "code": "1",
                                    "label": "Liverpool"
                                },
                                {
                                    "code": "2",
                                    "label": "Belfast"
                                }
                            ],
                            "count": 3,
                            "variable": {
                                "label": "City",
                                "name": "city"
                            }
                        },
                        {
                            "categories": [
                                {
                                    "code": "0",
                                    "label": "No siblings"
                                },
                                {
                                    "code": "1",
                                    "label": "1 sibling"
                                },
                                {
                                    "code": "2",
                                    "label": "2 siblings"
                                },
                                {
                                    "code": "3",
                                    "label": "3 siblings"
                                },
                                {
                                    "code": "4",
                                    "label": "4 siblings"
                                },
                                {
                                    "code": "5",
                                    "label": "5 siblings"
                                },
                                {
                                    "code": "6",
                                    "label": "6 or more siblings"
                                }
                            ],
                            "count": 7,
                            "variable": {
                                "label": "Number of siblings",
                                "name": "siblings"
                            }
                        }
                    ],
                    "error": null,
                    "values": [
                        1,
                        0,
                        0,
                        1,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        1,
                        0,
                        0,
                        0,
                        0,
                        1,
                        0,
                        0,
                        1,
                        1
                    ]
                }
            }
        }
      }
      """

    And dp-dataset-api is healthy
    And cantabular server is healthy
    And cantabular api extension is healthy

    And the following instance with id "instance-happy-01" is available from dp-dataset-api:
      """
      {
        "import_tasks": {
          "build_hierarchies": null,
          "build_search_indexes": null,
          "import_observations": {
            "total_inserted_observations": 0,
            "state": "created"
          }
        },
        "id": "057cd26b-e0ae-431f-9316-913db61cec39",
        "last_updated": "2021-07-19T09:59:28.417Z",
        "links": {
          "dataset": {
            "href": "http://localhost:22000/datasets/cantabular-dataset",
            "id": "cantabular-dataset"
          },
          "job": {
            "href": "http://localhost:21800/jobs/e7f99293-44f2-47ce-b6cb-db2f6618ef40",
            "id": "e7f99293-44f2-47ce-b6cb-db2f6618ef40"
          },
          "self": {
            "href": "http://10.201.4.160:10400/instances/057cd26b-e0ae-431f-9316-913db61cec39"
          }
        },
        "state": "associated",
        "headers": [
          "ftb_table",
          "city",
          "siblings"
        ],
        "is_based_on": {
          "@type": "cantabular_table",
          "@id": "Example"
        }
      }
      """

    Scenario: Consuming a cantabular-export-start event with correct fields

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
    Then a dataset version with dataset-id "dataset-happy-01", edition "edition-happy-01" and version "version-happy-01" is updated to dp-dataset-api

    And a private file with filename "datasets/dataset-happy-01-edition-happy-01-version-happy-01.csv" can be seen in minio

    And these cantabular-csv-created events are produced:
      | InstanceID        | DatasetID        | Edition          | Version          | RowCount |
      | instance-happy-01 | dataset-happy-01 | edition-happy-01 | version-happy-01 | 22       |
