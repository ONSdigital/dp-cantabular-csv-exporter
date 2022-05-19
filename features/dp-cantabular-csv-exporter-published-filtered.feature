Feature: Cantabular-Csv-Exporter-Published-Filtered

  # This file validates that CSV files generated for an instance in published state are stored in the public S3 bucket

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

    And filter API is healthy
    And dp-dataset-api is healthy
    And cantabular server is healthy
    And cantabular api extension is healthy

    And the following instance with id "instance-happy-02" is available from dp-dataset-api:
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
        "state": "published",
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

    And the following filter output with id "filter-output-happy-02" will be updated:
    """
    {
      "downloads":{
        "CSV":{
          "href":"http://localhost:23600/downloads/filter-outputs/filter-output-happy-02.csv",
          "size": "502",
          "public": "http://minio:9000/public-bucket/datasets/dataset-happy-02-edition-happy-02-version-happy-02-filtered-2022-01-26T12:27:04Z.csv",
          "skipped": false
        }
      }
    }
    """

    And for the following filter "filter-output-happy-02" these dimensions are available:
    """
    {
      "items": [
        {
          "name": "City",
          "options": [
            "Cardiff",
            "London",
            "Swansea"
          ],
          "dimension_url": "http://dimension.url/city",
          "is_area_type": true
        },
        {
          "name": "Number of siblings (3 mappings)",
          "options": [
            "0-3",
            "4-7",
            "7+"
          ],
          "dimension_url": "http://dimension.url/siblings",
          "is_area_type": false
        }
      ],
      "count": 2,
      "offset": 0,
      "limit": 20,
      "total_count": 2
    }
    """

    And the following filter is returned for the filter output "filter-output-happy-02":
    """
    {
      "filter_id": "filter-output-happy-02",
      "links": {
        "version": {
          "href": "http://mockhost:9999/datasets/cantabular-example-1/editions/2021/version/1",
          "id": "1"
        },
        "self": {
          "href": ":27100/filters/94310d8d-72d6-492a-bc30-27584627edb1"
        }
      },
      "events": null,
      "instance_id": "c733977d-a2ca-4596-9cb1-08a6e724858b",
      "dimension_list_url":":27100/filters/94310d8d-72d6-492a-bc30-27584627edb1/dimensions",
      "dataset": {
        "id": "cantabular-example-1",
        "edition": "2021",
        "version": 1
      },
      "published": true,
      "population_type": "Example"
    }
    """

  Scenario: Consuming a cantabular-export-start event with correct fields for a published instance with a filter output id present

    When the service starts

    And this cantabular-export-start event is queued, to be consumed:
      """
      {
        "InstanceID":     "instance-happy-02",
        "DatasetID":      "dataset-happy-02",
        "Edition":        "edition-happy-02",
        "Version":        "version-happy-02",
        "FilterOutputID": "filter-output-happy-02"
      }
      """

    Then a public filtered file, that should contain "datasets/dataset-happy-02-edition-happy-02-version-happy-02-filtered-20" on the filename can be seen in minio

    And one event with the following fields are in the produced kafka topic cantabular-csv-created:
      | InstanceID        | DatasetID        | Edition          | Version          | RowCount | Dimensions |
      | instance-happy-02 | dataset-happy-02 | edition-happy-02 | version-happy-02 | 22       | []         |