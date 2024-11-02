# Kafka to BigQuery Streaming Pipeline
This project contains a Spark application designed to run on a Google Dataproc cluster. It reads streaming data from Kafka, processes it using Spark Structured Streaming, and writes it to a Google BigQuery table in real-time.

# Overview
This pipeline enables real-time data ingestion from Kafka and stores processed data in BigQuery, providing a scalable solution for handling streaming data on Google Cloud.

# Prerequisites

To successfully deploy and run this pipeline, ensure the following prerequisites are met:

1. Google Cloud Project
A Google Cloud project with billing enabled.
Necessary permissions to create resources in BigQuery and Google Cloud Storage.

2. Google Dataproc Cluster
A Dataproc cluster with Spark installed.
Configure the cluster to include the following Spark packages:
org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1
org.apache.kafka:kafka-clients:3.8.0

3. Kafka Setup
A running Kafka cluster with a configured topic for streaming data.
Kafka topic and bootstrap server details.

4. BigQuery and Google Cloud Storage
A BigQuery dataset and table (projectid.dataset.tablename) to store processed data.
A Google Cloud Storage bucket for temporary data staging during BigQuery writes.

5. Google Cloud SDK
Ensure gcloud CLI is installed and authenticated with the necessary permissions.

6. Python Dependencies
pyspark installed in your environment (if running locally for testing).

# Pipeline Workflow
1. Spark Session Initialization
Configures Spark with the necessary dependencies for both Kafka and BigQuery integration. This session runs under the team-plutus Google Cloud project.
2. Kafka Configuration
The application connects to a specified Kafka server and subscribes to a topic:

kafka.bootstrap.servers: Kafka server address
subscribe: Kafka topic
auto.offset.reset: Manages data offset (set to earliest for initial load)

3. Message Schema
Defines a schema for Kafka messages in JSON format with fields like:

hashed_device_id
timezone_visit
day_of_week_visit
lat_visit
lon_visit
timestamp fields

4. Data Processing

Reads Kafka messages and parses them according to the defined schema
Extracts individual fields for transformation and storage

5. BigQuery Configuration

Specifies the BigQuery table (projectid.dataset.tablename) as the target
Uses Google Cloud Storage as a temporary bucket for staging data

6. Writing to BigQuery

Defines the write_to_bigquery function to handle each micro-batch, appending data to the BigQuery table
Sets a trigger interval of 10 seconds for consistent data streaming

7. Stream Execution

The query runs continuously until terminated, ensuring a steady data flow from Kafka to BigQuery

# Usage

Deploy this application to a Dataproc Spark cluster
Set up Kafka with the appropriate topic and configurations
Ensure Google Cloud Storage and BigQuery tables are configured as specified

This setup provides a resilient, scalable streaming data pipeline that leverages Google Cloud's capabilities for real-time analytics.


# Bigquery table schema

[
  {
    "name": "hashed_device_id",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "timezone_visit",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "day_of_week_visit",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "time_stamp",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "lat_visit",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "data_visit",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "time_visit",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "lon_visit",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  }
]