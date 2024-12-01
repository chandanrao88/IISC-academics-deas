# Kafka to BigQuery Streaming Pipeline
This project contains a Spark application designed to run on a Google Dataproc cluster.
It reads streaming data from a Kafka topic, processes it using Apache Spark, and applies KMeans clustering to geographical data (latitude and longitude). The process begins by setting up a Spark session and defining the schema for Kafka messages. It then parses the messages, hashes the device_id, converts timestamps into a human-readable format, and filters valid latitude and longitude values. The VectorAssembler is used to prepare the data for clustering, and the optimal number of clusters is determined using the Elbow Method. Finally, the clustered data is written to BigQuery using a micro-batch approach.

# Overview
This pipeline enables real-time data ingestion from Kafka and stores processed data in BigQuery, providing a scalable solution for handling streaming data on Google Cloud.

# Prerequisites

To successfully deploy and run this pipeline, ensure the following prerequisites are met:

## 1. Google Cloud Project
A Google Cloud project with billing enabled.
Necessary permissions to create resources in BigQuery and Google Cloud Storage.

## 2. Google Dataproc Cluster
A Dataproc cluster with Spark installed.
Configure the cluster to include the following Spark packages:
org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1
org.apache.kafka:kafka-clients:3.8.0

## 3. Kafka Setup
A running Kafka cluster with a configured topic for streaming data.
Kafka topic and bootstrap server details.

## 4. BigQuery and Google Cloud Storage
A BigQuery dataset and table (projectid.dataset.tablename) to store processed data.
A Google Cloud Storage bucket for temporary data staging during BigQuery writes.

## 5. Google Cloud SDK
Ensure gcloud CLI is installed and authenticated with the necessary permissions.

## 6. Python Dependencies
pyspark installed in your environment (if running locally for testing).

# Pipeline Workflow
## 1. Spark Session Initialization
Configures Spark with the necessary dependencies for both Kafka and BigQuery integration. This session runs under the team-plutus Google Cloud project.
## 2. Kafka Configuration
The application connects to a specified Kafka server and subscribes to a topic:

kafka.bootstrap.servers: Kafka server address
subscribe: Kafka topic
auto.offset.reset: Manages data offset (set to earliest for initial load)

## 3. Message Schema
Defines a schema for Kafka messages in JSON format with fields like:

- device_id
- timezone_visit
- day_of_week_visit
- lat_visit
- lon_visit
- timestamp

## 4. Data Processing

- Reads Kafka messages and parses them according to the defined schema.
   - Extracts individual fields for transformation and storage.
  - **Data Transformation**:
     - Masking `device_id` using SHA-256 hash and a substitution cipher.
     - Converting `time_stamp` from Unix format to datetime format.
     ````
     col("data.device_id").isNotNull(), sha2(col("data.device_id"), 256)).otherwise(None)
    
     from_unixtime(floor(col("data.time_stamp")), "yyyy-MM-dd HH:mm:ss")
     ````

## 5. BigQuery Configuration

Specifies the BigQuery table (projectid.dataset.tablename) as the target
Uses Google Cloud Storage as a temporary bucket for staging data

## 6. Spark ML

Preprocess geographical data (latitude and longitude) by applying KMeans clustering. It dynamically determines the optimal number of clusters using the Elbow Method and stores the results, including device information and visit date, in a BigQuery table for further analysis.

## 1. Vectorization for Clustering:
- The VectorAssembler is used to combine the lat_visit and lon_visit columns into a single feature vector for each record. This is required for clustering algorithms like KMeans, which work on numerical features in vector form.
`````
vector_assembler = VectorAssembler(inputCols=["lat_visit", "lon_visit"], outputCol="features")
vectorized_df = vector_assembler.transform(parsed_df)
`````
The VectorAssembler takes the lat_visit and lon_visit columns as inputs and combines them into a new column features which will be used as the input for the KMeans algorithm.

## 2. Finding the Optimal Number of Clusters (Elbow Method):
- The find_optimal_k function uses the Elbow Method to determine the optimal number of clusters for KMeans. This method evaluates different values of k (number of clusters) and selects the value that minimizes the cost (within-cluster sum of squared errors)

-The function iterates over different values of k (from 2 to max_k) and computes the clustering cost for each value. The optimal number of clusters is the one with the lowest cost, which is then used in the KMeans clustering.

## 3. Applying KMeans on Each Batch:
- The apply_kmeans_on_batch function performs clustering on each batch of data, determining the optimal number of clusters and applying the KMeans algorithm. After clustering, the results are transformed and relevant columns are selected for storage in BigQuery.
- The function first determines the optimal number of clusters (optimal_k) for the batch.
- Then, it applies KMeans clustering to the batch_df using the determined k.
- After clustering, the function selects the relevant columns (device_id, date_visit, lat_visit, lon_visit, and cluster_pred), which include the predicted cluster for each record.
- Finally, the data is written to BigQuery using the write_to_bigquery function.

## 7. Writing to BigQuery

Defines the write_to_bigquery function to handle each micro-batch, appending data to the BigQuery table
Sets a trigger interval of 10 seconds for consistent data streaming

## 8. Stream Execution

The query runs continuously until terminated, ensuring a steady data flow from Kafka to BigQuery

# Usage

Deploy this application to a Dataproc Spark cluster
Set up Kafka with the appropriate topic and configurations
Ensure Google Cloud Storage and BigQuery tables are configured as specified

This setup provides a resilient, scalable streaming data pipeline that leverages Google Cloud's capabilities for real-time analytics.


# Bigquery table schema
```yaml

[
  {
    "name": "device_id",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "Hashed DeviceId",
    "fields": []
  },
  {
    "name": "lat_visit",
    "mode": "NULLABLE",
    "type": "FLOAT",
    "description": "Latitide",
    "fields": []
  },
  {
    "name": "lon_visit",
    "mode": "NULLABLE",
    "type": "FLOAT",
    "description": "longitude",
    "fields": []
  },
  {
    "name": "date_visit",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "date time",
    "fields": []
  },
  {
    "name": "cluster_pred",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "clustring predection after the ML model execution",
    "fields": []
  }
]