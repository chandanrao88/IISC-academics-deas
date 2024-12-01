# Mobility Data Kafka Publisher Cloud Function

This repository contains a Google Cloud Function that generates synthetic mobility footfall data and sends it to a Kafka topic. 
The provided code defines a cloud function to generate and push sample mobility footfall data to Kafka. It starts by importing necessary libraries like pandas, random, datetime, and kafka.

The core functionality revolves around generating mock mobility data, including device IDs, visit times, locations (latitude and longitude), and associated timestamps. The generate_sample_data function creates a DataFrame with randomly generated values for these fields, simulating visitor data for a specified location and a given number of records.

The create_kafka_producer function sets up a Kafka producer configured to send data to a Kafka server located at a specified address. The push_data_to_kafka function iterates over the DataFrame and sends each row as a message to a Kafka topic ("visit-data-topic").

The location_data_gen function is an HTTP-triggered cloud function that processes incoming requests. It expects JSON input containing latitude, longitude, and the number of records to generate. It validates the input, generates the sample data, and pushes it to Kafka. If any errors occur (e.g., missing or invalid parameters), appropriate error messages are returned.

Overall, the code simulates location-based visit data and sends it to a Kafka topic for further processing or analysis.

# Overview

## The Cloud Function:

Generates random sample records representing footfall data, including fields like latitude, longitude, and visit time.

Connects to a Kafka broker and pushes the generated records to a specified Kafka topic.

Can be configured with different numbers of records and Kafka topics through HTTP request parameters.

# Prerequisites

To deploy and run this Cloud Function, ensure you have:

### Google Cloud SDK: 
Install and configure the Google Cloud SDK to manage your Google Cloud resources.

### Kafka Broker: 
A running Kafka server that the Cloud Function can access. Modify bootstrap_servers with the correct Kafka server address.

### Python Libraries:
Install required Python packages:
 - pip install pandas kafka-python functions-framework

### Google Cloud Project: 
A Google Cloud project with permissions to deploy Cloud Functions.

# Code Explanation

## Main Components

### Data Generation:
The location_data_gen function creates a DataFrame with synthetic footfall data, including:
- device_id: Simulated device identifiers.
- timezone_visit
- day_of_week_visit
- time_stamp
- lat_visit
- lon_visit
- data_visit
- time_visit

Various fields representing the location, timestamp, and visit details for each record.
By default, it generates 100 records, though this can be adjusted.

Sample HTTPS calls to generate random test records example :- 

````
curl -m 70 -X POST https://us-east1-team-plutus-iisc.cloudfunctions.net/location-data-gen \
-H "Authorization: bearer $(gcloud auth print-identity-token)" \
-H "Content-Type: application/json" \
-d '{
  "lat": "53.5500",
  "lon": "-2.4333",
  "num_records": "500"
}'
````

### Kafka Producer Setup:
create_kafka_producer initializes a Kafka producer with JSON serialization to communicate with the Kafka server.

### Pushing Data to Kafka:
The push_data_to_kafka function iterates over each row in the DataFrame, converting it to a dictionary and sending it as a message to the specified Kafka topic.

### Cloud Function HTTP Trigger:
The location_data_gen function is the entry point for this Cloud Function. It accepts an HTTP request with two optional parameters:

### topic: 
Kafka topic to which the data will be sent (default is 'visit-data-topic').

## Code Steps

### Generate Data:
The function generates synthetic mobility data using location_data_gen.

### Create Kafka Producer:
A Kafka producer is created with create_kafka_producer to send data to the Kafka topic.

### Push Data to Kafka:
Each row in the generated data is sent to Kafka using push_data_to_kafka.

### Return Status:
A success message is returned upon completing the data push.