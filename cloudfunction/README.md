# Mobility Data Kafka Publisher Cloud Function

This repository contains a Google Cloud Function that generates synthetic mobility footfall data for Bangalore and sends it to a Kafka topic. The function simulates location and visit data for a specified number of devices and publishes this data in JSON format to Kafka for downstream processing or analytics.

# Overview

## The Cloud Function:

Generates random sample records representing footfall data in Bangalore, including fields like latitude, longitude, and visit time.

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
The generate_bangalore_sample_data function creates a DataFrame with synthetic footfall data, including:
hashed_device_id: Simulated device identifiers.
timezone_visit, day_of_week_visit, time_stamp, lat_visit, lon_visit, data_visit, and time_visit: Various fields representing the location, timestamp, and visit details for each record.
By default, it generates 100 records, though this can be adjusted.

### Kafka Producer Setup:
create_kafka_producer initializes a Kafka producer with JSON serialization to communicate with the Kafka server.

### Pushing Data to Kafka:
The push_data_to_kafka function iterates over each row in the DataFrame, converting it to a dictionary and sending it as a message to the specified Kafka topic.

### Cloud Function HTTP Trigger:
The send_bangalore_data_to_kafka function is the entry point for this Cloud Function. It accepts an HTTP request with two optional parameters:

### num_records: 
Number of data records to generate (default is 100).

### topic: 
Kafka topic to which the data will be sent (default is 'visit-data-topic').

## Code Steps

### Generate Data:
The function generates synthetic mobility data using generate_bangalore_sample_data.

### Create Kafka Producer:
A Kafka producer is created with create_kafka_producer to send data to the Kafka topic.

### Push Data to Kafka:
Each row in the generated data is sent to Kafka using push_data_to_kafka.

### Return Status:
A success message is returned upon completing the data push.