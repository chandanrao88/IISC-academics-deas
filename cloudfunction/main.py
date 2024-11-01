import functions_framework
import pandas as pd
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json

# Function to generate sample mobility footfall data in Bangalore
def generate_bangalore_sample_data(num_records=100):
    bangalore_lat = 12.9821
    bangalore_lon = 77.5806
    days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    data = {
        'hashed_device_id': [f'device_{i}' for i in range(num_records)],
        'timezone_visit': ['UTC+5:30'] * num_records,
        'day_of_week_visit': [random.choice(days_of_week) for _ in range(num_records)],
        'time_stamp': [(datetime.now() - timedelta(days=random.randint(0, 30))).timestamp() for _ in range(num_records)],
        'lat_visit': [round(bangalore_lat + random.uniform(-0.01, 0.01), 6) for _ in range(num_records)],
        'data_visit': [(datetime.now() - timedelta(days=random.randint(0, 30))).date().isoformat() for _ in range(num_records)],
        'time_visit': [datetime.now().time().replace(hour=random.randint(0, 23), minute=random.randint(0, 59), second=random.randint(0, 59)).strftime('%H:%M:%S') for _ in range(num_records)],
        'lon_visit': [round(bangalore_lon + random.uniform(-0.01, 0.01), 6) for _ in range(num_records)]
    }
    df = pd.DataFrame(data)
    return df

# Kafka producer configuration
def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=['10.142.0.3:9092'],  # Replace <GCP_VM_IP> with your Kafka broker IP address
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

# Function to push DataFrame rows to Kafka
def push_data_to_kafka(df, topic):
    producer = create_kafka_producer()
    for _, row in df.iterrows():
        producer.send(topic, value=row.to_dict())
    producer.flush()  # Ensure all messages are sent before closing
    producer.close()

# Cloud Function Entry Point
@functions_framework.http
def send_bangalore_data_to_kafka(request):
    num_records = int(request.args.get('num_records', 100))  # Default to 100 if not specified
    topic = request.args.get('topic', 'visit-data-topic')  # Default topic if not specified

    # Generate sample data
    sample_data = generate_bangalore_sample_data(num_records)

    # Push data to Kafka
    push_data_to_kafka(sample_data, topic)
    
    return f"Successfully sent {num_records} records to Kafka topic '{topic}'.", 200
