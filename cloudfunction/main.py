import functions_framework
import pandas as pd
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json

# Function to generate sample mobility footfall data for a given location
def generate_sample_data(lat, lon, num_records=100):
    days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    data = {
        'device_id': [f"+447{random.randint(100000000, 999999999)}" for _ in range(num_records)],
        'timezone_visit': ['UTC+0'] * num_records,  # Default to UTC+0; adjust if needed
        'day_of_week_visit': [random.choice(days_of_week) for _ in range(num_records)],
        'time_stamp': [(datetime.now() - timedelta(days=random.randint(0, 30))).timestamp() for _ in range(num_records)],
        'lat_visit': [round(lat + random.uniform(-0.0001, 0.0001), 6) for _ in range(num_records)],
        'data_visit': [(datetime.now() - timedelta(days=random.randint(0, 30))).date().isoformat() for _ in range(num_records)],
        'time_visit': [datetime.now().time().replace(hour=random.randint(0, 23), minute=random.randint(0, 59), second=random.randint(0, 59)).strftime('%H:%M:%S') for _ in range(num_records)],
        'lon_visit': [round(lon + random.uniform(-0.0001, 0.0001), 6) for _ in range(num_records)]
    }
    df = pd.DataFrame(data)
    return df

# Kafka producer configuration
def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=['10.142.0.3:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

# Function to push DataFrame rows to Kafka
def push_data_to_kafka(df, topic):
    producer = create_kafka_producer()
    for _, row in df.iterrows():
        producer.send(topic, value=row.to_dict())
    producer.flush()  # Ensure all messages are sent before closing
    producer.close()

@functions_framework.http
def location_data_gen(request):
    try:
        request_json = request.get_json()

        if not request_json:
            return "Invalid JSON body.", 400

        # Extract required parameters
        lat = float(request_json['lat'])
        lon = float(request_json['lon'])
        num_records = int(request_json['num_records'])

        # Generate data and push to Kafka
        sample_data = generate_sample_data(lat, lon, num_records)
        push_data_to_kafka(sample_data, "visit-data-topic")

        return f"Successfully processed {num_records} records for lat={lat}, lon={lon}.", 200

    except KeyError as e:
        return f"Missing required parameter: {str(e)}", 400
    except ValueError as e:
        return f"Invalid parameter value: {str(e)}", 400
    except Exception as e:
        return f"An error occurred: {str(e)}", 500
