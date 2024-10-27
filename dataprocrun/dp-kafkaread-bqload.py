import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .appName("KafkaToBigQuery") \
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.kafka:kafka-clients:3.8.0") \
    .config("parentProject", "team-plutus-iisc") \
    .getOrCreate()
    
# Kafka topic and servers configuration
kafka_bootstrap_servers = "10.142.0.3:9092"
kafka_topic = "visit-data-topic"

# Schema for parsing Kafka messages (assuming JSON format)
message_schema = StructType([
    StructField("hashed_device_id", StringType(), True),
    StructField("timezone_visit", StringType(), True),
    StructField("day_of_week_visit", StringType(), True),
    StructField("time_stamp", StringType(), True),
    StructField("lat_visit", StringType(), True),
    StructField("data_visit", StringType(), True),
    StructField("time_visit", StringType(), True),
    StructField("lon_visit", StringType(), True)
])

# Read data from Kafka topic
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.security.protocol", "PLAINTEXT") \
    .option("auto.offset.reset", "earliest") \
    .load()

# Convert the Kafka value (binary) to a string and parse the JSON
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), message_schema).alias("data")) \
    .select("data.*")  # Extract the individual fields

# BigQuery configuration
bigquery_table = "team-plutus-iisc.location.visited_location"

# Function to write micro-batch to BigQuery
def write_to_bigquery(df, epoch_id):
    df.write \
        .format("com.google.cloud.spark.bigquery.v2.Spark34BigQueryTableProvider") \
        .option("table", bigquery_table) \
        .option("temporaryGcsBucket", "gs://visited-location/data/") \
        .mode("append") \
        .save()

# Write to BigQuery in micro-batches
query = parsed_df \
    .writeStream \
    .foreachBatch(write_to_bigquery) \
    .option("project", "team-plutus-iisc") \
    .option("checkpointLocation", "gs://visited-location/data/") \
    .trigger(processingTime="10 seconds") \
    .start()

# Await termination
query.awaitTermination()