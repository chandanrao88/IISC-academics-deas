import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Set environment variables for GCS
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/team-plutus.json'  # Replace with valid SA json key

# Note provide the valide parentProject name
spark = SparkSession.builder \
    .appName("KafkaToBigQuery") \
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .config("spark.jars.packages","com.google.cloud.spark:spark-bigquery-with-dependencies_2.12-0.41.0.jar") \
    .config("spark.jars.packages", "com.google.api-client:google-api-client:2.7.0") \
    .config("spark.jars.packages", "com.google.http-client:google-http-client:1.45.0") \
    .config("spark.jars.packages","com.google.oauth-client:google-oauth-client:1.36.0") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")) \
    .config("parentProject", "team-plutus") \
    .getOrCreate()
    
# Kafka topic and servers configuration
kafka_bootstrap_servers = "localhost:9092" #Replace local host to GCP hosted VM IP
kafka_topic = "topic" #Replace with acuall topic

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
    .option("auto.offset.reset", "earliest") \
    .load()

# Convert the Kafka value (binary) to a string and parse the JSON
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), message_schema).alias("data")) \
    .select("data.*")  # Extract the individual fields

# Show the parsed data (optional for testing)
parsed_df.printSchema()

# BigQuery configuration
bigquery_table = "team-plutus-iisc.location.visited_location"

# Function to write micro-batch to BigQuery
# Proivide the gcs bucket path
def write_to_bigquery(df, epoch_id):
    df.write \
        .format("bigquery") \
        .option("table", bigquery_table) \
        .option("temporaryGcsBucket", "gs://data/") \
        .mode("append") \
        .save()

# Write to BigQuery in micro-batches
# Note provide the valide GCP project name
query = parsed_df \
    .writeStream \
    .foreachBatch(write_to_bigquery) \
    .option("project", "team-plutus") \
    .option("checkpointLocation", "/Users/footfall") \
    .trigger(processingTime="1 minute") \
    .start()

# Await termination
query.awaitTermination()