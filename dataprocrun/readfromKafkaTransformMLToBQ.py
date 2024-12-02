import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sha2, from_unixtime, when, floor, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import ClusteringEvaluator
import numpy as np

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaToBigQueryWithKMeans") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.kafka:kafka-clients:3.8.0") \
    .config("parentProject", "team-plutus-iisc") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = "10.142.0.3:9092"
kafka_topic = "visit-data-topic"

# Schema for parsing Kafka messages
message_schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("timezone_visit", StringType(), True),
    StructField("day_of_week_visit", StringType(), True),
    StructField("time_stamp", StringType(), True),
    StructField("lat_visit", StringType(), True),  # Latitude
    StructField("date_visit", StringType(), True),
    StructField("time_visit", StringType(), True),
    StructField("lon_visit", StringType(), True)   # Longitude
])

# BigQuery configuration
bigquery_table = "team-plutus-iisc.location.visited_location_cluster_pred"
gcs_temp_bucket = "gs://visited-location/data_ml/"

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.security.protocol", "PLAINTEXT") \
    .load()

# Parse Kafka messages
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), message_schema).alias("data")) \
    .select(
        when(col("data.device_id").isNotNull(), sha2(col("data.device_id"), 256)).otherwise(None).alias("device_id"),  # Hash device_id
        from_unixtime(floor(col("data.time_stamp")), "yyyy-MM-dd HH:mm:ss").alias("date_visit"),  # Human-readable timestamp
        col("data.lat_visit").cast(DoubleType()).alias("lat_visit"),
        col("data.lon_visit").cast(DoubleType()).alias("lon_visit")
    ) \
    .filter(col("lat_visit").isNotNull() & col("lon_visit").isNotNull())

# Vectorize features for clustering
vector_assembler = VectorAssembler(inputCols=["lat_visit", "lon_visit"], outputCol="features")
vectorized_df = vector_assembler.transform(parsed_df)

# Utility function to check if a DataFrame is empty
def is_empty(df):
    return df.select(lit(1)).limit(1).count() == 0

# Elbow Method to find the optimal number of clusters
def find_optimal_k(data, max_k=10):
    if data.count() < 2:  # Minimum rows required for clustering
        raise ValueError("Insufficient data to determine optimal K")
    evaluator = ClusteringEvaluator()
    costs = []
    for k in range(2, max_k + 1):
        kmeans = KMeans(k=k, seed=42, featuresCol="features")
        model = kmeans.fit(data)
        predictions = model.transform(data)
        cost = evaluator.evaluate(predictions)
        costs.append(cost)
    optimal_k = np.argmin(costs) + 2  # Since k starts from 2
    return optimal_k

# Function to apply KMeans clustering on micro-batches
def apply_kmeans_on_batch(batch_df, batch_id):
    print(f"Processing batch {batch_id} with {batch_df.count()} rows.")
    if is_empty(batch_df):
        print(f"Batch {batch_id} is empty. Skipping processing.")
        return

    # Determine the optimal number of clusters
    try:
        optimal_k = find_optimal_k(batch_df)
    except ValueError as e:
        print(f"Error determining optimal K for batch {batch_id}: {e}")
        return

    # Fit KMeans model
    kmeans = KMeans(k=optimal_k, seed=42, featuresCol="features", predictionCol="cluster_pred")
    model = kmeans.fit(batch_df)
    clustered_df = model.transform(batch_df)

    # Select relevant columns
    final_df = clustered_df.select(
        "device_id",
        "date_visit",
        "lat_visit",
        "lon_visit",
        "cluster_pred"
    )

    # Write to BigQuery
    write_to_bigquery(final_df, batch_id)

# Function to write DataFrame to BigQuery
def write_to_bigquery(df, epoch_id):
    df.write \
        .format("com.google.cloud.spark.bigquery.v2.Spark34BigQueryTableProvider") \
        .option("table", bigquery_table) \
        .option("temporaryGcsBucket", gcs_temp_bucket) \
        .mode("append") \
        .save()

# Write clustered output to BigQuery with foreachBatch
query = vectorized_df.writeStream \
    .foreachBatch(apply_kmeans_on_batch) \
    .option("checkpointLocation", gcs_temp_bucket + "checkpoints/") \
    .trigger(processingTime="10 seconds") \
    .start()

# Await termination
query.awaitTermination()
