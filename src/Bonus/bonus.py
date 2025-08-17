# Bonus for Transform in ETL
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, expr, window, unix_timestamp, lit,
    coalesce, min as spark_min, first, when, isnan, isnull, date_format, round
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Topic Kafka
KAFKA_PRICE_TOPIC = "btc-price"
KAFKA_HIGHER_TOPIC = "btc-price-higher"
KAFKA_LOWER_TOPIC = "btc-price-lower"

# Spark Session
spark = SparkSession \
    .builder \
    .appName("BTCPriceWindow") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
    .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define the schema of the JSON data received from Kafka
schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("timestamp", StringType()),  # ISO8601 string
])

# Read stream from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \ # Or "broker:9092" if using Docker
    .option("subscribe", KAFKA_PRICE_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON string and cast timestamp
price_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(
        col("data.symbol"),
        col("data.price"),
        col("data.timestamp").cast(TimestampType()).alias("event_time")
    ).withWatermark("event_time", "10 seconds")

# Self join to find future records within the next 20s
# Spark requires an "=" condition for stream–stream join
# Use symbol as the equality condition, then filter by timestamp after the join
joined_df = price_df.alias("base").join(
    price_df.alias("future"),
    col("base.symbol") == col("future.symbol"),
    how="inner"
).filter(
    (col("future.event_time") > col("base.event_time")) &
    (col("future.event_time") <= col("base.event_time") + expr("interval 20 seconds"))
).select(
    col("base.event_time").alias("base_time"),
    col("base.price").alias("base_price"),
    col("future.event_time").alias("future_time"),
    col("future.price").alias("future_price")
).withColumn(
    # Calculate the time difference between two timestamps
    "time_diff",
    (col("future_time").cast("double") - col("base_time").cast("double"))
)

# Find the first timestamp where the price increases within the next 20s
higher_candidates = joined_df \
    .filter(col("future_price") > col("base_price")) \
    .groupBy("base_time", "base_price") \
    .agg(spark_min("time_diff").alias("min_time_diff")) \
    .select(
        col("base_time"),
        col("base_price"),
        round(col("min_time_diff"), 3).alias("higher_window")  # Làm tròn 3 chữ số
    )

# Find the first timestamp where the price decreases within the next 20s
lower_candidates = joined_df \
    .filter(col("future_price") < col("base_price")) \
    .groupBy("base_time", "base_price") \
    .agg(spark_min("time_diff").alias("min_time_diff")) \
    .select(
        col("base_time"),
        col("base_price"),
        round(col("min_time_diff"), 3).alias("lower_window")  # Làm tròn 3 chữ số
    )

# Get all original records from price_df (distinct)
base_records = price_df.select(
    col("event_time").alias("base_time"),
    col("price").alias("base_price")
).distinct()

# Create the final result for the higher window, assign 20.0 if not found
final_higher = base_records.alias("br").join(
    higher_candidates.alias("hc"),
    (col("br.base_time") == col("hc.base_time")) &
    (col("br.base_price") == col("hc.base_price")),
    how="left"
).select(
    col("br.base_time").alias("timestamp"),
    coalesce(col("hc.higher_window"), lit(20.0)).alias("higher_window")
)

# Create the final result for the lower window, assign 20.0 if not found
final_lower = base_records.alias("br").join(
    lower_candidates.alias("lc"),
    (col("br.base_time") == col("lc.base_time")) &
    (col("br.base_price") == col("lc.base_price")),
    how="left"
).select(
    col("br.base_time").alias("timestamp"),
    coalesce(col("lc.lower_window"), lit(20.0)).alias("lower_window")
)

# Write higher window results to Kafka
higher_query = final_higher.select(
    to_json(struct(
        date_format(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("timestamp"),
        col("higher_window")
    )).alias("value")
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \ # Or "broker:9092" if using Docker
    .option("topic", KAFKA_HIGHER_TOPIC) \
    .option("checkpointLocation", "/tmp/spark-checkpoint-higher") \
    .outputMode("append") \
    .trigger(processingTime='5 seconds') \
    .start()

# Write lower window results to Kafka
lower_query = final_lower.select(
    to_json(struct(
        date_format(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("timestamp"),
        col("lower_window")
    )).alias("value")
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \ # Or "broker:9092" if using Docker
    .option("topic", KAFKA_LOWER_TOPIC) \
    .option("checkpointLocation", "/tmp/spark-checkpoint-lower") \
    .outputMode("append") \
    .trigger(processingTime='5 seconds') \
    .start()

# Wait for both streams to end
try:
    higher_query.awaitTermination()
    lower_query.awaitTermination()
except KeyboardInterrupt:
    print("Shutting down...")
    higher_query.stop()
    lower_query.stop()
    spark.stop()
