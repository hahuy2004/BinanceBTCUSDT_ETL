from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, explode, to_timestamp, lit, current_timestamp,
    sha2, concat_ws, when
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, ArrayType
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ---- Configuration ----
# Kafka connection and topic settings
KAFKA_BOOTSTRAP_SERVERS = "broker:9092"
KAFKA_ZSCORE_TOPIC = "btc-price-zscore"
# MongoDB connection settings
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE")
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_COLLECTION_PREFIX = "btc-price-zscore-"
# Spark checkpoint path for recovery
LOAD_CHECKPOINT_PATH="/tmp/spark_checkpoints/load"

def create_spark_session():
    """
    Creates and configures the Spark session with necessary packages and settings
    for Kafka and MongoDB connectivity.
    """
    spark = SparkSession \
        .builder \
        .appName("Load") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.5.0") \
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
        .config("spark.mongodb.write.connection.uri", MONGODB_URI) \
        .config("spark.mongodb.write.database", MONGODB_DATABASE) \
        .config("spark.mongodb.write.operationType", "update") \
        .config("spark.mongodb.write.upsertDocument", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def define_zscore_schema():
    return StructType([
        StructField("timestamp", TimestampType()),
        StructField("symbol", StringType()),
        StructField("zscores", ArrayType(StructType([
            StructField("window", StringType()),
            StructField("zscore_price", DoubleType())
        ])))
    ])

def read_zscore_stream(spark):
    # Create streaming connection to Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_ZSCORE_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", "100") \
        .load()

    schema = define_zscore_schema()
    # Parse JSON data and add watermark for late data
    parsed_df = kafka_df \
        .select(
            from_json(col("value").cast("string"), schema).alias("data")
        ) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withWatermark("timestamp", "10 seconds")

    return parsed_df

def write_to_mongodb(micro_batch_df, epoch_id):
    if micro_batch_df.rdd.isEmpty():
        print(f"Epoch {epoch_id}: Empty micro-batch, skipping MongoDB write.")
        return

    spark = SparkSession.builder.getOrCreate()

    # Explode the zscores array and prepare data for MongoDB
    df_for_mongo = micro_batch_df.select(
        col("timestamp"), col("symbol"), explode(col("zscores")).alias("zscore_info")
    ).select(
        # Create a unique document ID using hash of composite key
        sha2(concat_ws("||", col("timestamp").cast("string"), col("symbol"), col("zscore_info.window")), 256).alias("_id"),
        col("timestamp"),
        col("symbol"),
        col("zscore_info.window").alias("window"),
        col("zscore_info.zscore_price").alias("zscore_price"),
        lit(epoch_id).alias("batch_id"),
        current_timestamp().alias("last_updated")
    )

    print(f"Epoch {epoch_id}: DataFrame prepared with {df_for_mongo.count()} records for potential upsert.")
    distinct_windows_in_batch = [row.window for row in df_for_mongo.select("window").distinct().collect()]

    # Process each time window separately
    for window_val in distinct_windows_in_batch:
        window_specific_df_to_write = df_for_mongo.filter(col("window") == window_val)
        if window_specific_df_to_write.rdd.isEmpty():
            continue

        # Each time window gets its own collection
        collection_name = f"{MONGODB_COLLECTION_PREFIX}{window_val}"

        # --- Read existing data from MongoDB collection ---
        try:
            existing_df = spark.read \
                .format("mongodb") \
                .option("spark.mongodb.read.connection.uri", MONGODB_URI) \
                .option("spark.mongodb.read.database", MONGODB_DATABASE) \
                .option("spark.mongodb.read.collection", collection_name) \
                .load() \
                .select("_id", "update_count")
        except:
            existing_df = None

        # --- Join to calculate new update_count ---
        if existing_df and not existing_df.rdd.isEmpty():
            # Increment update_count for existing records
            joined_df = window_specific_df_to_write \
                .join(
                    existing_df, on="_id", how="left"
                ) \
                .withColumn(
                    "update_count", when(col("update_count").isNotNull(), col("update_count") + 1).otherwise(lit(1))
                )
        else:
            # Initialize update_count for new records
            joined_df = window_specific_df_to_write.withColumn("update_count", lit(1))

        # --- Write back to MongoDB ---
        try:
            joined_df.write \
                .format("mongodb") \
                .mode("append") \
                .option("spark.mongodb.write.connection.uri", MONGODB_URI) \
                .option("spark.mongodb.write.database", MONGODB_DATABASE) \
                .option("spark.mongodb.write.collection", collection_name) \
                .save()
            print(f"Epoch {epoch_id}: Successfully wrote to collection '{collection_name}'")
        except Exception as e:
            print(f"Epoch {epoch_id}: ERROR for collection '{collection_name}': {e}")
            raise e

def main():
    # Verify MongoDB settings are loaded
    if not MONGODB_DATABASE or not MONGODB_URI:
        print("ERROR: MongoDB settings not found in .env file")
        return
        
    spark = create_spark_session()

    zscore_df = read_zscore_stream(spark)

    # Configure streaming query to process data in micro-batches
    streaming_query = zscore_df \
        .writeStream \
        .foreachBatch(write_to_mongodb) \
        .option("checkpointLocation", LOAD_CHECKPOINT_PATH) \
        .outputMode("append") \
        .trigger(processingTime='20 seconds') \
        .start()
    print(f"INFO: Streaming query started! Query ID: {streaming_query.id}")
    try:
        # Monitor streaming query and check for exceptions
        while streaming_query.isActive:
            exception = streaming_query.exception();
            if exception: print(f"ERROR: Streaming Query Exception: {exception}"); streaming_query.stop(); break
            # Sleep to avoid CPU spinning
            spark.sparkContext.parallelize([1]).foreach(lambda x: __import__('time').sleep(1))
        streaming_query.awaitTermination()
    except KeyboardInterrupt:
        print("\nINFO: Stopping...");
        if streaming_query and streaming_query.isActive:
            print("INFO: Attempting to stop streaming query...")
            streaming_query.stop()
            print("INFO: Streaming query stop request sent.")
    except Exception as e:
        print(f"ERROR: {e}")
        if streaming_query and streaming_query.isActive:
            streaming_query.stop()
    finally:
        print("INFO: Shutting down Spark.")
        spark.stop()

if __name__ == "__main__":
    main()