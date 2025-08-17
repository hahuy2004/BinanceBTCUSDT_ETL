from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, lit, window,
    avg, stddev, collect_list, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType
)

# Kafka and checkpoint configuration constants
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092" # Or "broker:9092" if using Docker
KAFKA_PRICE_TOPIC = "btc-price"
KAFKA_MOVING_TOPIC = "btc-price-moving"
BASE_CHECKPOINT_PATH = "/tmp/spark_checkpoints/moving"

def create_spark_session():
    spark = SparkSession \
        .builder \
        .appName("TransformMovingAverage") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark

def define_raw_schema():
    return StructType([
        StructField("symbol", StringType()),
        StructField("price", DoubleType()),
        StructField("timestamp", TimestampType())
    ])

def consume_price_topic(spark):
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_PRICE_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "100") \
        .load()

    schema = define_raw_schema()

    # Parse JSON data from Kafka messages
    parsed_df = kafka_df \
        .select(
            from_json(col("value").cast("string"), schema).alias("data")
        ) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp")))

    return parsed_df

def define_time_windows():
    return [
        ("30s", "30 seconds"),
        ("1m", "1 minute"),
        ("5m", "5 minutes"),
        ("15m", "15 minutes"),
        ("30m", "30 minutes"),
        ("1h", "1 hour")
    ]

def cal_stats(parsed_df, time_windows):
    df_with_watermark = parsed_df.withWatermark("timestamp", "10 seconds")

    # Define slide intervals for each window duration
    slide_intervals = {
        "30 seconds": "10 seconds",
        "1 minute": "20 seconds",
        "5 minutes": "2 minutes",
        "15 minutes": "5 minutes",
        "30 minutes": "10 minutes",
        "1 hour": "20 minutes"
    }

    window_dfs = None
    # Process each time window
    for name, duration in time_windows:
        slide_interval = slide_intervals.get(duration, "10 seconds")
        window_spec = window(col("timestamp"), duration, slide_interval)

        # Calculate average and standard deviation for the current window
        window_df = df_with_watermark \
            .groupBy(
                col("symbol"),
                window_spec.alias("window")
            ) \
            .agg(
                avg("price").alias("avg_price"),
                stddev("price").alias("std_price")
            ) \
            .select(
                col("symbol"),
                col("window.end").alias("timestamp"),
                lit(name).alias("window"),
                col("avg_price"),
                col("std_price")
            )

        # Union results from all windows
        if window_dfs is None:
            window_dfs = window_df
        else:
            window_dfs = window_dfs.union(window_df)

    # Group statistics by timestamp and symbol
    final_df = window_dfs \
        .groupBy("timestamp", "symbol") \
        .agg(
            collect_list(
                struct(
                    col("window"),
                    col("avg_price"),
                    col("std_price")
                )
            ).alias("windows")
        ) \
        .select(
            col("timestamp"),
            col("symbol"),
            col("windows"),
            # Prepare JSON output for Kafka
            to_json(
                struct(
                    col("timestamp"),
                    col("symbol"),
                    col("windows")
                )
            ).alias("value")
        )

    return final_df

def produce_to_kafka(df, topic, checkpoint_path):
    return df \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", topic) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("update") \
        .start()

def main():
    spark = create_spark_session()
    parsed_df = consume_price_topic(spark)
    time_windows = define_time_windows()
    final_df = cal_stats(parsed_df, time_windows)

    # Write results to output Kafka topic
    query = produce_to_kafka(
        df=final_df,
        topic=KAFKA_MOVING_TOPIC,
        checkpoint_path=BASE_CHECKPOINT_PATH
    )

    # Keep the application running until manually terminated
    query.awaitTermination()

if __name__ == "__main__":
    main()
