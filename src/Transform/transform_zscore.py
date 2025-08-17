from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, lit, window,
    avg, stddev_samp, coalesce, date_format, expr,
    array, size, explode, when, collect_list, max
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, ArrayType
)

# Kafka and checkpoint configuration constants
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092" # Or "broker:9092" if using Docker
KAFKA_PRICE_TOPIC = "btc-price"
KAFKA_MOVING_TOPIC = "btc-price-moving"
KAFKA_ZSCORE_TOPIC = "btc-price-zscore"
BASE_CHECKPOINT_PATH = "/tmp/spark_checkpoints/zscore"

def create_spark_session():
    spark = SparkSession \
        .builder \
        .appName("TransformZscore") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark

def define_raw_schema():
    """Define schema for raw BTC price data from Kafka"""
    return StructType([
        StructField("symbol", StringType()),
        StructField("price", DoubleType()),
        StructField("timestamp", TimestampType())
    ])

def define_moving_schema():
    """Define schema for moving average data coming from the moving average processor"""
    return StructType([
        StructField("timestamp", TimestampType()),
        StructField("symbol", StringType()),
        StructField("windows", ArrayType(StructType([
            StructField("window", StringType()),
            StructField("avg_price", DoubleType()),
            StructField("std_price", DoubleType()),
        ])))
    ])

def consume_price_topic(spark):
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_PRICE_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    schema = define_raw_schema()

    # Parse JSON data and add watermark for late data
    parsed_df = kafka_df \
        .select(
            from_json(col("value").cast("string"), schema).alias("data")
        ) \
        .select("data.*") \
        .withWatermark("timestamp", "10 seconds")

    return parsed_df

def consume_moving_topic(spark):
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_MOVING_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    schema = define_moving_schema()

    # Parse JSON data and add watermark for late data
    parsed_df = kafka_df \
        .select(
            from_json(col("value").cast("string"), schema).alias("data")
        ) \
        .select("data.*") \
        .withWatermark("timestamp", "10 seconds")

    # Flatten the 'windows' array so that each window entry becomes an individual row
    flatten_df = parsed_df \
        .select (
            col("timestamp"),
            col("symbol"),
            explode(col("windows")).alias("win")
        ) \
        .select (
            col("timestamp"),
            col("symbol"),
            col("win.window").alias("window"),
            col("win.avg_price").alias("avg"),
            col("win.std_price").alias("std")
        )

    return flatten_df

def cal_zscore(price_df, moving_df):
    # Rename columns to avoid conflicts during join
    price_df = price_df.select(
        col("timestamp").alias("price_ts"),
        col("symbol").alias("price_sym"),
        col("price")
    )

    moving_df = moving_df.select(
        col("timestamp").alias("moving_ts"),
        col("symbol").alias("moving_sym"),
        col("window"),
        col("avg"),
        col("std")
    )

    # Join data based on matching timestamp and symbol
    joined_df = price_df.join(
        moving_df,
        (col("price_ts") == col("moving_ts")) & (col("price_sym") == col("moving_sym")),
        how="inner"
    )

    # Calculate Z-score: (price - avg) / std
    # Handle division by zero with a threshold check
    zscore_col = when(col("std") < 1e-4, lit(0.0)) \
        .otherwise((col("price") - col("avg")) / col("std"))

    zscore_df = joined_df.withColumn(
        "zscore_price", zscore_col
    ).select(
        col("price_ts").alias("timestamp"),
        col("price_sym").alias("symbol"),
        col("window"),
        col("zscore_price")
    )

    # Remove potential duplicates by taking max Z-score per group
    max_zscore_df = zscore_df.groupBy("timestamp", "symbol", "window") \
        .agg(max("zscore_price").alias("zscore_price"))

    # Format final output with nested structure and JSON value for Kafka
    final_df = max_zscore_df.groupBy("timestamp", "symbol") \
        .agg(
            collect_list(
                struct(
                    col("window"),
                    col("zscore_price")
                )
            ).alias("zscores")
        ) \
        .select(
            col("timestamp"),
            col("symbol"),
            col("zscores"),
            to_json(
                struct(
                    col("timestamp"),
                    col("symbol"),
                    col("zscores")
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
        .outputMode("append") \
        .start()

def main():
    spark = create_spark_session()
    price_df = consume_price_topic(spark)
    moving_df = consume_moving_topic(spark)
    final_df = cal_zscore(price_df, moving_df)

    # Write results to output Kafka topic
    query = produce_to_kafka(
        df=final_df,
        topic=KAFKA_ZSCORE_TOPIC,
        checkpoint_path=BASE_CHECKPOINT_PATH
    )

    # Keep the application running until manually terminated
    query.awaitTermination()

if __name__ == "__main__":
    main()
