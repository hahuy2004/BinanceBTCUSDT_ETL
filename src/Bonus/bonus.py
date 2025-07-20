from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, expr, window, unix_timestamp, lit,
    coalesce, min as spark_min, first, when, isnan, isnull, date_format, round
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Tên các topic Kafka
KAFKA_PRICE_TOPIC = "btc-price"
KAFKA_HIGHER_TOPIC = "btc-price-higher"
KAFKA_LOWER_TOPIC = "btc-price-lower"

# Tạo phiên Spark
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

# Định nghĩa schema của dữ liệu JSON nhận từ Kafka
schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("timestamp", StringType()),  # Dạng chuỗi ISO8601
])

# Đọc stream từ Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", KAFKA_PRICE_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Parse chuỗi JSON và ép kiểu timestamp
price_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(
        col("data.symbol"),
        col("data.price"),
        col("data.timestamp").cast(TimestampType()).alias("event_time")
    ).withWatermark("event_time", "10 seconds")

# Self join để tìm các bản ghi tương lai trong 20s tiếp theo
# Spark yêu cầu phải có điều kiện bằng "=" khi join stream–stream
# Ta dùng symbol làm điều kiện bằng, rồi lọc theo thời gian sau join
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
    # Tính khoảng thời gian chênh lệch giữa 2 thời điểm
    "time_diff",
    (col("future_time").cast("double") - col("base_time").cast("double"))
)

# Tìm thời điểm đầu tiên mà giá tăng trong vòng 20s tiếp theo
higher_candidates = joined_df \
    .filter(col("future_price") > col("base_price")) \
    .groupBy("base_time", "base_price") \
    .agg(spark_min("time_diff").alias("min_time_diff")) \
    .select(
        col("base_time"),
        col("base_price"),
        round(col("min_time_diff"), 3).alias("higher_window")  # Làm tròn 3 chữ số
    )

# Tìm thời điểm đầu tiên mà giá giảm trong vòng 20s tiếp theo
lower_candidates = joined_df \
    .filter(col("future_price") < col("base_price")) \
    .groupBy("base_time", "base_price") \
    .agg(spark_min("time_diff").alias("min_time_diff")) \
    .select(
        col("base_time"),
        col("base_price"),
        round(col("min_time_diff"), 3).alias("lower_window")  # Làm tròn 3 chữ số
    )

# Lấy tất cả các bản ghi gốc từ price_df (distinct)
base_records = price_df.select(
    col("event_time").alias("base_time"),
    col("price").alias("base_price")
).distinct()

# Tạo kết quả cuối cho higher window, nếu không tìm được thì gán 20.0
final_higher = base_records.alias("br").join(
    higher_candidates.alias("hc"),
    (col("br.base_time") == col("hc.base_time")) &
    (col("br.base_price") == col("hc.base_price")),
    how="left"
).select(
    col("br.base_time").alias("timestamp"),
    coalesce(col("hc.higher_window"), lit(20.0)).alias("higher_window")
)

# Tạo kết quả cuối cho lower window, nếu không tìm được thì gán 20.0
final_lower = base_records.alias("br").join(
    lower_candidates.alias("lc"),
    (col("br.base_time") == col("lc.base_time")) &
    (col("br.base_price") == col("lc.base_price")),
    how="left"
).select(
    col("br.base_time").alias("timestamp"),
    coalesce(col("lc.lower_window"), lit(20.0)).alias("lower_window")
)

# Ghi kết quả higher window ra Kafka
higher_query = final_higher.select(
    to_json(struct(
        date_format(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("timestamp"),
        col("higher_window")
    )).alias("value")
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("topic", KAFKA_HIGHER_TOPIC) \
    .option("checkpointLocation", "/tmp/spark-checkpoint-higher") \
    .outputMode("append") \
    .trigger(processingTime='5 seconds') \
    .start()

# Ghi kết quả lower window ra Kafka
lower_query = final_lower.select(
    to_json(struct(
        date_format(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("timestamp"),
        col("lower_window")
    )).alias("value")
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("topic", KAFKA_LOWER_TOPIC) \
    .option("checkpointLocation", "/tmp/spark-checkpoint-lower") \
    .outputMode("append") \
    .trigger(processingTime='5 seconds') \
    .start()

# Chờ cả hai stream kết thúc
try:
    higher_query.awaitTermination()
    lower_query.awaitTermination()
except KeyboardInterrupt:
    print("Shutting down...")
    higher_query.stop()
    lower_query.stop()
    spark.stop()