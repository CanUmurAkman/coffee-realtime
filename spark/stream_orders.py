import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, to_timestamp,
    sum as _sum
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Config via environment for easy switching (host vs container later)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "orders")
DATA_ROOT = os.getenv("DATA_ROOT", ".")  # path root for data and checkpoints

schema = StructType([
    StructField("order_id",    StringType(), True),
    StructField("store_id",    StringType(), True),
    StructField("product_id",  StringType(), True),
    StructField("unit_price",  DoubleType(), True),
    StructField("quantity",    IntegerType(), True),
    StructField("total_price", DoubleType(), True),
    StructField("event_time",  StringType(), True),  # will cast to timestamp
])

spark = (
    SparkSession.builder
    .appName("orders-stream")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    )
    .getOrCreate()
)

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) as json_str")
       .select(from_json(col("json_str"), schema).alias("d"))
       .select("d.*")
       .withColumn("event_ts", to_timestamp(col("event_time")))
)

# 1) Raw events → Parquet ("bronze" layer)
bronze_path = os.path.join(DATA_ROOT, "data/bronze/orders")
bronze_cp   = os.path.join(DATA_ROOT, "checkpoints/bronze_orders")

bronze_q = (
    parsed.writeStream
          .format("parquet")
          .option("path", bronze_path)
          .option("checkpointLocation", bronze_cp)
          .outputMode("append")
          .start()
)

# 2) Per-minute revenue → Parquet ("silver" layer)
metrics = (
    parsed.withWatermark("event_ts", "5 minutes")
          .groupBy(window(col("event_ts"), "1 minute"))
          .agg(_sum("total_price").alias("revenue"))
          .select(
              col("window.start").alias("minute_start"),
              col("window.end").alias("minute_end"),
              col("revenue")
          )
)

silver_path = os.path.join(DATA_ROOT, "data/silver/minute_metrics")
silver_cp   = os.path.join(DATA_ROOT, "checkpoints/minute_metrics")

silver_q = (
    metrics.writeStream
           .format("parquet")
           .option("path", silver_path)
           .option("checkpointLocation", silver_cp)
           .outputMode("append")
           .start()
)

spark.streams.awaitAnyTermination()
