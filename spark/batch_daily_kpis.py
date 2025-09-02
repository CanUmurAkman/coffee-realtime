import os
import sys
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, sum as _sum, count as _count, lit

DATA_ROOT = os.getenv("DATA_ROOT", ".")

# Argument: YYYY-MM-DD (defaults to today's UTC date)
target = sys.argv[1] if len(sys.argv) > 1 else datetime.datetime.utcnow().date().isoformat()

spark = (
    SparkSession.builder
    .appName("daily-kpis")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

bronze_path = os.path.join(DATA_ROOT, "data/bronze/orders")
df = spark.read.parquet(bronze_path)

df = df.withColumn("event_date", to_date(col("event_ts")))

daily = (
    df.filter(col("event_date") == target)
      .agg(
          _count("*").alias("orders_count"),
          _sum("total_price").alias("revenue_usd")
      )
)

# Add the date column explicitly
daily = daily.select(
    lit(target).alias("date"),
    col("orders_count"),
    col("revenue_usd")
)

gold_dir = os.path.join(DATA_ROOT, f"data/gold/daily_key_metrics_{target}")
(daily.coalesce(1)
      .write.mode("overwrite")
      .option("header", True)
      .csv(gold_dir))

print(f"Wrote daily KPIs to: {gold_dir}")