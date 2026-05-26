"""
ZADANIE 5 – Revenue per Category (okno 1 min, watermark)
Czyta wzbogacone zamówienia z topiku `orders.enriched`,
agreguje SUM(line_value) per category w tumbling window 1 min
z watermarkiem 1 min,
publikuje wyniki na topik `agg.revenue.by.category`.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct,
    window, sum as spark_sum,
    round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)


# Konfiguracja
KAFKA_BROKER     = "broker:9092"
TOPIC_ENRICHED   = "orders.enriched"
TOPIC_OUTPUT     = "agg.revenue.by.category"
CHECKPOINT_DIR   = "/tmp/checkpoints/revenue_by_category"

WINDOW_DURATION  = "1 minute"
WATERMARK_DELAY  = "1 minute"


# SparkSession
spark = (
    SparkSession.builder
    .appName("RevenueByCategoryAggregator")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# Schema – dokładnie to co publikuje zadanie 3

enriched_schema = StructType([
    StructField("order_id",     StringType()),
    StructField("user_id",      StringType()),
    StructField("product_id",   StringType()),
    StructField("quantity",     IntegerType()),
    StructField("timestamp",    StringType()),
    StructField("product_name", StringType()),
    StructField("category",     StringType()),
    StructField("unit_price",   DoubleType()),
    StructField("line_value",   DoubleType()),
])


# Odczyt strumienia z Kafki
enriched_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_ENRICHED)
    .option("startingOffsets", "latest")
    .load()
)


# Parsowanie i rzutowanie timestamp na typ czasowy

parsed = (
    enriched_stream
    .select(from_json(col("value").cast("string"), enriched_schema).alias("e"))
    .select("e.*")
    .withColumn("event_time", col("timestamp").cast(TimestampType()))
)


# Watermark + agregacja w oknie 1 minuta

agg = (
    parsed
    .withWatermark("event_time", WATERMARK_DELAY)
    .groupBy(
        window(col("event_time"), WINDOW_DURATION),
        col("category"),
    )
    .agg(
        spark_sum("line_value").alias("total_revenue")
    )
    .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("category"),
        col("total_revenue"),
    )
)


# Publikacja na topik

query = (
    agg
    .select(to_json(struct("*")).alias("value"))
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("topic", TOPIC_OUTPUT)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .outputMode("update")   # emituje tylko zmienione okna
    .start()
)

print(f" Revenue aggregator działa")

query.awaitTermination()
