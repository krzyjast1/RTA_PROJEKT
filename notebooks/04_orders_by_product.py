"""
ZADANIE 4 – Orders per Product (okno 5 min sliding co 1 min, watermark)
Czyta wzbogacone zamówienia z topiku `orders.enriched`,
agreguje COUNT(*) per product_id w sliding window 5 min (krok 1 min)
z watermarkiem 1 min,
publikuje wyniki na topik `agg.orders.by.product`.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct,
    window, count
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)

# Konfiguracja
KAFKA_BROKER    = "broker:9092"
TOPIC_ENRICHED  = "orders.enriched"
TOPIC_OUTPUT    = "agg.orders.by.product"
CHECKPOINT_DIR  = "/tmp/checkpoints/orders_by_product"
WINDOW_DURATION = "5 minutes"
SLIDE_DURATION  = "1 minute"
WATERMARK_DELAY = "1 minute"

# SparkSession
spark = (
    SparkSession.builder
    .appName("OrdersByProductAggregator")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Schema – identyczna jak w zadaniu 5
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

# Watermark + sliding window 5 min z krokiem 1 min
agg = (
    parsed
    .withWatermark("event_time", WATERMARK_DELAY)
    .groupBy(
        window(col("event_time"), WINDOW_DURATION, SLIDE_DURATION),  # <-- sliding
        col("product_id"),
        col("product_name"),
    )
    .agg(
        count("order_id").alias("order_count")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("product_id"),
        col("product_name"),
        col("order_count"),
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

print(" Orders-by-product aggregator działa")
query.awaitTermination()
