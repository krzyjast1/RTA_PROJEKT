"""
ZADANIE 3 – Enrichment Consumer
Czyta surowe zamówienia z topiku `orders.raw`,
spłaszcza koszyk (1 wiersz = 1 produkt),
wzbogaca o product_name, category, unit_price z katalogu CSV,
liczy line_value = quantity * unit_price,
publikuje na topik `orders.enriched`.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, from_json, to_json, struct,
    round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, ArrayType
)


# Konfiguracja

KAFKA_BROKER     = "broker:9092"          
TOPIC_RAW        = "orders.raw"         
TOPIC_ENRICHED   = "orders.enriched"
CATALOG_PATH     = "data/product_catalog.csv"
CHECKPOINT_DIR   = "/tmp/checkpoints/enrichment"


# SparkSession

spark = (
    SparkSession.builder
    .appName("EnrichmentConsumer")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# Schema – dokładnie taka jak publikuje orders.py
# {
#   "order_id":  "...",
#   "user_id":   "...",
#   "timestamp": "2026-05-24T12:34:56Z",
#   "items":     [{"product_id": "P001", "quantity": 2}, ...]
# }

item_schema = StructType([
    StructField("product_id", StringType()),
    StructField("quantity",   IntegerType()),
])

raw_schema = StructType([
    StructField("order_id",  StringType()),
    StructField("user_id",   StringType()),
    StructField("timestamp", StringType()),
    StructField("items",     ArrayType(item_schema)),
])


# Katalog produktów (statyczny – wczytany raz)
# Kolumny CSV: product_id, product_name, category, unit_price

catalog_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(CATALOG_PATH)
    .select("product_id", "product_name", "category", "unit_price")
)


# Odczyt strumienia z Kafki
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_RAW)
    .option("startingOffsets", "latest")
    .load()
)


# Parsowanie JSON -spłaszczenie koszyka
parsed = (
    raw_stream
    .select(from_json(col("value").cast("string"), raw_schema).alias("o"))
    .select("o.*")
)

# explode: z 1 zamówienia z N produktami -N wierszy
flat = (
    parsed
    .select(
        col("order_id"),
        col("user_id"),
        col("timestamp"),
        explode(col("items")).alias("item"),
    )
    .select(
        col("order_id"),
        col("user_id"),
        col("timestamp"),
        col("item.product_id").alias("product_id"),
        col("item.quantity").alias("quantity"),
    )
)


# Wzbogacenie – join z katalogiem CSV

enriched = (
    flat
    .join(catalog_df, on="product_id", how="left")
    .withColumn("line_value", spark_round(col("quantity") * col("unit_price"), 2))
    .select(
        "order_id",
        "user_id",
        "product_id",
        "quantity",
        "timestamp",
        "product_name",
        "category",
        "unit_price",
        "line_value",
    )
)


# Publikacja na orders.enriched

query = (
    enriched
    .select(to_json(struct("*")).alias("value"))
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("topic", TOPIC_ENRICHED)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .outputMode("append")
    .start()
)

print(f" Enrichment consumer działa")

query.awaitTermination()
