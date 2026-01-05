# Write your streaming app code here

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from schema import event_schema

spark = (
    SparkSession.builder
    .appName("Advanced-Structured-Streaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 1️⃣ Lire Kafka
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "iot-events")
    .option("startingOffsets", "latest")
    .load()
)

# 2️⃣ Parser JSON sans crash
parsed = raw_df.select(
    col("value").cast("string").alias("raw_value"),
    from_json(col("value").cast("string"), event_schema).alias("event")
)

# 3️⃣ Séparer données valides / invalides
valid_events = parsed.filter(col("event").isNotNull()).select("event.*")
invalid_events = parsed.filter(col("event").isNull())

# 4️⃣ Sink des données invalides
invalid_query = (
    invalid_events
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

# 5️⃣ Event time + watermark
with_watermark = (
    valid_events
    .withWatermark("event_time", "10 minutes")
)

# 6️⃣ Agrégation stateful par fenêtre
avg_temp = (
    with_watermark
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("device_id")
    )
    .avg("temperature")
)

# 7️⃣ Output streaming
agg_query = (
    avg_temp
    .writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", "false")
    .start()
)

spark.streams.awaitAnyTermination()
