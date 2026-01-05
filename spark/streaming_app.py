from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    lit,
    window,
    when,
)

from schema import event_schema


spark = (
    SparkSession.builder.appName("Advanced-Structured-Streaming").getOrCreate()
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
    from_json(col("value").cast("string"), event_schema).alias("event"),
).select("raw_value", "event.*")


# 3️⃣ Validation de la qualité des données
parsed_with_flags = parsed.withColumn(
    "is_valid",
    col("device_id").isNotNull()
    & col("country").isNotNull()
    & col("temperature").isNotNull()
    & col("event_time").isNotNull()
    & col("temperature").between(-50, 150),
).withColumn(
    "error_reason",
    when(
        col("device_id").isNull()
        & col("country").isNull()
        & col("temperature").isNull()
        & col("event_time").isNull(),
        lit("invalid_json_or_schema_mismatch"),
    )
    .when(col("device_id").isNull(), lit("missing_device_id"))
    .when(col("country").isNull(), lit("missing_country"))
    .when(col("temperature").isNull(), lit("missing_temperature"))
    .when(~col("temperature").between(-50, 150), lit("temperature_out_of_range"))
    .when(col("event_time").isNull(), lit("missing_event_time"))
    .otherwise(lit(None)),
)

valid_events = parsed_with_flags.filter(col("is_valid")).drop("is_valid", "error_reason")
invalid_events = parsed_with_flags.filter(~col("is_valid")).select(
    "raw_value", "device_id", "country", "temperature", "event_time", "error_reason"
)


# 4️⃣ Sink des données invalides (console + fichier)
invalid_query_console = (
    invalid_events.writeStream.format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

invalid_query_file = (
    invalid_events.writeStream.format("json")
    .option("path", "/data/invalid_events")
    .option("checkpointLocation", "/data/checkpoints/invalid_events")
    .outputMode("append")
    .start()
)


# 5️⃣ Event time + watermark
with_watermark = valid_events.withWatermark("event_time", "10 minutes")


# 6️⃣ Agrégation stateful par fenêtre
avg_temp = (
    with_watermark.groupBy(window(col("event_time"), "5 minutes"), col("device_id"))
    .avg("temperature")
    .withColumnRenamed("avg(temperature)", "avg_temperature")
)

country_counts = with_watermark.groupBy(
    window(col("event_time"), "5 minutes"), col("country")
).count()


# 7️⃣ Output streaming (console + fichiers parquet)
avg_temp_query = (
    avg_temp.writeStream.format("console")
    .outputMode("update")
    .option("truncate", "false")
    .start()
)

avg_temp_file_query = (
    avg_temp.writeStream.format("parquet")
    .option("path", "/data/output/avg_temperature")
    .option("checkpointLocation", "/data/checkpoints/avg_temperature")
    .outputMode("append")
    .start()
)

country_counts_query = (
    country_counts.writeStream.format("parquet")
    .option("path", "/data/output/country_counts")
    .option("checkpointLocation", "/data/checkpoints/country_counts")
    .outputMode("append")
    .start()
)

spark.streams.awaitAnyTermination()
