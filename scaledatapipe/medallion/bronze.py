"""Bronze layer: raw, immutable copy of every Kafka record.

One streaming query reads all topics and appends the records untouched (value
kept as the original JSON string) plus their Kafka coordinates, so any later
layer can be rebuilt from Bronze. Parquet file sink + checkpoint gives
exactly-once output.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from scaledatapipe.common import config

TABLE = "events"

# Schema of the Bronze Parquet table (partition columns last), needed by
# streaming readers since file sources cannot infer it.
SCHEMA = StructType([
    StructField("partition", IntegerType()),
    StructField("offset", LongType()),
    StructField("kafka_ts", TimestampType()),
    StructField("key", StringType()),
    StructField("value", StringType()),
    StructField("ingest_ts", TimestampType()),
    StructField("topic", StringType()),
    StructField("ingest_date", DateType()),
])


def path() -> str:
    return config.layer_path("bronze", TABLE)


def checkpoint() -> str:
    return config.checkpoint_path("bronze")


def transform(raw: DataFrame) -> DataFrame:
    return raw.select(
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_ts"),
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        F.current_timestamp().alias("ingest_ts"),
        F.to_date("timestamp").alias("ingest_date"),
    )


def start(spark: SparkSession, available_now: bool):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", ",".join(config.TOPICS))
        .option("startingOffsets", config.STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )
    writer = (
        transform(raw).writeStream.queryName("bronze")
        .format("parquet")
        .option("path", path())
        .option("checkpointLocation", checkpoint())
        .partitionBy("topic", "ingest_date")
        .outputMode("append")
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=config.TRIGGER_INTERVAL)
    return writer.start()


def read_stream(spark: SparkSession) -> DataFrame:
    """Bronze as a streaming source (reads the file sink's commit log)."""
    return spark.readStream.schema(SCHEMA).parquet(path())
