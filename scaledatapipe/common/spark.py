"""Spark helpers shared by all consumers."""
import argparse
import re
from pathlib import Path

import pyspark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from scaledatapipe.common import config


def kafka_package() -> str:
    """Kafka connector coordinates matching the installed PySpark.

    Derived from the Scala library bundled with PySpark, so the connector can
    never drift from the Spark/Scala version actually running.
    """
    jars = Path(pyspark.__file__).parent / "jars"
    scala_jar = next(jars.glob("scala-library-*.jar"))
    scala_binary = re.match(r"scala-library-(\d+\.\d+)", scala_jar.name).group(1)
    return f"org.apache.spark:spark-sql-kafka-0-10_{scala_binary}:{pyspark.__version__}"


def get_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", kafka_package())
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_topic(spark: SparkSession, topic: str, schema: StructType) -> DataFrame:
    """Stream a Kafka topic and parse its JSON values with `schema`."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", config.STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw.select(F.from_json(F.col("value").cast("string"), schema).alias("obj"))
        .select("obj.*")
    )


def parse_args(description: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Process everything currently in the topic, then stop (instead of running forever).",
    )
    return parser.parse_args()


def start(df: DataFrame, domain: str, process_batch, available_now: bool):
    """Start the streaming query writing through `process_batch`, then block."""
    writer = df.writeStream.foreachBatch(process_batch).option(
        "checkpointLocation", config.checkpoint_path(domain)
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=config.TRIGGER_INTERVAL)
    query = writer.start()
    query.awaitTermination()


def append_csv(batch_df: DataFrame, domain: str) -> None:
    batch_df.coalesce(1).write.mode("append").option("header", True).csv(
        config.output_path(domain)
    )
