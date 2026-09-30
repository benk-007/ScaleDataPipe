"""Spark helpers shared by the Medallion jobs."""
import os
import re
from pathlib import Path

import pyspark
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from scaledatapipe.common import config


def _spark_jars_dir() -> Path:
    candidates = [Path(pyspark.__file__).parent / "jars"]
    if os.getenv("SPARK_HOME"):
        candidates.insert(0, Path(os.environ["SPARK_HOME"]) / "jars")
    for jars in candidates:
        if any(jars.glob("scala-library-*.jar")):
            return jars
    raise RuntimeError(f"Spark jars directory not found in {candidates}")


def kafka_package() -> str | None:
    """Kafka connector coordinates matching the running Spark, or None if the
    connector is already on the classpath (as in the Docker image).

    Derived from the Scala library bundled with Spark, so the connector can
    never drift from the Spark/Scala version actually running.
    """
    jars = _spark_jars_dir()
    if any(jars.glob("spark-sql-kafka-0-10_*.jar")):
        return None
    scala_jar = next(jars.glob("scala-library-*.jar"))
    scala_binary = re.match(r"scala-library-(\d+\.\d+)", scala_jar.name).group(1)
    return f"org.apache.spark:spark-sql-kafka-0-10_{scala_binary}:{pyspark.__version__}"


def get_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
    )
    package = kafka_package()
    if package:
        builder = builder.config("spark.jars.packages", package)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def with_trigger(writer, available_now: bool):
    """availableNow drains what is there and stops; otherwise micro-batch forever."""
    if available_now:
        return writer.trigger(availableNow=True)
    return writer.trigger(processingTime=config.TRIGGER_INTERVAL)


def _hadoop_path(spark: SparkSession, path: str):
    jvm = spark.sparkContext._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    return hpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), hpath


def path_exists(spark: SparkSession, path: str) -> bool:
    """Existence check on any Hadoop filesystem (local or hdfs://)."""
    fs, hpath = _hadoop_path(spark, path)
    return fs.exists(hpath)


def read_parquet(spark: SparkSession, path: str) -> DataFrame | None:
    """Parquet table, or None if missing or still empty (no data file committed yet)."""
    if not path_exists(spark, path):
        return None
    try:
        return spark.read.parquet(path)
    except AnalysisException:  # e.g. only _spark_metadata so far: no schema to infer
        return None


def replace_dir(spark: SparkSession, src: str, dst: str) -> None:
    """Move src onto dst, replacing it. Readers only ever see the old or the new
    table, apart from the instant between the delete and the rename."""
    fs, src_path = _hadoop_path(spark, src)
    _, dst_path = _hadoop_path(spark, dst)
    if fs.exists(dst_path) and not fs.delete(dst_path, True):
        raise IOError(f"Could not delete {dst}")
    if not fs.rename(src_path, dst_path):
        raise IOError(f"Could not rename {src} to {dst}")

