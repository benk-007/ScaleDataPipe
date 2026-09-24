"""Row counts and data-quality summary of the Bronze / Silver / quarantine tables.

    docker compose run --rm app submit scripts/lake_report.py
"""
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F

from scaledatapipe.common.spark import get_spark, path_exists
from scaledatapipe.medallion import bronze, silver


def read_table(spark, path):
    """Parquet table, or None if missing or still empty (no data file committed yet)."""
    if not path_exists(spark, path):
        return None
    try:
        return spark.read.parquet(path)
    except AnalysisException:  # only _spark_metadata so far: schema cannot be inferred
        return None


def main():
    spark = get_spark("LakeReport")
    spark.sparkContext.setLogLevel("ERROR")

    print(f"\n=== Bronze ({bronze.path()})")
    if not path_exists(spark, bronze.path()):
        print("missing")
        return
    spark.read.parquet(bronze.path()).groupBy("topic").count().orderBy("topic").show(truncate=False)

    print("=== Silver / quarantine")
    for domain in silver.DOMAINS:
        valid = read_table(spark, silver.path(domain))
        n_valid = valid.count() if valid is not None else 0
        dup_keys = (valid.groupBy(*domain.keys).count().filter("count > 1").count()
                    if valid is not None else 0)
        rejected = read_table(spark, silver.quarantine_path(domain))
        n_rejected = rejected.count() if rejected is not None else 0
        print(f"{domain.name:<8} silver={n_valid:<4} quarantine={n_rejected:<4} duplicate keys in silver={dup_keys}")
        if rejected is not None and n_rejected:
            for row in rejected.groupBy("reject_reason").count().orderBy(F.desc("count")).collect():
                print(f"{'':<9}- {row.reject_reason}: {row['count']}")


if __name__ == "__main__":
    main()
