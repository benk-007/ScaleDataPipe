"""Row counts and data-quality summary of the Bronze, Silver, quarantine and Gold tables.

    docker compose run --rm app submit scripts/lake_report.py
"""
from pyspark.sql import functions as F

from scaledatapipe.common.spark import get_spark, path_exists, read_parquet
from scaledatapipe.medallion import bronze, gold, silver


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
        valid = read_parquet(spark, silver.path(domain))
        n_valid = valid.count() if valid is not None else 0
        dup_keys = (valid.groupBy(*domain.keys).count().filter("count > 1").count()
                    if valid is not None else 0)
        rejected = read_parquet(spark, silver.quarantine_path(domain))
        n_rejected = rejected.count() if rejected is not None else 0
        print(f"{domain.name:<8} silver={n_valid:<4} quarantine={n_rejected:<4} duplicate keys in silver={dup_keys}")
        if rejected is not None and n_rejected:
            for row in rejected.groupBy("reject_reason").count().orderBy(F.desc("count")).collect():
                print(f"{'':<9}- {row.reject_reason}: {row['count']}")

    print("\n=== Gold")
    for table in gold.TABLES:
        df = read_parquet(spark, gold.path(table))
        if df is None:
            print(f"{table.name:<22} missing")
            continue
        computed_at = df.agg(F.max("computed_at")).first()[0]
        print(f"{table.name:<22} rows={df.count():<4} computed_at={computed_at}")


if __name__ == "__main__":
    main()
