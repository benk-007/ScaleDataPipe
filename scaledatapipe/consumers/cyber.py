"""Cybersecurity consumer: persists exploited CVE events."""
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from scaledatapipe.common import config
from scaledatapipe.common.spark import append_csv, get_spark, parse_args, read_topic, start

DOMAIN = "cyber"

SCHEMA = StructType([
    StructField("date", StringType()),
    StructField("source", StringType()),
    StructField("cve", StringType()),
])


def process_batch(batch_df, batch_id: int):
    if batch_df.isEmpty():
        print(f"[cyber batch {batch_id}] empty")
        return
    print(f"[cyber batch {batch_id}]")
    batch_df.show(truncate=False)
    append_csv(batch_df, DOMAIN)


def main():
    args = parse_args(__doc__)
    spark = get_spark("CyberConsumer")
    parsed = read_topic(spark, config.TOPIC_CYBER, SCHEMA).withColumn(
        "date", F.coalesce("date", F.date_format(F.current_timestamp(), "yyyy-MM-dd"))
    )
    start(parsed, DOMAIN, process_batch, args.available_now)


if __name__ == "__main__":
    main()
