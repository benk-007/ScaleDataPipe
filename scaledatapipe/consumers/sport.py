"""Sport consumer: persists football matches."""
from pyspark.sql.types import StringType, StructField, StructType

from scaledatapipe.common import config
from scaledatapipe.common.spark import append_csv, get_spark, parse_args, read_topic, start

DOMAIN = "sport"

SCHEMA = StructType([
    StructField("date", StringType()),
    StructField("source", StringType()),
    StructField("home", StringType()),
    StructField("away", StringType()),
    StructField("status", StringType()),
])


def process_batch(batch_df, batch_id: int):
    if batch_df.isEmpty():
        print(f"[sport batch {batch_id}] empty")
        return
    print(f"[sport batch {batch_id}]")
    batch_df.show(truncate=False)
    append_csv(batch_df, DOMAIN)


def main():
    args = parse_args(__doc__)
    spark = get_spark("SportConsumer")
    start(read_topic(spark, config.TOPIC_SPORT, SCHEMA), DOMAIN, process_batch, args.available_now)


if __name__ == "__main__":
    main()
