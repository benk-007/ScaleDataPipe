"""Weather consumer: persists current weather observations per city."""
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from scaledatapipe.common import config
from scaledatapipe.common.spark import append_csv, get_spark, parse_args, read_topic, start

DOMAIN = "weather"

SCHEMA = StructType([
    StructField("date", StringType()),
    StructField("source", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("wind_speed", DoubleType()),
])


def process_batch(batch_df, batch_id: int):
    if batch_df.isEmpty():
        print(f"[weather batch {batch_id}] empty")
        return
    print(f"[weather batch {batch_id}]")
    batch_df.show(truncate=False)
    append_csv(batch_df, DOMAIN)


def main():
    args = parse_args(__doc__)
    spark = get_spark("WeatherConsumer")
    start(read_topic(spark, config.TOPIC_WEATHER, SCHEMA), DOMAIN, process_batch, args.available_now)


if __name__ == "__main__":
    main()
