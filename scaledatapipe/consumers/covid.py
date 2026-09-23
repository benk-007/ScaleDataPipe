"""COVID consumer: daily frequency of the word "covid" in news texts."""
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from scaledatapipe.common import config
from scaledatapipe.common.spark import append_csv, get_spark, parse_args, read_topic, start

DOMAIN = "covid"

SCHEMA = StructType([
    StructField("date", StringType()),
    StructField("source", StringType()),
    StructField("title", StringType()),
    StructField("text", StringType()),
])


def transform(parsed):
    return (
        parsed
        .withColumn("date", F.coalesce("date", F.date_format(F.current_timestamp(), "yyyy-MM-dd")))
        .withColumn("text", F.coalesce("text", F.lit("")))
        .withColumn("words", F.split(F.regexp_replace(F.lower("text"), r"[^a-z0-9\s]", " "), r"\s+"))
        .withColumn("covid_count", F.size(F.filter("words", lambda w: w == "covid")))
        .select("date", "covid_count")
    )


def process_batch(batch_df, batch_id: int):
    if batch_df.isEmpty():
        print(f"[covid batch {batch_id}] empty")
        return
    out = (
        batch_df.groupBy("date")
        .agg(F.sum("covid_count").alias("covid_frequency"), F.count("*").alias("articles"))
        .orderBy("date")
    )
    print(f"[covid batch {batch_id}]")
    out.show(truncate=False)
    append_csv(out, DOMAIN)


def main():
    args = parse_args(__doc__)
    spark = get_spark("CovidConsumer")
    start(transform(read_topic(spark, config.TOPIC_COVID, SCHEMA)), DOMAIN, process_batch, args.available_now)


if __name__ == "__main__":
    main()
