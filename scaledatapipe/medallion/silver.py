"""Silver layer: typed, validated and deduplicated tables, one per domain.

For each domain, two streaming queries read Bronze:
- valid rows  -> silver/<domain>      (typed columns, business-key dedup)
- invalid rows -> quarantine/<domain> (raw value + reject_reason)
Rows are never dropped silently: every Bronze record lands in exactly one of
the two tables (minus duplicates, which Silver removes on purpose).
"""
from dataclasses import dataclass
from typing import Callable

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from scaledatapipe.common import config
from scaledatapipe.common.spark import with_trigger
from scaledatapipe.medallion import bronze

LINEAGE = ["topic", "partition", "offset", "kafka_ts"]

SPORT_STATUSES = [
    "SCHEDULED", "TIMED", "IN_PLAY", "PAUSED", "LIVE", "FINISHED",
    "SUSPENDED", "POSTPONED", "CANCELLED", "AWARDED",
]


@dataclass(frozen=True)
class Domain:
    name: str
    topic: str
    json_schema: StructType
    # obj (parsed JSON struct column) -> typed business columns, incl. event_date
    columns: Callable[[Column], dict[str, Column]]
    # -> (message, "row is invalid" condition) pairs, checked in order.
    # Callables because Columns can only be built once a SparkSession exists.
    rules: Callable[[], list[tuple[str, Column]]]
    keys: list[str]


# Words counted as COVID mentions. "COVID-19" splits into "covid" + "19";
# "covid19" is its hyphen-less spelling.
COVID_TERMS = ["covid", "covid19", "coronavirus"]


def _words(text: Column) -> Column:
    return F.split(F.regexp_replace(F.lower(text), r"[^a-z0-9\s]", " "), r"\s+")


COVID = Domain(
    name="covid",
    topic=config.TOPIC_COVID,
    json_schema=StructType([
        StructField("date", StringType()),
        StructField("source", StringType()),
        StructField("title", StringType()),
        StructField("text", StringType()),
        StructField("url", StringType()),
        StructField("published_at", StringType()),
        StructField("source_name", StringType()),
    ]),
    columns=lambda o: {
        # Fallback events have no URL: identify them by title instead.
        "article_id": F.sha2(F.coalesce(o.url, o.title), 256),
        "url": o.url,
        "title": F.trim(o.title),
        "text": o.text,
        "source_name": F.coalesce(o.source_name, o.source),
        "origin": o.source,  # "newsapi" or "fallback"
        "published_at": F.to_timestamp(o.published_at),
        "covid_mentions": F.size(F.filter(_words(F.coalesce(o.text, F.lit(""))),
                                          lambda w: w.isin(*COVID_TERMS))),
        "event_date": F.coalesce(F.to_date(F.to_timestamp(o.published_at)), F.to_date(o.date)),
    },
    rules=lambda: [
        ("missing title", F.col("title").isNull() | (F.col("title") == "")),
        ("missing date", F.col("event_date").isNull()),
    ],
    keys=["article_id"],
)

WEATHER = Domain(
    name="weather",
    topic=config.TOPIC_WEATHER,
    json_schema=StructType([
        StructField("source", StringType()),
        StructField("temperature", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("wind_speed", DoubleType()),
        StructField("observed_at", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
    ]),
    columns=lambda o: {
        "city": o.source,
        "latitude": o.latitude,
        "longitude": o.longitude,
        "observed_at": F.to_timestamp(o.observed_at),
        "temperature_c": o.temperature,
        "humidity_pct": o.humidity,
        "wind_speed_kmh": o.wind_speed,
        "event_date": F.to_date(F.to_timestamp(o.observed_at)),
    },
    rules=lambda: [
        ("missing city", F.col("city").isNull()),
        ("missing observed_at", F.col("observed_at").isNull()),
        ("temperature out of range", ~F.col("temperature_c").between(-90, 60)),
        ("humidity out of range", ~F.col("humidity_pct").between(0, 100)),
        ("negative wind speed", F.col("wind_speed_kmh") < 0),
    ],
    keys=["city", "observed_at"],
)

SPORT = Domain(
    name="sport",
    topic=config.TOPIC_SPORT,
    json_schema=StructType([
        StructField("source", StringType()),
        StructField("home", StringType()),
        StructField("away", StringType()),
        StructField("status", StringType()),
        StructField("match_id", LongType()),
        StructField("competition_code", StringType()),
        StructField("utc_kickoff", StringType()),
        StructField("matchday", IntegerType()),
        StructField("home_score", IntegerType()),
        StructField("away_score", IntegerType()),
    ]),
    columns=lambda o: {
        "match_id": o.match_id,
        "competition_code": o.competition_code,
        "competition": o.source,
        "kickoff_utc": F.to_timestamp(o.utc_kickoff),
        "matchday": o.matchday,
        "home_team": o.home,
        "away_team": o.away,
        "status": F.upper(o.status),
        "home_score": o.home_score,
        "away_score": o.away_score,
        "event_date": F.to_date(F.to_timestamp(o.utc_kickoff)),
    },
    rules=lambda: [
        ("missing match_id", F.col("match_id").isNull()),
        ("missing kickoff", F.col("kickoff_utc").isNull()),
        ("missing team", F.col("home_team").isNull() | F.col("away_team").isNull()),
        ("unknown status", ~F.col("status").isin(SPORT_STATUSES)),
        ("finished match without score",
         (F.col("status") == "FINISHED") & (F.col("home_score").isNull() | F.col("away_score").isNull())),
    ],
    # A match legitimately reappears when its status or score changes.
    keys=["match_id", "status", "home_score", "away_score"],
)

CYBER = Domain(
    name="cyber",
    topic=config.TOPIC_CYBER,
    json_schema=StructType([
        StructField("cve", StringType()),
        StructField("vendor", StringType()),
        StructField("product", StringType()),
        StructField("vulnerability_name", StringType()),
        StructField("date_added", StringType()),
        StructField("due_date", StringType()),
        StructField("known_ransomware_use", StringType()),
    ]),
    columns=lambda o: {
        "cve_id": F.upper(F.trim(o.cve)),
        "vendor": o.vendor,
        "product": o.product,
        "vulnerability_name": o.vulnerability_name,
        "date_added": F.to_date(o.date_added),
        "due_date": F.to_date(o.due_date),
        "known_ransomware_use": F.lower(o.known_ransomware_use) == "known",
        "event_date": F.to_date(o.date_added),
    },
    rules=lambda: [
        ("invalid cve_id", F.col("cve_id").isNull() | ~F.col("cve_id").rlike(r"^CVE-\d{4}-\d{4,}$")),
        ("missing date_added", F.col("date_added").isNull()),
    ],
    keys=["cve_id"],
)

DOMAINS = [COVID, WEATHER, SPORT, CYBER]


def path(domain: Domain) -> str:
    return config.layer_path("silver", domain.name)


def quarantine_path(domain: Domain) -> str:
    return config.layer_path("quarantine", domain.name)


def with_reject_reason(bronze_df: DataFrame, domain: Domain) -> DataFrame:
    """Parse and type one domain's Bronze rows, adding reject_reason (null = valid)."""
    parsed = bronze_df.filter(F.col("topic") == domain.topic).withColumn(
        "obj", F.from_json("value", domain.json_schema)
    )
    typed = parsed.select(
        *LINEAGE, "value", "ingest_date", "obj",
        *[col.alias(name) for name, col in domain.columns(F.col("obj")).items()],
    )
    # from_json yields a struct of nulls (not null) on malformed input.
    reasons = [F.when(F.get_json_object("value", "$").isNull(), F.lit("unparseable JSON"))]
    # A null condition (e.g. comparison on a null field) counts as valid here;
    # required fields have their own explicit isNull() rule.
    reasons += [F.when(F.coalesce(cond, F.lit(False)), F.lit(msg)) for msg, cond in domain.rules()]
    return typed.withColumn("reject_reason", F.coalesce(*reasons))


def valid_rows(checked: DataFrame, domain: Domain) -> DataFrame:
    business = list(domain.columns(F.col("obj")).keys())
    return (
        checked.filter(F.col("reject_reason").isNull())
        .select(*business, *LINEAGE)
        .withWatermark("kafka_ts", config.SILVER_DEDUP_WINDOW)
        .dropDuplicatesWithinWatermark(domain.keys)
    )


def rejected_rows(checked: DataFrame) -> DataFrame:
    return checked.filter(F.col("reject_reason").isNotNull()).select(
        *LINEAGE, "value", "reject_reason", "ingest_date"
    )


def _start(df: DataFrame, name: str, out: str, partition: str, available_now: bool):
    writer = (
        df.writeStream.queryName(name)
        .format("parquet")
        .option("path", out)
        .option("checkpointLocation", config.checkpoint_path(name))
        .partitionBy(partition)
        .outputMode("append")
    )
    return with_trigger(writer, available_now).start()


def start(spark: SparkSession, available_now: bool) -> list:
    source = bronze.read_stream(spark)
    queries = []
    for domain in DOMAINS:
        checked = with_reject_reason(source, domain)
        queries.append(_start(valid_rows(checked, domain), f"silver_{domain.name}",
                              path(domain), "event_date", available_now))
        queries.append(_start(rejected_rows(checked), f"quarantine_{domain.name}",
                              quarantine_path(domain), "ingest_date", available_now))
    return queries
