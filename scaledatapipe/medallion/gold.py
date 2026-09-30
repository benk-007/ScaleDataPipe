"""Gold layer: business aggregates, recomputed in batch from Silver.

    submit scaledatapipe/medallion/gold.py                  # every table
    submit scaledatapipe/medallion/gold.py --table covid_daily

Each run rebuilds the tables from the whole of Silver (idempotent, fine at this
data volume). Silver only deduplicates within its watermark window, so Gold
first keeps the latest record per business key. Tables are written to a
temporary directory and then swapped in, so readers never see a partially
written table (at worst, none for the instant of the swap).
"""
import argparse
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from scaledatapipe.common import config
from scaledatapipe.common.spark import get_spark, read_parquet, replace_dir
from scaledatapipe.medallion import silver


def latest(df: DataFrame, keys: list[str]) -> DataFrame:
    """Keep the most recently ingested record per key (Kafka order breaks ties)."""
    w = Window.partitionBy(*keys).orderBy(F.col("kafka_ts").desc(), F.col("offset").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def covid_daily(covid: DataFrame) -> DataFrame:
    articles = latest(covid, ["article_id"])
    mentioning = F.col("covid_mentions") > 0
    return (
        articles.groupBy("event_date")
        .agg(
            F.count("*").alias("articles"),
            F.count(F.when(mentioning, True)).alias("articles_mentioning_covid"),
            F.sum("covid_mentions").cast("long").alias("covid_mentions"),
            F.countDistinct("source_name").alias("sources"),
        )
        .withColumn("mention_share", F.round(F.col("articles_mentioning_covid") / F.col("articles"), 3))
    )


def weather_city_daily(weather: DataFrame) -> DataFrame:
    observations = latest(weather, ["city", "observed_at"])
    return (
        observations.groupBy("city", "event_date")
        .agg(
            F.count("*").alias("observations"),
            F.round(F.min("temperature_c"), 1).alias("temp_min_c"),
            F.round(F.avg("temperature_c"), 1).alias("temp_avg_c"),
            F.round(F.max("temperature_c"), 1).alias("temp_max_c"),
            F.round(F.avg("humidity_pct"), 1).alias("humidity_avg_pct"),
            F.round(F.max("wind_speed_kmh"), 1).alias("wind_max_kmh"),
            F.max("observed_at").alias("last_observed_at"),
            F.max_by("temperature_c", "observed_at").alias("last_temperature_c"),
        )
    )


def sport_matches(sport: DataFrame) -> DataFrame:
    """Current state of each match: its most recently ingested record."""
    result = (
        F.when(F.col("status") != "FINISHED", F.lit(None))
        .when(F.col("home_score") > F.col("away_score"), "HOME_WIN")
        .when(F.col("home_score") < F.col("away_score"), "AWAY_WIN")
        .otherwise("DRAW")
    )
    return latest(sport, ["match_id"]).select(
        "match_id", "competition_code", "competition", "matchday", "kickoff_utc", "event_date",
        "home_team", "away_team", "status", "home_score", "away_score", result.alias("result"),
    )


def sport_team_stats(sport: DataFrame) -> DataFrame:
    """Per-team record over the finished matches ingested so far (not an official table)."""
    finished = sport_matches(sport).filter(F.col("status") == "FINISHED")
    sides = [
        finished.select("competition_code", "competition", F.col("home_team").alias("team"),
                        F.col("home_score").alias("goals_for"), F.col("away_score").alias("goals_against")),
        finished.select("competition_code", "competition", F.col("away_team").alias("team"),
                        F.col("away_score").alias("goals_for"), F.col("home_score").alias("goals_against")),
    ]
    per_team = sides[0].unionByName(sides[1])
    won, drawn = F.col("goals_for") > F.col("goals_against"), F.col("goals_for") == F.col("goals_against")
    return (
        per_team.groupBy("competition_code", "competition", "team")
        .agg(
            F.count("*").alias("played"),
            F.count(F.when(won, True)).alias("wins"),
            F.count(F.when(drawn, True)).alias("draws"),
            F.count(F.when(F.col("goals_for") < F.col("goals_against"), True)).alias("losses"),
            F.sum("goals_for").cast("int").alias("goals_for"),
            F.sum("goals_against").cast("int").alias("goals_against"),
        )
        .withColumn("goal_diff", F.col("goals_for") - F.col("goals_against"))
        .withColumn("points", 3 * F.col("wins") + F.col("draws"))
    )


def cyber_vendor_exposure(cyber: DataFrame) -> DataFrame:
    cves = latest(cyber, ["cve_id"])
    return (
        cves.groupBy("vendor")
        .agg(
            F.count("*").alias("cves"),
            F.count(F.when(F.col("known_ransomware_use"), True)).alias("ransomware_linked_cves"),
            F.countDistinct("product").alias("products"),
            F.min("date_added").alias("first_added"),
            F.max("date_added").alias("last_added"),
            F.min(F.when(F.col("due_date") >= F.current_date(), F.col("due_date"))).alias("next_due_date"),
        )
    )


@dataclass(frozen=True)
class GoldTable:
    name: str
    source: silver.Domain
    build: Callable[[DataFrame], DataFrame]


TABLES = [
    GoldTable("covid_daily", silver.COVID, covid_daily),
    GoldTable("weather_city_daily", silver.WEATHER, weather_city_daily),
    GoldTable("sport_matches", silver.SPORT, sport_matches),
    GoldTable("sport_team_stats", silver.SPORT, sport_team_stats),
    GoldTable("cyber_vendor_exposure", silver.CYBER, cyber_vendor_exposure),
]


def path(table: GoldTable) -> str:
    return config.layer_path("gold", table.name)


def refresh(spark: SparkSession, table: GoldTable, computed_at: str) -> int | None:
    """Rebuild one Gold table; returns its row count, or None if Silver has no data yet."""
    source = read_parquet(spark, silver.path(table.source))
    if source is None:
        return None
    # A UTC string cast in the (UTC) session: a Python datetime would be shifted
    # by the local timezone of the driver process.
    df = table.build(source).withColumn("computed_at", F.lit(computed_at).cast("timestamp"))
    tmp = f"{path(table)}.__tmp"
    df.write.mode("overwrite").parquet(tmp)
    replace_dir(spark, tmp, path(table))
    return spark.read.parquet(path(table)).count()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--table", choices=[t.name for t in TABLES], action="append",
                        help="Table to refresh (repeatable); default: all")
    args = parser.parse_args()

    spark = get_spark("Gold")
    computed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")  # same for every table of a run
    missing = []
    for table in TABLES:
        if args.table and table.name not in args.table:
            continue
        rows = refresh(spark, table, computed_at)
        if rows is None:
            missing.append(table.name)
            print(f"[gold] {table.name}: skipped, no Silver {table.source.name} data yet")
        else:
            print(f"[gold] {table.name}: {rows} rows -> {path(table)}")
    spark.stop()
    # Nothing to build at all is an error worth surfacing to the scheduler.
    if missing and len(missing) == len(args.table or TABLES):
        sys.exit("[gold] no Silver data available")


if __name__ == "__main__":
    main()
