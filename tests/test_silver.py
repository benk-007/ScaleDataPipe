"""Silver parsing, validation and deduplication on hand-made Bronze rows."""
import json
import uuid
from datetime import datetime

import pytest

from scaledatapipe.common import config
from scaledatapipe.medallion import bronze, silver


def bronze_stream(spark, tmp_path, topic, values):
    """Write hand-made rows as a Bronze-shaped Parquet table and stream it back,
    so tests go through the same streaming path as production."""
    rows = [
        (i, i, datetime(2026, 9, 24, 10, 0, i), None,
         v if isinstance(v, str) else json.dumps(v),
         datetime(2026, 9, 24, 10, 1), topic, datetime(2026, 9, 24).date())
        for i, v in enumerate(values)
    ]
    table = str(tmp_path / "bronze")
    spark.createDataFrame(rows, bronze.SCHEMA).coalesce(1).write.partitionBy("topic", "ingest_date").parquet(table)
    return spark.readStream.schema(bronze.SCHEMA).parquet(table)


def collect_stream(spark, df):
    name = f"t{uuid.uuid4().hex}"
    df.writeStream.format("memory").queryName(name).trigger(availableNow=True).start().awaitTermination()
    return spark.table(name).orderBy("offset").collect()


def reasons(spark, tmp_path, domain, values):
    checked = silver.with_reject_reason(bronze_stream(spark, tmp_path, domain.topic, values), domain)
    return [r.reject_reason for r in collect_stream(spark, checked)]


def valid(spark, tmp_path, domain, values):
    checked = silver.with_reject_reason(bronze_stream(spark, tmp_path, domain.topic, values), domain)
    return collect_stream(spark, silver.valid_rows(checked, domain))


WEATHER_OK = {"source": "Paris", "temperature": 21.5, "humidity": 40.0, "wind_speed": 8.0,
              "observed_at": "2026-09-24T10:00", "latitude": 48.85, "longitude": 2.35}


def test_weather_rules(spark, tmp_path):
    assert reasons(spark, tmp_path, silver.WEATHER, [
        WEATHER_OK,
        {**WEATHER_OK, "humidity": 140.0},
        {**WEATHER_OK, "temperature": -120.0},
        {k: v for k, v in WEATHER_OK.items() if k != "observed_at"},  # pre-Lot-3 producer format
        "not json at all",
    ]) == [None, "humidity out of range", "temperature out of range", "missing observed_at", "unparseable JSON"]


def test_weather_typing_and_dedup(spark, tmp_path):
    rows = valid(spark, tmp_path, silver.WEATHER, [WEATHER_OK, WEATHER_OK, {**WEATHER_OK, "observed_at": "2026-09-24T10:15"}])
    assert len(rows) == 2
    assert rows[0].observed_at == datetime(2026, 9, 24, 10, 0)
    assert str(rows[0].event_date) == "2026-09-24"
    assert rows[0].offset == 0  # first occurrence kept


SPORT_OK = {"source": "Premier League", "home": "Arsenal FC", "away": "Chelsea FC", "status": "TIMED",
            "match_id": 1, "competition_code": "PL", "utc_kickoff": "2026-09-27T14:00:00Z",
            "matchday": 6, "home_score": None, "away_score": None}


def test_sport_rules(spark, tmp_path):
    assert reasons(spark, tmp_path, silver.SPORT, [
        SPORT_OK,
        {**SPORT_OK, "status": "FINISHED"},
        {**SPORT_OK, "status": "WHATEVER"},
        {"date": "2026-09-24", "source": "Premier League", "home": "A", "away": "B", "status": "TIMED"},
    ]) == [None, "finished match without score", "unknown status", "missing match_id"]


def test_sport_keeps_state_changes(spark, tmp_path):
    finished = {**SPORT_OK, "status": "FINISHED", "home_score": 2, "away_score": 1}
    rows = valid(spark, tmp_path, silver.SPORT, [SPORT_OK, SPORT_OK, finished, finished])
    assert [(r.status, r.home_score) for r in rows] == [("TIMED", None), ("FINISHED", 2)]
    assert rows[0].kickoff_utc == datetime(2026, 9, 27, 14, 0)


CYBER_OK = {"cve": "CVE-2026-12345", "vendor": "Acme", "product": "Widget",
            "vulnerability_name": "Acme Widget RCE", "date_added": "2026-09-20",
            "due_date": "2026-10-11", "known_ransomware_use": "Known"}


def test_cyber_rules_and_typing(spark, tmp_path):
    assert reasons(spark, tmp_path, silver.CYBER, [
        CYBER_OK,
        {**CYBER_OK, "cve": "CVE-26-1"},
        {"date": "2026-09-24", "source": "cisa_kev", "cve": "CVE-2026-12345"},  # pre-Lot-3 format
    ]) == [None, "invalid cve_id", "missing date_added"]
    rows = valid(spark, tmp_path / "dedup", silver.CYBER, [CYBER_OK, {**CYBER_OK, "cve": " cve-2026-12345 "}])
    assert len(rows) == 1  # normalized id deduplicates
    assert rows[0].known_ransomware_use is True
    assert str(rows[0].event_date) == "2026-09-20"


def test_covid_mentions_and_fallback_ids(spark, tmp_path):
    article = {"date": "2026-09-24", "source": "newsapi", "title": "COVID update",
               "text": "COVID update: covid-19 cases, not covidiots", "url": "https://example.org/a",
               "published_at": "2026-09-23T08:00:00Z", "source_name": "Example"}
    fallback = {"date": "2026-09-24", "source": "fallback", "title": "new covid variant reported",
                "text": "new covid variant reported"}
    corona = {**article, "url": "https://example.org/b", "title": "Coronavirus wave",
              "text": "Coronavirus wave: coronavirus cases up, new COVID19 wing, coronaviruses studied"}
    rows = valid(spark, tmp_path, silver.COVID, [article, article, fallback, {**article, "title": ""}, corona])
    assert len(rows) == 3  # duplicate dropped, empty title rejected
    assert rows[0].covid_mentions == 2  # "covid", "covid-19" -> "covid 19"; not "covidiots"
    assert rows[2].covid_mentions == 3  # 2 x "coronavirus" + "covid19"; not "coronaviruses"
    assert str(rows[0].event_date) == "2026-09-23"  # published date wins over ingestion date
    assert rows[1].origin == "fallback" and rows[1].article_id is not None


@pytest.mark.parametrize("domain", silver.DOMAINS, ids=lambda d: d.name)
def test_every_domain_has_its_own_topic(domain):
    assert domain.topic in config.TOPICS
