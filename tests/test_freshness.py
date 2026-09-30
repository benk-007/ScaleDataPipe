"""Catch-up logic of the Airflow sensor (checkpoint parsing and comparisons)."""
from scaledatapipe.orchestration import freshness

OFFSETS_LOG = """v1
{"batchWatermarkMs":0,"batchTimestampMs":1727690000000,"conf":{"spark.sql.shuffle.partitions":"4"}}
{"covid_topic":{"0":38},"weather_topic":{"0":10},"sport_topic":{"0":93,"1":4}}
"""


def test_parse_offsets_log():
    assert freshness.parse_offsets_log(OFFSETS_LOG) == {
        ("covid_topic", 0): 38, ("weather_topic", 0): 10, ("sport_topic", 0): 93, ("sport_topic", 1): 4,
    }


def test_lagging_partitions():
    end = {("covid_topic", 0): 40, ("weather_topic", 0): 10, ("cyber_topic", 0): 5}
    committed = {("covid_topic", 0): 38, ("weather_topic", 0): 10}
    assert freshness.lagging_partitions(end, committed) == ["covid_topic[0] 38/40", "cyber_topic[0] 0/5"]
    assert freshness.lagging_partitions(end, {**committed, ("covid_topic", 0): 40, ("cyber_topic", 0): 5}) == []


def test_stale_queries():
    assert freshness.stale_queries(1000, {"silver_covid": 1500, "silver_sport": 900,
                                          "quarantine_sport": None}) == ["quarantine_sport", "silver_sport"]


def test_silver_queries_cover_every_domain():
    assert len(freshness.silver_queries()) == 8
    assert "quarantine_cyber" in freshness.silver_queries()


def test_caught_up_flow(monkeypatch):
    commits = {"bronze": (7, 1000), **{q: (3, 2000) for q in freshness.silver_queries()}}
    monkeypatch.setattr(freshness, "latest_commit", lambda q: commits[q])
    monkeypatch.setattr(freshness, "committed_offsets", lambda q, b: {("covid_topic", 0): 38})
    monkeypatch.setattr(freshness, "kafka_end_offsets", lambda: {("covid_topic", 0): 40})
    ok, why = freshness.caught_up()
    assert not ok and "covid_topic[0] 38/40" in why

    monkeypatch.setattr(freshness, "kafka_end_offsets", lambda: {("covid_topic", 0): 38})
    commits["silver_weather"] = (2, 900)  # committed before Bronze's last batch
    ok, why = freshness.caught_up()
    assert not ok and "silver_weather" in why

    commits["silver_weather"] = (3, 2000)
    assert freshness.caught_up() == (True, "caught up at Bronze batch 7")
