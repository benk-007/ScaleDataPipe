"""Gold aggregates on hand-made Silver rows."""
from datetime import date, datetime, timedelta

from scaledatapipe.medallion import gold

T0 = datetime(2026, 9, 24, 10, 0)


def silver_df(spark, rows):
    """Rows as dicts; adds Kafka lineage (kafka_ts/offset increase with list order)."""
    rows = [{**r, "kafka_ts": T0 + timedelta(minutes=i), "offset": i} for i, r in enumerate(rows)]
    return spark.createDataFrame(rows)


def by(rows, *keys):
    return {tuple(r[k] for k in keys) if len(keys) > 1 else r[keys[0]]: r for r in rows}


def match(match_id, status, home_score=None, away_score=None, home="A", away="B"):
    return {"match_id": match_id, "competition_code": "PL", "competition": "Premier League",
            "matchday": 1, "kickoff_utc": datetime(2026, 9, 20, 14), "event_date": date(2026, 9, 20),
            "home_team": home, "away_team": away, "status": status,
            "home_score": home_score, "away_score": away_score}


def test_sport_matches_keeps_latest_state(spark):
    rows = gold.sport_matches(silver_df(spark, [
        match(1, "TIMED"), match(1, "FINISHED", 2, 1),   # progression: latest wins
        match(2, "FINISHED", 0, 0), match(2, "FINISHED", 0, 0),  # duplicate that escaped Silver
        match(3, "FINISHED", 0, 3),
        match(4, "TIMED"),
    ])).collect()
    m = by(rows, "match_id")
    assert len(rows) == 4
    assert (m[1].status, m[1].result) == ("FINISHED", "HOME_WIN")
    assert m[2].result == "DRAW" and m[3].result == "AWAY_WIN" and m[4].result is None


def test_sport_team_stats(spark):
    rows = gold.sport_team_stats(silver_df(spark, [
        match(1, "TIMED", home="Arsenal", away="Chelsea"),
        match(1, "FINISHED", 2, 1, home="Arsenal", away="Chelsea"),
        match(2, "FINISHED", 1, 1, home="Chelsea", away="Arsenal"),
        match(3, "TIMED", home="Arsenal", away="Spurs"),  # not played yet: ignored
    ])).collect()
    t = by(rows, "team")
    assert set(t) == {"Arsenal", "Chelsea"}
    a = t["Arsenal"]
    assert (a.played, a.wins, a.draws, a.losses, a.goals_for, a.goals_against, a.goal_diff, a.points) == \
        (2, 1, 1, 0, 3, 2, 1, 4)
    assert (t["Chelsea"].points, t["Chelsea"].goal_diff) == (1, -1)


def test_covid_daily(spark):
    def article(article_id, mentions, source="Reuters", day=date(2026, 9, 23)):
        return {"article_id": article_id, "covid_mentions": mentions, "source_name": source, "event_date": day}
    rows = gold.covid_daily(silver_df(spark, [
        article("a", 2), article("a", 2),  # duplicate
        article("b", 0, "BBC"), article("c", 1, "BBC"),
        article("d", 5, day=date(2026, 9, 24)),
    ])).collect()
    d = by(rows, "event_date")
    day = d[date(2026, 9, 23)]
    assert (day.articles, day.articles_mentioning_covid, day.covid_mentions, day.sources) == (3, 2, 3, 2)
    assert day.mention_share == 0.667
    assert d[date(2026, 9, 24)].articles == 1


def test_weather_city_daily(spark):
    def obs(city, hour, temp, humidity=50.0, wind=10.0):
        return {"city": city, "observed_at": datetime(2026, 9, 24, hour), "event_date": date(2026, 9, 24),
                "temperature_c": temp, "humidity_pct": humidity, "wind_speed_kmh": wind}
    rows = gold.weather_city_daily(silver_df(spark, [
        obs("Paris", 12, 20.0, wind=5.0), obs("Paris", 8, 12.0, humidity=90.0, wind=20.0),
        obs("Paris", 12, 20.0, wind=5.0),  # duplicate observation
        obs("Tokyo", 9, 25.0),
    ])).collect()
    p = by(rows, "city")["Paris"]
    assert (p.observations, p.temp_min_c, p.temp_avg_c, p.temp_max_c) == (2, 12.0, 16.0, 20.0)
    assert (p.humidity_avg_pct, p.wind_max_kmh) == (70.0, 20.0)
    assert p.last_observed_at == datetime(2026, 9, 24, 12) and p.last_temperature_c == 20.0  # latest by time, not by ingestion


def test_cyber_vendor_exposure(spark):
    def cve(cve_id, vendor, product, ransomware, added, due):
        return {"cve_id": cve_id, "vendor": vendor, "product": product, "known_ransomware_use": ransomware,
                "date_added": added, "due_date": due}
    past, future, later = date(2020, 1, 1), date(2099, 1, 1), date(2099, 6, 1)
    rows = gold.cyber_vendor_exposure(silver_df(spark, [
        cve("CVE-2026-0001", "Acme", "Widget", True, date(2026, 9, 1), later),
        cve("CVE-2026-0002", "Acme", "Gadget", False, date(2026, 9, 5), future),
        cve("CVE-2026-0002", "Acme", "Gadget", False, date(2026, 9, 5), future),  # duplicate
        cve("CVE-2019-0003", "Acme", "Widget", False, date(2019, 12, 1), past),
        cve("CVE-2026-0004", "Other", "Thing", False, date(2026, 9, 2), past),
    ])).collect()
    v = by(rows, "vendor")
    acme = v["Acme"]
    assert (acme.cves, acme.ransomware_linked_cves, acme.products) == (3, 1, 2)
    assert (acme.first_added, acme.last_added) == (date(2019, 12, 1), date(2026, 9, 5))
    assert acme.next_due_date == future  # earliest due date not yet passed
    assert v["Other"].next_due_date is None  # all deadlines passed


def test_refresh_swaps_table_in_place(spark, tmp_path, monkeypatch):
    """refresh() writes Gold next to a temp dir, swaps it in, and is idempotent."""
    from scaledatapipe.common import config
    from scaledatapipe.medallion import silver
    monkeypatch.setattr(config, "DATA_ROOT", str(tmp_path))
    silver_df(spark, [match(1, "FINISHED", 1, 0), match(2, "TIMED")]) \
        .write.parquet(silver.path(silver.SPORT))
    table = next(t for t in gold.TABLES if t.name == "sport_matches")
    assert gold.refresh(spark, table, "2026-09-24 10:00:00") == 2
    assert gold.refresh(spark, table, "2026-09-24 11:00:00") == 2  # rerun replaces, never appends
    out = spark.read.parquet(gold.path(table)).collect()
    assert {r.computed_at for r in out} == {datetime(2026, 9, 24, 11)}
    assert not (tmp_path / "gold" / "sport_matches.__tmp").exists()
    missing = next(t for t in gold.TABLES if t.name == "covid_daily")
    assert gold.refresh(spark, missing, "2026-09-24 11:00:00") is None  # no Silver data: skipped
