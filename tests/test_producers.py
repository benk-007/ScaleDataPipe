"""Producers, with the HTTP APIs stubbed out."""
from datetime import date

from scaledatapipe.common import config
from scaledatapipe.producers import sport


class FakeResponse:
    status_code = 200

    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def fake_match(match_id, status="TIMED"):
    return {"id": match_id, "utcDate": "2026-10-10T14:00:00Z", "status": status, "matchday": 7,
            "homeTeam": {"name": "Home FC"}, "awayTeam": {"name": "Away FC"},
            "score": {"fullTime": {"home": None, "away": None}}}


def test_sport_queries_a_window_around_today_without_capping(monkeypatch):
    calls = []

    def fake_get(url, headers, params, timeout):
        calls.append((url, params))
        return FakeResponse({"competition": {"name": "Some League"},
                             "matches": [fake_match(i) for i in range(25)]})

    monkeypatch.setattr(sport.requests, "get", fake_get)
    monkeypatch.setattr(config, "SPORT_DAYS_BACK", 14)
    monkeypatch.setattr(config, "SPORT_DAYS_AHEAD", 14)
    events = sport.fetch_events(today=date(2026, 9, 30))

    assert [params for _, params in calls] == [{"dateFrom": "2026-09-16", "dateTo": "2026-10-14"}] * 3
    assert [url.rsplit("/", 2)[1] for url, _ in calls] == sport.COMPETITION_CODES
    assert len(events) == 75  # 25 per competition: no more "first 10 of the season" cap
    assert events[0]["match_id"] == 0 and events[0]["competition_code"] == "PL"
