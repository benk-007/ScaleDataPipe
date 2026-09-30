"""Football producer (football-data.org, API key required).

Fetches the matches in a window around today: recent ones (to capture final
scores) and upcoming ones (fixtures). Re-running it picks up status and score
changes; Silver keeps each state and Gold the latest one.
"""
import sys
from datetime import date, timedelta

import requests

from scaledatapipe.common import config
from scaledatapipe.producers.base import send_all

COMPETITION_CODES = ["PL", "CL", "PD"]  # Premier League, Champions League, La Liga


def date_window(today: date) -> tuple[str, str]:
    return (
        (today - timedelta(days=config.SPORT_DAYS_BACK)).isoformat(),
        (today + timedelta(days=config.SPORT_DAYS_AHEAD)).isoformat(),
    )


def fetch_events(today: date | None = None) -> list[dict]:
    date_from, date_to = date_window(today or date.today())
    events = []
    for code in COMPETITION_CODES:
        try:
            r = requests.get(
                f"https://api.football-data.org/v4/competitions/{code}/matches",
                headers={"X-Auth-Token": config.FOOTBALL_DATA_KEY},
                params={"dateFrom": date_from, "dateTo": date_to},
                timeout=30,
            )
            if r.status_code != 200:
                print(f"API error for {code}: HTTP {r.status_code}")
                continue
            data = r.json()
            matches = data.get("matches", [])
            events += [
                {
                    "date": m["utcDate"][:10],
                    "source": data["competition"]["name"],
                    "home": m["homeTeam"]["name"],
                    "away": m["awayTeam"]["name"],
                    "status": m["status"],
                    "match_id": m["id"],
                    "competition_code": code,
                    "utc_kickoff": m["utcDate"],
                    "matchday": m.get("matchday"),
                    "home_score": m["score"]["fullTime"]["home"],
                    "away_score": m["score"]["fullTime"]["away"],
                }
                for m in matches
            ]
            print(f"Fetched {len(matches)} matches from {code} ({date_from} to {date_to})")
        except requests.RequestException as e:
            print(f"Failed to fetch football data for {code}: {e}")
    return events


def main():
    if not config.FOOTBALL_DATA_KEY:
        sys.exit("FOOTBALL_DATA_KEY is not set (see .env.example)")
    send_all(config.TOPIC_SPORT, fetch_events())


if __name__ == "__main__":
    main()
