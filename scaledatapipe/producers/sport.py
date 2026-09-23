"""Football producer (football-data.org, API key required)."""
import sys

import requests

from scaledatapipe.common import config
from scaledatapipe.producers.base import send_all

COMPETITION_CODES = ["PL", "CL", "PD"]  # Premier League, Champions League, La Liga
MATCHES_PER_COMPETITION = 10


def fetch_events() -> list[dict]:
    events = []
    for code in COMPETITION_CODES:
        try:
            r = requests.get(
                f"https://api.football-data.org/v4/competitions/{code}/matches",
                headers={"X-Auth-Token": config.FOOTBALL_DATA_KEY},
                timeout=30,
            )
            if r.status_code != 200:
                print(f"API error for {code}: HTTP {r.status_code}")
                continue
            data = r.json()
            matches = data.get("matches", [])[:MATCHES_PER_COMPETITION]
            events += [
                {
                    "date": m["utcDate"][:10],
                    "source": data["competition"]["name"],
                    "home": m["homeTeam"]["name"],
                    "away": m["awayTeam"]["name"],
                    "status": m["status"],
                }
                for m in matches
            ]
            print(f"Fetched {len(matches)} matches from {code}")
        except requests.RequestException as e:
            print(f"Failed to fetch football data for {code}: {e}")
    return events


def main():
    if not config.FOOTBALL_DATA_KEY:
        sys.exit("FOOTBALL_DATA_KEY is not set (see .env.example)")
    send_all(config.TOPIC_SPORT, fetch_events())


if __name__ == "__main__":
    main()
