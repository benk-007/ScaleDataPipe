"""Cybersecurity producer (CISA Known Exploited Vulnerabilities feed, no key)."""
import requests

from scaledatapipe.common import config
from scaledatapipe.producers.base import send_all, today

KEV_URL = "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"
MAX_EVENTS = 10


def fetch_events() -> list[dict]:
    r = requests.get(KEV_URL, timeout=30)
    r.raise_for_status()
    date = today()
    return [
        {"date": date, "source": "cisa_kev", "cve": v.get("cveID", "")}
        for v in r.json().get("vulnerabilities", [])[:MAX_EVENTS]
    ]


def main():
    send_all(config.TOPIC_CYBER, fetch_events())


if __name__ == "__main__":
    main()
