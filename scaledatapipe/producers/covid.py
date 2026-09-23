"""COVID news producer (NewsAPI, with a fallback when the API is unavailable)."""
import requests

from scaledatapipe.common import config
from scaledatapipe.producers.base import send_all, today

FALLBACK_TEXTS = [
    "covid cases rising in some regions",
    "new covid variant reported",
    "public health covid recommendations updated",
    "covid vaccination campaign continues",
]


def fetch_events() -> list[dict]:
    date = today()
    if config.NEWSAPI_KEY:
        try:
            r = requests.get(
                "https://newsapi.org/v2/everything",
                params={
                    "q": "covid OR coronavirus",
                    "language": "en",
                    "pageSize": 20,
                    "apiKey": config.NEWSAPI_KEY,
                },
                timeout=30,
            )
            r.raise_for_status()
            articles = r.json().get("articles", [])
            if articles:
                return [
                    {
                        "date": date,
                        "source": "newsapi",
                        "title": a.get("title") or "",
                        "text": f"{a.get('title') or ''} {a.get('description') or ''}".strip(),
                    }
                    for a in articles
                ]
            print("NewsAPI returned no articles, using fallback events")
        except requests.RequestException as e:
            print(f"NewsAPI request failed ({type(e).__name__}), using fallback events")
    else:
        print("NEWSAPI_KEY not set, using fallback events")

    return [{"date": date, "source": "fallback", "title": t, "text": t} for t in FALLBACK_TEXTS]


def main():
    send_all(config.TOPIC_COVID, fetch_events())


if __name__ == "__main__":
    main()
