"""Weather producer (Open-Meteo, no API key required)."""
import requests

from scaledatapipe.common import config
from scaledatapipe.producers.base import send_all, today

LOCATIONS = [
    {"city": "Casablanca", "lat": 33.57, "lon": -7.58},
    {"city": "New York", "lat": 40.71, "lon": -74.00},
    {"city": "London", "lat": 51.50, "lon": -0.12},
    {"city": "Tokyo", "lat": 35.68, "lon": 139.65},
    {"city": "Paris", "lat": 48.85, "lon": 2.35},
]


def fetch_events() -> list[dict]:
    date = today()
    events = []
    for loc in LOCATIONS:
        try:
            r = requests.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": loc["lat"],
                    "longitude": loc["lon"],
                    "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
                },
                timeout=10,
            )
            r.raise_for_status()
            curr = r.json()["current"]
            events.append(
                {
                    "date": date,
                    "source": loc["city"],
                    "temperature": float(curr["temperature_2m"]),
                    "humidity": float(curr["relative_humidity_2m"]),
                    "wind_speed": float(curr["wind_speed_10m"]),
                    "observed_at": curr["time"],  # UTC (Open-Meteo default timezone)
                    "latitude": loc["lat"],
                    "longitude": loc["lon"],
                }
            )
        except (requests.RequestException, KeyError, ValueError) as e:
            print(f"Error fetching {loc['city']}: {e}")
    return events


def main():
    send_all(config.TOPIC_WEATHER, fetch_events())


if __name__ == "__main__":
    main()
