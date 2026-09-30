"""Central configuration, read from environment variables (and .env if present).

No secret is hardcoded here: API keys must come from the environment.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(REPO_ROOT / ".env")


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


# Kafka
KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP", "localhost:9092")

TOPIC_COVID = "covid_topic"
TOPIC_WEATHER = "weather_topic"
TOPIC_SPORT = "sport_topic"
TOPIC_CYBER = "cyber_topic"
TOPICS = [TOPIC_COVID, TOPIC_WEATHER, TOPIC_SPORT, TOPIC_CYBER]

# Streaming jobs
STARTING_OFFSETS = _env("STARTING_OFFSETS", "earliest")
TRIGGER_INTERVAL = _env("TRIGGER_INTERVAL", "5 seconds")

# Root of the data lake and checkpoints.
# Either a local path or a URI such as hdfs://namenode:8020/scaledatapipe
def _resolve_data_root(value: str) -> str:
    if "://" in value:
        return value.rstrip("/")
    path = Path(value).expanduser()
    return str(path if path.is_absolute() else (REPO_ROOT / path).resolve())


DATA_ROOT = _resolve_data_root(_env("DATA_ROOT", str(REPO_ROOT / "data")))


def checkpoint_path(query: str) -> str:
    return f"{DATA_ROOT}/checkpoints/{query}"


# Medallion layers: {DATA_ROOT}/bronze/events, {DATA_ROOT}/silver/<domain>, ...
def layer_path(layer: str, table: str) -> str:
    return f"{DATA_ROOT}/{layer}/{table}"


# Silver drops a duplicate only if it arrives within this window of the first
# occurrence (bounded streaming state); Gold deduplicates fully in batch.
SILVER_DEDUP_WINDOW = _env("SILVER_DEDUP_WINDOW", "7 days")


# External sources
NEWSAPI_KEY = _env("NEWSAPI_KEY")
FOOTBALL_DATA_KEY = _env("FOOTBALL_DATA_KEY")

# Sport producer: matches fetched in [today - BACK, today + AHEAD]. Fixture
# breaks (international windows) can leave a narrower window empty.
SPORT_DAYS_BACK = int(_env("SPORT_DAYS_BACK", "14"))
SPORT_DAYS_AHEAD = int(_env("SPORT_DAYS_AHEAD", "14"))
