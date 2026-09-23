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

# Spark consumers
STARTING_OFFSETS = _env("STARTING_OFFSETS", "earliest")
TRIGGER_INTERVAL = _env("TRIGGER_INTERVAL", "5 seconds")

# Outputs and checkpoints (one folder per domain)
DATA_ROOT = Path(_env("DATA_ROOT", str(REPO_ROOT / "data"))).expanduser()
if not DATA_ROOT.is_absolute():
    DATA_ROOT = (REPO_ROOT / DATA_ROOT).resolve()


def output_path(domain: str) -> str:
    return str(DATA_ROOT / "outputs" / domain)


def checkpoint_path(domain: str) -> str:
    return str(DATA_ROOT / "checkpoints" / domain)


# External sources
NEWSAPI_KEY = _env("NEWSAPI_KEY")
FOOTBALL_DATA_KEY = _env("FOOTBALL_DATA_KEY")
