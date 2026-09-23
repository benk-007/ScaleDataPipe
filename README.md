# ScaleDataPipe

Real-time data pipeline: Python producers pull four public sources into Apache Kafka,
and Spark Structured Streaming consumers process each topic.

| Domain  | Source                                   | Kafka topic     |
|---------|------------------------------------------|-----------------|
| COVID   | [NewsAPI](https://newsapi.org) (key)     | `covid_topic`   |
| Weather | [Open-Meteo](https://open-meteo.com)     | `weather_topic` |
| Sport   | [football-data.org](https://www.football-data.org) (key) | `sport_topic` |
| Cyber   | [CISA KEV feed](https://www.cisa.gov/known-exploited-vulnerabilities-catalog) | `cyber_topic` |

> Work in progress: Medallion storage (Bronze/Silver/Gold) on HDFS, a full
> `docker-compose.yml` and Airflow orchestration are the next milestones.
> Consumers currently write CSV files locally.

## Layout

```
scaledatapipe/
├── common/
│   ├── config.py     # all settings, from environment / .env
│   └── spark.py      # SparkSession, Kafka source, streaming helpers
├── producers/        # covid.py, weather.py, sport.py, cyber.py (one-shot)
└── consumers/        # covid.py, weather.py, sport.py, cyber.py (streaming)
scripts/
├── create_topics.py  # idempotent topic creation
└── check_topics.py   # message count + sample per topic
```

## Requirements

- Python 3.10 or 3.11 (PySpark 3.5 does not support 3.12+)
- Java 17
- Docker (for the Kafka broker)

## Quick start

```bash
cp .env.example .env            # then fill in NEWSAPI_KEY and FOOTBALL_DATA_KEY
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Temporary single-node Kafka (KRaft) until docker-compose lands
docker run -d --name sdp-kafka -p 9092:9092 apache/kafka:3.9.1

python -m scripts.create_topics
python -m scaledatapipe.producers.covid      # likewise weather, sport, cyber
python -m scripts.check_topics

python -m scaledatapipe.consumers.covid                  # runs until Ctrl+C
python -m scaledatapipe.consumers.covid --available-now  # drains the topic, then exits
```

Outputs land in `$DATA_ROOT/outputs/<domain>/`, Spark checkpoints in
`$DATA_ROOT/checkpoints/<domain>/`. Delete a domain's checkpoint to reprocess its
topic from `STARTING_OFFSETS`.

### Note on exFAT / external drives (macOS)

macOS writes `._*` metadata files on exFAT volumes, which breaks Spark's file
commit (`FileNotFoundException ... _temporary/0/.__temporary`). If the repository
lives on such a drive, point `DATA_ROOT` in `.env` to an APFS location, e.g.
`DATA_ROOT=~/scaledatapipe-data`.

## Versions

`pyspark==3.5.1` bundles Scala 2.12, so the Kafka connector must be
`spark-sql-kafka-0-10_2.12:3.5.1`. `common/spark.py` derives it from the installed
PySpark, so upgrading PySpark alone keeps them consistent.
