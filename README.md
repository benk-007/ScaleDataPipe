# ScaleDataPipe

Real-time data pipeline: Python producers pull four public sources into Apache Kafka,
and Spark Structured Streaming consumers process each topic.

| Domain  | Source                                   | Kafka topic     |
|---------|------------------------------------------|-----------------|
| COVID   | [NewsAPI](https://newsapi.org) (key)     | `covid_topic`   |
| Weather | [Open-Meteo](https://open-meteo.com)     | `weather_topic` |
| Sport   | [football-data.org](https://www.football-data.org) (key) | `sport_topic` |
| Cyber   | [CISA KEV feed](https://www.cisa.gov/known-exploited-vulnerabilities-catalog) | `cyber_topic` |

> Work in progress: Medallion storage (Bronze/Silver/Gold) and Airflow
> orchestration are the next milestones. Consumers currently write CSV files,
> to HDFS when run on the cluster, or locally from a venv.

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
docker/
├── spark/Dockerfile  # Spark 3.5.1 + baked-in Kafka connector + app code
├── spark/submit.sh   # spark-submit wrapper for the standalone cluster
└── hadoop.env        # HDFS configuration
docker-compose.yml
```

## Run with Docker (recommended)

| Service        | Role                                   | Host access            |
|----------------|----------------------------------------|------------------------|
| `kafka`        | Kafka 3.9.1, KRaft (no ZooKeeper)      | `localhost:9092`       |
| `kafka-init`   | creates the topics, then exits         |                        |
| `namenode`     | HDFS NameNode                          | http://localhost:9870  |
| `datanode`     | HDFS DataNode                          |                        |
| `spark-master` | Spark 3.5.1 standalone master          | http://localhost:8080  |
| `spark-worker` | Spark worker (2 cores, 1.5 GB)         |                        |
| `app`          | one-off tools container (profile `tools`) |                     |

```bash
cp .env.example .env                  # fill in NEWSAPI_KEY and FOOTBALL_DATA_KEY
docker compose up -d --build          # not --wait: kafka-init is a one-shot job

# Producers -> Kafka
docker compose run --rm app python3 -m scaledatapipe.producers.covid   # likewise weather, sport, cyber
docker compose run --rm app python3 -m scripts.check_topics

# Consumers on the Spark cluster -> HDFS (hdfs://namenode:8020/scaledatapipe)
docker compose run --rm app submit scaledatapipe/consumers/covid.py --available-now
docker compose exec namenode hdfs dfs -ls -R /scaledatapipe/outputs

docker compose down        # keeps Kafka and HDFS data (add -v to wipe it)
```

`app` waits until the topics exist and HDFS has left safe mode. The Kafka
connector jars are part of the image (checksum-pinned), so jobs never download
from Maven at runtime. API keys come from `.env` at run time and are never
copied into the image (see `.dockerignore`).

Resources: about 1.6 GB of RAM at idle, plus about 1 GB per running job. The
HDFS images are amd64-only and run emulated on Apple Silicon, which is slower
to start but works.

## Run locally (venv, without the cluster)

Requires Python 3.10 or 3.11 and Java 17. Kafka still comes from Docker.

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d kafka kafka-init

python -m scaledatapipe.producers.covid                  # likewise weather, sport, cyber
python -m scripts.check_topics
python -m scaledatapipe.consumers.covid                  # runs until Ctrl+C
python -m scaledatapipe.consumers.covid --available-now  # drains the topic, then exits
```

Outputs land in `$DATA_ROOT/outputs/<domain>/`, Spark checkpoints in
`$DATA_ROOT/checkpoints/<domain>/`. Delete a domain's checkpoint to reprocess its
topic from `STARTING_OFFSETS`.

## Working from an exFAT drive (macOS)

macOS stores file metadata in `._*` files on exFAT volumes, which breaks two things:

- Spark's local file commit (`FileNotFoundException ... _temporary/0/.__temporary`):
  point `DATA_ROOT` in `.env` to an APFS location, e.g. `DATA_ROOT=~/scaledatapipe-data`.
- `docker compose build` (`failed to xattr ... ._<file>: operation not permitted`):
  delete them before building with
  `find . -name '._*' -not -path './.git/*' -type f -delete`.

Keeping the repository on an APFS disk avoids both.

## Versions

`pyspark==3.5.1` bundles Scala 2.12, so the Kafka connector must be
`spark-sql-kafka-0-10_2.12:3.5.1`. `common/spark.py` derives it from the installed
PySpark, so upgrading PySpark alone keeps them consistent. The Docker image pins
the same jars in `docker/spark/Dockerfile`: update both together.
