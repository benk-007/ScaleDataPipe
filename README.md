# ScaleDataPipe

Real-time data pipeline: Python producers pull four public sources into Apache Kafka,
and a Spark Structured Streaming job lands them in a Medallion lake on HDFS
(Bronze raw copy -> Silver typed, validated and deduplicated tables), from which
a batch job builds Gold business aggregates.

| Domain  | Source                                   | Kafka topic     |
|---------|------------------------------------------|-----------------|
| COVID   | [NewsAPI](https://newsapi.org) (key)     | `covid_topic`   |
| Weather | [Open-Meteo](https://open-meteo.com)     | `weather_topic` |
| Sport   | [football-data.org](https://www.football-data.org) (key) | `sport_topic` |
| Cyber   | [CISA KEV feed](https://www.cisa.gov/known-exploited-vulnerabilities-catalog) | `cyber_topic` |

> Work in progress: Airflow orchestration is the next milestone.

## Layout

```
scaledatapipe/
├── common/
│   ├── config.py     # all settings, from environment / .env
│   └── spark.py      # SparkSession, Kafka source, streaming helpers
├── producers/        # covid.py, weather.py, sport.py, cyber.py (one-shot)
├── medallion/
│   ├── bronze.py     # Kafka -> Bronze (raw records + Kafka coordinates)
│   ├── silver.py     # Bronze -> Silver / quarantine, per-domain rules
│   ├── run.py        # streaming entry point (both layers, supervised)
│   └── gold.py       # Silver -> Gold aggregates (batch)
scripts/
├── create_topics.py  # idempotent topic creation
├── check_topics.py   # message count + sample per topic
└── lake_report.py    # row counts and reject reasons per lake table
tests/                # Silver rules/typing/dedup and Gold aggregates (local Spark)
docker/
├── spark/Dockerfile  # Spark 3.5.1 + baked-in Kafka connector + app code
├── spark/submit.sh   # spark-submit wrapper for the standalone cluster
└── hadoop.env        # HDFS configuration
docker-compose.yml
```

## Data lake layout

Everything lives under `DATA_ROOT` (`hdfs://namenode:8020/scaledatapipe` in Docker).

| Table                    | Content                                                     | Partitions              |
|--------------------------|-------------------------------------------------------------|-------------------------|
| `bronze/events`          | every Kafka record, untouched JSON + topic/partition/offset | `topic`, `ingest_date`  |
| `silver/<domain>`        | typed business columns + Kafka lineage, deduplicated        | `event_date`            |
| `quarantine/<domain>`    | rows failing validation: raw value + `reject_reason`        | `ingest_date`           |
| `gold/<table>`           | business aggregates, rebuilt from Silver on each run        |                         |
| `checkpoints/<query>`    | Structured Streaming state                                  |                         |

- **Bronze** is append-only and exactly-once (Parquet file sink + checkpoint), so
  every downstream table can be rebuilt from it.
- **Silver** parses each topic with an explicit schema, types the columns
  (UTC timestamps, dates, numbers), applies per-domain rules and drops duplicate
  business keys. Every Bronze record ends up either in Silver or in quarantine,
  never silently dropped (duplicates excepted).

| Domain  | Business key (dedup)                         | Rejected when                                               |
|---------|----------------------------------------------|-------------------------------------------------------------|
| covid   | `article_id` (hash of URL, or title)         | no title, no date                                           |

`covid_mentions` counts the words `covid`, `covid19` and `coronavirus` (so
"COVID-19" counts once, as "covid"). The sport producer fetches the matches
between `SPORT_DAYS_BACK` days ago and `SPORT_DAYS_AHEAD` days ahead (14/14 by
default) rather than the start of the season: recent final scores plus upcoming
fixtures. Keep the window wide enough to span international breaks.
| weather | `city`, `observed_at`                        | no city/time, temperature, humidity or wind out of range    |
| sport   | `match_id`, `status`, scores                 | no id/kickoff/team, unknown status, finished without score  |
| cyber   | `cve_id`                                     | malformed CVE id, no `date_added`                           |

Silver deduplicates within `SILVER_DEDUP_WINDOW` (default 7 days) of the first
occurrence, which keeps streaming state bounded; Gold will deduplicate fully.
Sport keys include status and score so a match's progression (TIMED -> FINISHED)
is kept.

### Gold tables

| Table                   | Grain                    | Content                                                              |
|-------------------------|--------------------------|----------------------------------------------------------------------|
| `covid_daily`           | day                      | articles, articles mentioning "covid", mentions, share, sources      |
| `weather_city_daily`    | city, day                | observations, min/avg/max temperature, last reading, humidity, wind  |
| `sport_matches`         | match                    | current state (latest status and score) and result                   |
| `sport_team_stats`      | competition, team        | played, W/D/L, goals, goal difference, points (ingested matches only)|
| `cyber_vendor_exposure` | vendor                   | CVEs, ransomware-linked CVEs, products, first/last added, next due   |

Gold is a batch job: each run recomputes every table from the whole of Silver,
first keeping the latest record per business key (Silver only deduplicates within
its window), so reruns are idempotent. Each table is written to a temporary
directory, then swapped in. Every row carries the run's `computed_at` (UTC). The
job exits with an error when no Silver data exists at all.

## Run with Docker (recommended)

| Service        | Role                                   | Host access            |
|----------------|----------------------------------------|------------------------|
| `kafka`        | Kafka 3.9.1, KRaft (no ZooKeeper)      | `localhost:9092`       |
| `kafka-init`   | creates the topics, then exits         |                        |
| `namenode`     | HDFS NameNode                          | http://localhost:9870  |
| `datanode`     | HDFS DataNode                          |                        |
| `spark-master` | Spark 3.5.1 standalone master          | http://localhost:8080  |
| `spark-worker` | Spark worker (2 cores, 1.5 GB)         |                        |
| `streaming`    | Medallion job: Kafka -> Bronze -> Silver, always on |           |
| `app`          | one-off tools container (profile `tools`) |                     |

```bash
cp .env.example .env                  # fill in NEWSAPI_KEY and FOOTBALL_DATA_KEY
docker compose up -d --build          # not --wait: kafka-init is a one-shot job

# Producers -> Kafka
docker compose run --rm app python3 -m scaledatapipe.producers.covid   # likewise weather, sport, cyber
docker compose run --rm app python3 -m scripts.check_topics

# The streaming service picks the events up within seconds; then build Gold
docker compose run --rm app submit-local scaledatapipe/medallion/gold.py
docker compose run --rm app submit-local scripts/lake_report.py
docker compose exec namenode hdfs dfs -ls /scaledatapipe/silver

docker compose down        # keeps Kafka and HDFS data (add -v to wipe it)
```

`app` waits until the topics exist and HDFS has left safe mode.

The streaming job is supervised: a failing query, or a cluster lost for more than
`EXECUTOR_GRACE_SECONDS` (default 120), stops it and Docker restarts it; it then
resumes from its checkpoints. Compose also restarts it whenever it restarts the
Spark master. After rebuilding the image, run `docker compose up -d` so every
service picks it up.

`submit` sends a job to the Spark cluster; `submit-local` runs it in a single
JVM inside the `app` container, which is what short batch jobs (Gold, reports)
use so the worker stays dedicated to the streaming job.

To process the lake in batch instead (e.g. to rebuild it), stop the service first:
two jobs must never share the same checkpoints.

```bash
docker compose stop streaming
docker compose run --rm app submit scaledatapipe/medallion/run.py --available-now
docker compose start streaming
```

To rebuild one Silver domain from Bronze (e.g. after changing its rules), delete
its tables and checkpoints while the service is stopped; the job then replays
the whole of Bronze for that domain:

```bash
docker compose stop streaming
docker compose exec namenode hdfs dfs -rm -r /scaledatapipe/silver/covid /scaledatapipe/quarantine/covid \
  /scaledatapipe/checkpoints/silver_covid /scaledatapipe/checkpoints/quarantine_covid
docker compose start streaming
``` The Kafka
connector jars are part of the image (checksum-pinned), so jobs never download
from Maven at runtime. API keys come from `.env` at run time and are never
copied into the image (see `.dockerignore`).

### Memory budget (6 GB Docker Desktop)

The stack is sized for a Docker VM with 6 GB of RAM (8 GB laptop). Every JVM has
an explicit heap and every container a hard `mem_limit`, set 25-50% above the
peak measured under load (producers + streaming + a Gold job):

| Service        | JVM heap         | Measured peak | `mem_limit` |
|----------------|------------------|---------------|-------------|
| kafka          | 256 MB           | 440 MB        | 576 MB      |
| namenode       | 256 MB           | 466 MB        | 640 MB      |
| datanode       | 192 MB           | 762 MB*       | 768 MB      |
| spark-master   | 192 MB           | 215 MB        | 320 MB      |
| spark-worker   | 192 MB + 512 MB executor | 793 MB | 1024 MB    |
| streaming      | 512 MB driver    | 685 MB        | 896 MB      |
| app (Gold)     | 640 MB, local    | 657 MB        | 1152 MB     |

\* includes page cache from block writes, which the kernel reclaims first.

All containers together peaked at 3.75 GB (4.7 GB before the heaps were
bounded), leaving about 1.7 GB before the VM would touch swap. The worker only
offers room for one executor, so an extra cluster job waits rather than
overcommitting memory; run batch jobs with `submit-local`. The HDFS images are
amd64-only and run emulated on Apple Silicon, which adds JVM overhead and slows
their start-up.

## Run locally (venv, without the cluster)

Requires Python 3.10 or 3.11 and Java 17. Kafka still comes from Docker.

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d kafka kafka-init

python -m scaledatapipe.producers.covid                  # likewise weather, sport, cyber
python -m scripts.check_topics
python -m scaledatapipe.medallion.run --available-now   # Bronze + Silver under ./data
python -m scaledatapipe.medallion.gold                   # Gold
python -m scripts.lake_report
```

## Tests

```bash
pip install -r requirements-dev.txt
pytest tests
```

The Silver tests write Bronze-shaped Parquet and go through the real streaming
path (`readStream` + `availableNow`), including watermark deduplication. The Gold
tests cover latest-state selection, points, dedup and the table swap.

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
