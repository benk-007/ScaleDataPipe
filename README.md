# ScaleDataPipe

Real-time data pipeline: Python producers pull four public sources into Apache Kafka,
and a Spark Structured Streaming job lands them in a Medallion lake on HDFS
(Bronze raw copy -> Silver typed, validated and deduplicated tables), from which
a batch job builds Gold business aggregates. Apache Airflow orchestrates the
producers and the Gold refresh every hour.

| Domain  | Source                                   | Kafka topic     |
|---------|------------------------------------------|-----------------|
| COVID   | [NewsAPI](https://newsapi.org) (key)     | `covid_topic`   |
| Weather | [Open-Meteo](https://open-meteo.com)     | `weather_topic` |
| Sport   | [football-data.org](https://www.football-data.org) (key) | `sport_topic` |
| Cyber   | [CISA KEV feed](https://www.cisa.gov/known-exploited-vulnerabilities-catalog) | `cyber_topic` |

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
└── orchestration/
    └── freshness.py  # "has streaming caught up?" check for the Airflow sensor
dags/
└── scaledatapipe_pipeline.py  # hourly: producers -> wait for ingestion -> Gold
scripts/
├── init_env.py       # generates the Airflow secrets into .env
├── create_topics.py  # idempotent topic creation
├── check_topics.py   # message count + sample per topic
└── lake_report.py    # row counts and reject reasons per lake table
tests/                # Silver/Gold logic (local Spark), producers, sensor, DAG
docker/
├── spark/Dockerfile  # Spark 3.5.1 + baked-in Kafka connector + app code
├── spark/submit*.sh  # spark-submit: cluster, local, or auto (cluster if configured)
├── airflow/Dockerfile # Airflow 2.9.3 + Java 17 + PySpark + app code
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
| weather | `city`, `observed_at`                        | no city/time, temperature, humidity or wind out of range    |
| sport   | `match_id`, `status`, scores                 | no id/kickoff/team, unknown status, finished without score  |
| cyber   | `cve_id`                                     | malformed CVE id, no `date_added`                           |

`covid_mentions` counts the words `covid`, `covid19` and `coronavirus` (so
"COVID-19" counts once, as "covid"). The sport producer fetches the matches
between `SPORT_DAYS_BACK` days ago and `SPORT_DAYS_AHEAD` days ahead (14/14 by
default) rather than the start of the season: recent final scores plus upcoming
fixtures. Keep the window wide enough to span international breaks.

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

| Service             | Role                                                      | Host access           |
|---------------------|-----------------------------------------------------------|-----------------------|
| `kafka`             | Kafka 3.9.1, KRaft (no ZooKeeper)                         | `localhost:9092`      |
| `kafka-init`        | creates the topics, then exits                            |                       |
| `namenode`          | HDFS NameNode                                             | http://localhost:9870 |
| `datanode`          | HDFS DataNode                                             |                       |
| `streaming`         | Medallion job: Kafka -> Bronze -> Silver, always on       |                       |
| `airflow-db`        | Postgres 16, Airflow metadata                             |                       |
| `airflow-init`      | migrates the DB, creates the `spark` pool and the admin   |                       |
| `airflow-scheduler` | scheduler + LocalExecutor (runs the DAG tasks)            |                       |
| `airflow-webserver` | web UI, profile `ui` (see "Validation status")           | http://localhost:8081 |
| `spark-master`, `spark-worker` | standalone cluster, profile `cluster` (optional) | http://localhost:8080 |
| `app`               | one-off tools container, profile `tools`                  |                       |

```bash
cp .env.example .env                  # fill in NEWSAPI_KEY and FOOTBALL_DATA_KEY
python3 scripts/init_env.py           # random Airflow secrets, written to .env only
docker compose up -d --build          # not --wait: kafka-init / airflow-init are one-shot jobs

# The DAG is created paused: enable it, or trigger one run
docker compose exec airflow-scheduler airflow dags unpause scaledatapipe_pipeline
docker compose exec airflow-scheduler airflow dags trigger scaledatapipe_pipeline
docker compose exec airflow-scheduler airflow dags list-runs -d scaledatapipe_pipeline

# Inspect the lake
docker compose run --rm app submit-local scripts/lake_report.py
docker compose exec namenode hdfs dfs -ls /scaledatapipe/gold

docker compose down        # keeps Kafka, HDFS and Airflow data (add -v to wipe it)
```

Producers and one-off jobs can also be run by hand:

```bash
docker compose run --rm app python3 -m scaledatapipe.producers.covid   # likewise weather, sport, cyber
docker compose run --rm app python3 -m scripts.check_topics
docker compose run --rm app submit-local scaledatapipe/medallion/gold.py
```

`app` waits until the topics exist and HDFS has left safe mode. The Kafka
connector jars are part of the Spark image (checksum-pinned), so jobs never
download from Maven at runtime. API keys and Airflow secrets come from `.env` at
run time and are never copied into the images (see `.dockerignore`).

### Orchestration (Airflow)

`dags/scaledatapipe_pipeline.py` runs hourly (NewsAPI's free tier allows 100
requests/day), one run at a time, without catch-up:

```
produce_covid ─┐
produce_weather┤
produce_sport  ├─> wait_for_ingestion ─> build_gold
produce_cyber ─┘
```

- **produce_\*** run the producers (2 retries each for flaky APIs).
- **wait_for_ingestion** is a sensor (reschedule mode, so it frees its worker slot
  between checks) that waits until the streaming job has caught up: the Kafka
  offsets of Bronze's last committed batch reach the end of every partition, and
  every Silver/quarantine query committed after it. It reads the streaming
  checkpoints over WebHDFS, so the check needs no JVM.
- **build_gold** runs Gold with Spark in local mode inside the scheduler container,
  in a 1-slot `spark` pool: never two Spark jobs at once.

The Bronze/Silver streaming job is not an Airflow task: it runs continuously in
its own container. Airflow tasks run in the Airflow image (Java + PySpark + app
code); Airflow has no access to the Docker socket.

### Streaming job and Spark modes

The streaming job is supervised: a failing query stops it and Docker restarts it;
it then resumes from its checkpoints. By default it runs Spark in local mode
(driver and executor in one JVM). The optional standalone cluster is kept for
demonstration:

```bash
SPARK_MASTER_URL=spark://spark-master:7077 docker compose --profile cluster up -d
```

In cluster mode the job also exits (and is restarted) when it has had no
executor for `EXECUTOR_GRACE_SECONDS` (default 120), and compose restarts it with
the master. `submit` targets the cluster, `submit-local` runs a job in a single
local JVM, `submit-auto` picks the cluster when `SPARK_MASTER_URL` is set. After
rebuilding the images, run `docker compose up -d` so every service picks them up.

To process the lake in batch instead (e.g. to rebuild it), stop the service first:
two jobs must never share the same checkpoints.

```bash
docker compose stop streaming
docker compose run --rm app submit-local scaledatapipe/medallion/run.py --available-now
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
```

### Memory budget (8 GB Mac, Docker Desktop at 4.5 GB)

Every JVM has an explicit heap and every container a hard `mem_limit`. Peaks were
measured during a full DAG run (producers, streaming, sensor and Gold):

| Service             | JVM heap              | Measured peak | `mem_limit` |
|---------------------|-----------------------|---------------|-------------|
| kafka               | 256 MB                | 496 MB        | 576 MB      |
| namenode            | 256 MB                | 497 MB        | 640 MB      |
| datanode            | 192 MB                | 621 MB*       | 768 MB      |
| streaming           | 640 MB (local mode)   | 672 MB        | 1152 MB     |
| airflow-db          | -                     | 48 MB         | 192 MB      |
| airflow-scheduler   | 640 MB during Gold    | 888 MB        | 1536 MB     |
| airflow-webserver   | -                     | not validated | 640 MB      |

\* includes page cache from block writes, which the kernel reclaims first.

All containers together peaked at 3.06 GB; the Docker VM never went below 932 MB
available and never used its own swap. The HDFS images are amd64-only and run
emulated on Apple Silicon, which adds JVM overhead and slows their start-up.

On macOS the Docker VM keeps the memory its containers touched, even after they
stop, so macOS ends up compressing or swapping it. Quit or restart Docker
Desktop after a session to give the memory back.

### Validation status

Validated on a MacBook with 8 GB of RAM (Docker Desktop limited to 4.5 GB):

- **Unit tests**: 22 in the local venv (Silver rules/typing/dedup and Gold
  aggregates through Spark, producers, sensor logic) plus 3 DAG-structure tests
  run in the Airflow image.
- **End to end**: one full scheduled DAG run succeeded through the Airflow CLI:
  6/6 tasks, Gold rebuilt its 5 tables on HDFS, 76 s. The stack was started stage
  by stage under a memory watchdog.
- **Memory**: that run stayed within the Docker limits above, but macOS swap usage
  grew by 134 MB by its end, above the 100 MB threshold set for this validation.
  The full stack is at the limit of an 8 GB machine; 16 GB is recommended to run
  it comfortably.
- **Airflow web UI**: not validated. Starting it next to the running pipeline
  pushed macOS into swap on this machine: it needs more RAM than is available
  here. The pipeline was validated through the CLI only.

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
tests cover latest-state selection, points, dedup and the table swap. The DAG
tests need Airflow and are skipped in the venv; run them in the Airflow image:

```bash
docker run --rm -v "$PWD":/src -w /src -e AIRFLOW__CORE__LOAD_EXAMPLES=false \
  scaledatapipe/airflow:2.9.3 bash -c "pip install -q pytest && python -m pytest -q -p no:cacheprovider tests/test_dag.py"
```

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
