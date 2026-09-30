"""ScaleDataPipe: hourly ingestion and Gold refresh.

    produce_* (4 sources -> Kafka)
        -> wait_for_ingestion (streaming job has landed everything in Bronze/Silver)
        -> build_gold (Silver -> Gold, Spark local mode, one Spark job at a time)

The Bronze/Silver streaming job runs continuously in its own container; this DAG
only feeds it and builds Gold once it has caught up.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor

SOURCES = ["covid", "weather", "sport", "cyber"]
APP = "/opt/scaledatapipe"


def _caught_up() -> bool:
    from scaledatapipe.orchestration.freshness import caught_up  # imported at run time, not parse time

    ok, summary = caught_up()
    print(summary)
    return ok


with DAG(
    dag_id="scaledatapipe_pipeline",
    description="Producers -> Kafka -> (streaming Bronze/Silver) -> Gold",
    schedule="@hourly",  # NewsAPI free tier: 100 requests/day
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["scaledatapipe"],
) as dag:
    produce = [
        BashOperator(
            task_id=f"produce_{source}",
            bash_command=f"cd {APP} && python -m scaledatapipe.producers.{source}",
            execution_timeout=timedelta(minutes=5),
        )
        for source in SOURCES
    ]

    wait_for_ingestion = PythonSensor(
        task_id="wait_for_ingestion",
        python_callable=_caught_up,
        mode="reschedule",  # frees the worker slot between pokes
        poke_interval=30,
        timeout=timedelta(minutes=15).total_seconds(),
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=f"submit-local {APP}/scaledatapipe/medallion/gold.py",
        pool="spark",  # 1 slot: never two Spark jobs at once (memory budget)
        execution_timeout=timedelta(minutes=20),
    )

    produce >> wait_for_ingestion >> build_gold
