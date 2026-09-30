"""DAG structure. Needs Airflow: skipped in the local venv, run it in the Airflow image:

    docker run --rm -v "$PWD":/src -w /src -e AIRFLOW__CORE__LOAD_EXAMPLES=false \
      scaledatapipe/airflow:2.9.3 bash -c "pip install -q pytest && python -m pytest -q -p no:cacheprovider tests/test_dag.py"
"""
from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(str(DAGS), include_examples=False)
    assert bag.import_errors == {}
    return bag.dags["scaledatapipe_pipeline"]


def test_schedule_and_concurrency(dag):
    assert dag.schedule_interval == "@hourly"
    assert dag.max_active_runs == 1 and dag.catchup is False


def test_task_graph(dag):
    producers = {f"produce_{s}" for s in ("covid", "weather", "sport", "cyber")}
    assert {t.task_id for t in dag.tasks} == producers | {"wait_for_ingestion", "build_gold"}
    assert dag.get_task("wait_for_ingestion").upstream_task_ids == producers
    assert dag.get_task("build_gold").upstream_task_ids == {"wait_for_ingestion"}


def test_spark_jobs_are_serialized(dag):
    assert dag.get_task("build_gold").pool == "spark"  # 1-slot pool created by airflow-init
    assert dag.get_task("wait_for_ingestion").mode == "reschedule"
