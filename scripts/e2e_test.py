"""End-to-end test of a running stack: triggers one DAG run and checks every layer.

    docker compose up -d
    python3 scripts/e2e_test.py            # exits 0 when every check passes

Standard library only, no JVM: Airflow is driven through its CLI and metadata DB,
HDFS is checked over WebHDFS (localhost:9870). The DAG's paused state is restored
at the end.

Checks:
1. the pipeline services are running;
2. a fresh DAG run (run_id e2e__<timestamp>) completes with all 6 tasks successful;
3. Bronze, every Silver table and every Gold table exist on HDFS with data;
4. every Gold table was rebuilt during this run.
"""
import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DAG_ID = "scaledatapipe_pipeline"
TASKS = {"produce_covid", "produce_weather", "produce_sport", "produce_cyber",
         "wait_for_ingestion", "build_gold"}
SERVICES = ["kafka", "namenode", "datanode", "streaming", "airflow-db", "airflow-scheduler"]
LAKE = "/scaledatapipe"
SILVER = ["covid", "weather", "sport", "cyber"]
GOLD = ["covid_daily", "weather_city_daily", "sport_matches", "sport_team_stats", "cyber_vendor_exposure"]
WEBHDFS = "http://localhost:9870/webhdfs/v1"


# --------------------------------------------------------------------- pure logic
def parse_rows(text: str) -> dict[str, str]:
    """psql -At -F'|' output "task_id|state" -> {task_id: state}."""
    return {task: state for task, state in (line.split("|", 1) for line in text.splitlines() if "|" in line)}


def run_outcome(states: dict[str, str]) -> str:
    """'success', 'failed' or 'running' for the task states of one DAG run."""
    if any(s in ("failed", "upstream_failed") for s in states.values()):
        return "failed"
    if set(states) == TASKS and all(s == "success" for s in states.values()):
        return "success"
    return "running"


# --------------------------------------------------------------------- I/O
def compose(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", "compose", *args], cwd=REPO, capture_output=True, text=True)


def psql(sql: str) -> str:
    r = compose("exec", "-T", "airflow-db", "psql", "-U", "airflow", "-d", "airflow", "-At", "-F", "|", "-c", sql)
    if r.returncode:
        raise RuntimeError(r.stderr.strip())
    return r.stdout.strip()


def airflow(*args: str) -> str:
    r = compose("exec", "-T", "airflow-scheduler", "airflow", *args)
    if r.returncode:
        raise RuntimeError(r.stderr.strip()[-500:])
    return r.stdout


def webhdfs(path: str, op: str) -> dict | None:
    try:
        with urllib.request.urlopen(f"{WEBHDFS}{path}?op={op}&user.name=e2e", timeout=30) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


class Report:
    def __init__(self):
        self.failures = 0

    def check(self, ok: bool, label: str, detail: str = ""):
        self.failures += not ok
        print(f"  [{'PASS' if ok else 'FAIL'}] {label}{f' ({detail})' if detail else ''}", flush=True)
        return ok


def check_services(report: Report) -> bool:
    ps = compose("ps", "--format", "{{.Service}}|{{.State}}").stdout
    running = {svc for svc, state in (l.split("|") for l in ps.splitlines() if "|" in l) if state == "running"}
    missing = [s for s in SERVICES if s not in running]
    return report.check(not missing, "pipeline services running", f"missing: {', '.join(missing)}" if missing else "")


def run_dag(report: Report, timeout: int) -> tuple[bool, float]:
    run_id = f"e2e__{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    started = time.time()
    airflow("dags", "trigger", DAG_ID, "--run-id", run_id)
    print(f"  triggered {run_id}, waiting up to {timeout}s", flush=True)
    states: dict[str, str] = {}
    while time.time() - started < timeout:
        states = parse_rows(psql(f"select task_id, coalesce(state, 'none') from task_instance "
                                 f"where dag_id = '{DAG_ID}' and run_id = '{run_id}'"))
        outcome = run_outcome(states)
        if outcome != "running":
            break
        time.sleep(15)
    else:
        outcome = "timeout"
    ok = report.check(outcome == "success", "DAG run: all 6 tasks successful",
                      f"{outcome} after {time.time() - started:.0f}s: "
                      + ", ".join(f"{t}={s}" for t, s in sorted(states.items())))
    return ok, started


def check_lake(report: Report, run_started: float):
    bronze = webhdfs(f"{LAKE}/bronze/events", "GETCONTENTSUMMARY")
    files = bronze["ContentSummary"]["fileCount"] if bronze else 0
    report.check(files > 0, "Bronze table has data", f"{files} files")
    for domain in SILVER:
        s = webhdfs(f"{LAKE}/silver/{domain}", "GETCONTENTSUMMARY")
        n = s["ContentSummary"]["fileCount"] if s else 0
        report.check(n > 0, f"Silver {domain} has data", f"{n} files")
    for table in GOLD:
        status = webhdfs(f"{LAKE}/gold/{table}", "GETFILESTATUS")
        summary = webhdfs(f"{LAKE}/gold/{table}", "GETCONTENTSUMMARY")
        if not (status and summary):
            report.check(False, f"Gold {table} rebuilt by this run", "missing")
            continue
        modified = status["FileStatus"]["modificationTime"] / 1000
        report.check(modified >= run_started and summary["ContentSummary"]["length"] > 0,
                     f"Gold {table} rebuilt by this run",
                     f"written {datetime.fromtimestamp(modified):%H:%M:%S}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--timeout", type=int, default=1500, help="seconds to wait for the DAG run")
    args = parser.parse_args()
    report = Report()

    print("1. Services")
    if not check_services(report):
        sys.exit("Start the stack first: docker compose up -d")

    was_paused = psql(f"select is_paused from dag where dag_id = '{DAG_ID}'") != "f"
    if was_paused:
        airflow("dags", "unpause", DAG_ID)
    try:
        print("2. DAG run")
        _, started = run_dag(report, args.timeout)
        print("3. Data lake")
        check_lake(report, started)
    finally:
        if was_paused:
            airflow("dags", "pause", DAG_ID)
            print("  (DAG paused again, as it was before the test)")

    print(f"\n{'PASSED' if not report.failures else f'FAILED: {report.failures} check(s)'}")
    sys.exit(1 if report.failures else 0)


if __name__ == "__main__":
    main()
