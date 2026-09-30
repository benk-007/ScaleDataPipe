"""Pure logic of scripts/e2e_test.py (the script itself needs a running stack)."""
from scripts import e2e_test

ALL_OK = {t: "success" for t in e2e_test.TASKS}


def test_parse_rows():
    assert e2e_test.parse_rows("produce_covid|success\nbuild_gold|none\n") == {
        "produce_covid": "success", "build_gold": "none"}


def test_run_outcome():
    assert e2e_test.run_outcome(ALL_OK) == "success"
    assert e2e_test.run_outcome({**ALL_OK, "build_gold": "running"}) == "running"
    assert e2e_test.run_outcome({**ALL_OK, "wait_for_ingestion": "up_for_reschedule"}) == "running"
    assert e2e_test.run_outcome({**ALL_OK, "build_gold": "upstream_failed"}) == "failed"
    assert e2e_test.run_outcome({"produce_weather": "success"}) == "running"  # a partial run is never a success
