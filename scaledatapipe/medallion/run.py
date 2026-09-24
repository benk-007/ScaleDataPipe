"""Run the Medallion streaming layers: Kafka -> Bronze -> Silver (+ quarantine).

    submit scaledatapipe/medallion/run.py                     # both layers, forever
    submit scaledatapipe/medallion/run.py --available-now     # drain, then exit
    submit scaledatapipe/medallion/run.py --layer silver      # one layer only
"""
import argparse
import os
import sys
import time

from scaledatapipe.common.spark import get_spark, path_exists
from scaledatapipe.medallion import bronze, silver

POLL_SECONDS = 5
# A standalone driver whose master restarted waits forever for a reconnection
# that never comes. Exit instead, so the container restart policy recovers.
EXECUTOR_GRACE_SECONDS = int(os.getenv("EXECUTOR_GRACE_SECONDS", "120"))


def wait_for_bronze_commit(spark, bronze_query=None):
    """Block until Bronze has committed a batch.

    Silver must not start before Bronze's _spark_metadata exists: a file source
    started on a directory without it lists raw files instead of reading the
    sink's commit log, losing exactly-once guarantees.
    """
    metadata = f"{bronze.path()}/_spark_metadata"
    announced = False
    while not path_exists(spark, metadata):
        if bronze_query is None:
            raise SystemExit(f"Bronze table not found at {bronze.path()}: run --layer bronze first")
        if not bronze_query.isActive:
            error = bronze_query.exception()
            raise SystemExit(f"Bronze query failed: {error}" if error else "Bronze is empty: no Kafka data yet")
        if not announced:
            print("Waiting for the first Bronze commit before starting Silver...")
            announced = True
        time.sleep(POLL_SECONDS)


def executor_count(spark) -> int:
    # getExecutorMemoryStatus() also lists the driver
    return spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1


def supervise(spark):
    """Run until a query fails (raises) or the job has had no executor for too long."""
    cluster_mode = not spark.sparkContext.master.startswith("local")
    no_executor_since = None
    while not spark.streams.awaitAnyTermination(POLL_SECONDS):
        if not cluster_mode:
            continue
        if executor_count(spark) > 0:
            no_executor_since = None
        elif no_executor_since is None:
            no_executor_since = time.monotonic()
        elif time.monotonic() - no_executor_since > EXECUTOR_GRACE_SECONDS:
            print(f"No executor for {EXECUTOR_GRACE_SECONDS}s (cluster lost?): exiting so the job gets restarted",
                  file=sys.stderr)
            for query in spark.streams.active:
                query.stop()
            sys.exit(1)
    sys.exit("A streaming query stopped unexpectedly")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--layer", choices=["bronze", "silver", "all"], default="all")
    parser.add_argument("--available-now", action="store_true",
                        help="Process everything currently available, then stop.")
    args = parser.parse_args()

    spark = get_spark(f"Medallion-{args.layer}")

    bronze_query = None
    if args.layer in ("bronze", "all"):
        bronze_query = bronze.start(spark, args.available_now)
        if args.available_now:
            # Silver must see everything Bronze ingests in this run.
            bronze_query.awaitTermination()

    if args.layer in ("silver", "all"):
        wait_for_bronze_commit(spark, bronze_query if args.layer == "all" else None)
        silver.start(spark, args.available_now)

    if args.available_now:
        for query in spark.streams.active:
            query.awaitTermination()
    else:
        supervise(spark)  # a failing query or a lost cluster stops the job (and the container restarts it)


if __name__ == "__main__":
    main()
