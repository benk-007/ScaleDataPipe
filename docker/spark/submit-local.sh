#!/bin/sh
# spark-submit in local mode: driver and executor share one JVM. Used for short
# batch jobs (Gold, reports): cheaper than a cluster driver + executor pair, and
# it leaves the worker to the streaming job.
set -e
exec spark-submit \
  --master "local[${SPARK_LOCAL_CORES:-2}]" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-640m}" \
  --conf spark.ui.enabled=false \
  "$@"
