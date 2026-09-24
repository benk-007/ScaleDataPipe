#!/bin/sh
# spark-submit to the standalone cluster from a compose container.
# Executors connect back to the driver, so advertise this container's IP
# (its hostname is a container id that other containers cannot resolve).
# Each app takes 1 core / 768 MB by default so the long-running streaming job
# leaves room on the worker (2 cores / 1.5 GB) for one-off jobs.
set -e
exec spark-submit \
  --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  --conf spark.driver.host="$(hostname -i | awk '{print $1}')" \
  --conf spark.cores.max="${SPARK_CORES_MAX:-1}" \
  --conf spark.executor.memory="${SPARK_EXECUTOR_MEMORY:-768m}" \
  "$@"
