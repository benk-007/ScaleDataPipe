#!/bin/sh
# spark-submit to the standalone cluster from a compose container.
# Executors connect back to the driver, so advertise this container's IP
# (its hostname is a container id that other containers cannot resolve).
# Defaults fit the 6 GB memory budget (see README): one app = 1 core, a 512 MB
# driver and a 512 MB executor; the worker only has room for one executor, so a
# second cluster app waits instead of overcommitting memory.
set -e
exec spark-submit \
  --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  --conf spark.driver.host="$(hostname -i | awk '{print $1}')" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-512m}" \
  --conf spark.cores.max="${SPARK_CORES_MAX:-1}" \
  --conf spark.executor.memory="${SPARK_EXECUTOR_MEMORY:-512m}" \
  "$@"
