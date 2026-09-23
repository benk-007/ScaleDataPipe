#!/bin/sh
# spark-submit to the standalone cluster from a compose container.
# Executors connect back to the driver, so advertise this container's IP
# (its hostname is a container id that other containers cannot resolve).
set -e
exec spark-submit \
  --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  --conf spark.driver.host="$(hostname -i | awk '{print $1}')" \
  --conf spark.executor.memory="${SPARK_EXECUTOR_MEMORY:-1g}" \
  "$@"
