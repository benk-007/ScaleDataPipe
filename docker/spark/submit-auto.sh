#!/bin/sh
# Cluster if SPARK_MASTER_URL is set (compose "cluster" profile), local otherwise.
set -e
if [ -n "$SPARK_MASTER_URL" ]; then
  exec submit "$@"
fi
exec submit-local "$@"
