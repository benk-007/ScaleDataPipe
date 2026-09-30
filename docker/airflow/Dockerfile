# Airflow 2.9.3 with Java 17 + PySpark 3.5.1 and the app code, so DAG tasks run
# the producers and Spark batch jobs (local mode) in-process. No Docker socket:
# Airflow never gets control of the Docker daemon.
FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && ln -s "/usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture)" /usr/lib/jvm/java-17 \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17

USER airflow
COPY requirements-base.txt /tmp/
# Airflow pinned so the extra packages cannot move it
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" pyspark==3.5.1 -r /tmp/requirements-base.txt

COPY --chmod=755 docker/spark/submit-local.sh /usr/local/bin/submit-local
COPY --chmod=755 scaledatapipe /opt/scaledatapipe/scaledatapipe
ENV PYTHONPATH=/opt/scaledatapipe
