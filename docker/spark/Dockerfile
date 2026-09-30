# Spark 3.5.1 (Scala 2.12, Java 17, Python 3.10) with the Kafka connector baked in,
# so no Maven download happens at job submission. Used by the master, the worker
# and the "app" container that runs producers, scripts and spark-submit.
FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu

USER root

ARG MAVEN=https://repo1.maven.org/maven2
ADD --chmod=644 --checksum=sha256:cb96af6c95c0beea714f73e5bc297f962bdd82fe492d8df94239c23e6a52bcf6 \
    ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:b753422d64544703919abded8cbbc2ed76098da74e6b194d3407233eefc822a0 \
    ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:9d0a9058c8ede79db6e54ae378ed16fe8d1bead2894635a4aa6aa0b7981668c6 \
    ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:ea0505ee7515e58b1ac0e686e4d1a5d9f7d808e251a61bc371aa0595b9963f83 \
    ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

COPY requirements-base.txt /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements-base.txt

COPY --chmod=755 docker/spark/submit.sh /usr/local/bin/submit
COPY --chmod=755 docker/spark/submit-local.sh /usr/local/bin/submit-local
COPY --chmod=755 docker/spark/submit-auto.sh /usr/local/bin/submit-auto
COPY --chmod=755 scaledatapipe /opt/app/scaledatapipe
COPY --chmod=755 scripts /opt/app/scripts

ENV PATH=/opt/spark/bin:$PATH \
    PYTHONPATH=/opt/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PYSPARK_PYTHON=python3
WORKDIR /opt/app

USER spark
