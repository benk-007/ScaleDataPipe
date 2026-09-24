import os
import time

import pytest
from pyspark.sql import SparkSession

# collect() converts timestamps to the Python process' local timezone: pin it to
# UTC like the Spark session, so assertions read the same wall-clock values.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
