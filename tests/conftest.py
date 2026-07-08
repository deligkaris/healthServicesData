import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session", autouse=True)
def spark():
    session = (SparkSession.builder
               .master("local[2]")
               .appName("healthServicesData-tests")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.default.parallelism", "2")
               .config("spark.ui.enabled", "false")
               .config("spark.driver.bindAddress", "127.0.0.1")
               # NOTE: intentionally left at Spark 3.5 defaults (ANSI SQL mode OFF) to match the OSC
               # cluster, which runs stock Apache Spark 3.5.1. Do not enable spark.sql.ansi.enabled here.
               .getOrCreate())
    yield session
    session.stop()
