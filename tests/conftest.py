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
               .getOrCreate())
    yield session
    session.stop()
