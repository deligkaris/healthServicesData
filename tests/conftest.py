from pathlib import Path

import pytest
from pyspark.sql import SparkSession

dataTestsDir = Path(__file__).parent / "data"

def pytest_addoption(parser):
    group = parser.getgroup("data", "data-consistency tests on the real datasets (tests/data, run at OSC)")
    group.addoption("--run-data", action="store_true", help="run only tests/data, which are skipped otherwise")
    group.addoption("--cms-path", default="/fs/ess/PAS2164/CMS", help="CMS root, as passed to get_cms_data")
    group.addoption("--data-path", default=None, help="non-CMS data root (default: get_data's pathToData)")
    group.addoption("--aha-path", default=None, help="AHA root (default: get_data's pathToAHAData)")
    group.addoption("--years", nargs=2, type=int, default=[2018, 2019], metavar=("YEARI", "YEARF"))
    group.addoption("--beneficiary-sample", type=int, default=100,
                    help="the enricher tests keep the beneficiaries with DSYSRTKY %% N == 0")
    group.addoption("--tmp-path", default=None,
                    help="where the enricher tests write their sampled CMS files, a directory every spark node can write")

def pytest_configure(config):
    config.addinivalue_line("markers", "data: needs the real CMS and external datasets, run with --run-data")

def pytest_collection_modifyitems(config, items):
    runData = config.getoption("--run-data")
    skipData = pytest.mark.skip(reason="needs the real datasets, run with --run-data at OSC")
    selected, deselected = [], []
    for item in items:
        isData = dataTestsDir in item.path.parents
        if isData:
            item.add_marker(pytest.mark.data)
            if not runData:
                item.add_marker(skipData)
        #--run-data runs the data tests alone: a unit test would first build the local[2] session below and
        #getOrCreate would then hand that session to the data tests
        (deselected if (runData and not isData) else selected).append(item)
    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = selected

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
