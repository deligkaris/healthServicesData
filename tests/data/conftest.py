'''Fixtures of the data tests: the real datasets, loaded once per run with the library's own loaders.
Skipped unless pytest runs with --run-data (see tests/conftest.py); at OSC run them with tests/data/run.py.'''
import inspect
import re

import pytest
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

import utilities as utilitiesF
import cms.utilities as cmsUtilitiesF
import cms.base as baseF
import cms.mbsf as mbsfF
from tests.data.checks import Metrics

metrics = Metrics()

def get_years(config):
    yearI, yearF = config.getoption("--years")
    return yearI, yearF

def get_cms_filenames(config):
    return cmsUtilitiesF.get_filenames(config.getoption("--cms-path"), *get_years(config))

def get_data_paths(config):
    '''The pathToData and pathToAHAData passed to get_data: the options, else get_data's own defaults.'''
    defaults = inspect.signature(utilitiesF.get_data).parameters
    return {"pathToData": config.getoption("--data-path") or defaults["pathToData"].default,
            "pathToAHAData": config.getoption("--aha-path") or defaults["pathToAHAData"].default}

def get_external_filenames(config):
    paths = get_data_paths(config)
    return utilitiesF.get_filenames(paths["pathToData"], paths["pathToAHAData"], *get_years(config))

def get_file_year(path):
    return int(re.search(r"_(\d{4})\.parquet$", path).group(1))

def pytest_generate_tests(metafunc):
    '''Parametrizes, from the command line options, the tests that take one CMS file, claim type and part, or
    external source, with ids that -k can select on (eg -k "not carLine").'''
    config = metafunc.config
    if not config.getoption("--run-data"):
        #the tests are skipped, one placeholder case each keeps them reported as skipped
        for name in ["cmsFile", "claimTypePart", "baseClaimTypePart", "externalSource", "sourcePath"]:
            if name in metafunc.fixturenames:
                metafunc.parametrize(name, [None], ids=["skipped"])
        return
    cmsFilenames = get_cms_filenames(config)
    if "cmsFile" in metafunc.fixturenames:
        cmsFiles = [(k, get_file_year(path), path) for k, paths in cmsFilenames.items() for path in paths]
        metafunc.parametrize("cmsFile", cmsFiles, ids=[f"{k}-{year}" for k, year, _ in cmsFiles])
    if "claimTypePart" in metafunc.fixturenames:
        metafunc.parametrize("claimTypePart", list(cmsFilenames))
    if "baseClaimTypePart" in metafunc.fixturenames:
        metafunc.parametrize("baseClaimTypePart", [k for k in cmsFilenames if k.endswith("Base")])
    externalFilenames = get_external_filenames(config)
    if "externalSource" in metafunc.fixturenames:
        metafunc.parametrize("externalSource", [k for k in externalFilenames if k != "geojsonCounty"])
    if "sourcePath" in metafunc.fixturenames:
        sourcePaths = [(k, path) for k, paths in externalFilenames.items() if k != "geojsonCounty" for path in paths]
        metafunc.parametrize("sourcePath", sourcePaths, ids=[f"{k}-{path.rsplit('/', 1)[-1]}" for k, path in sourcePaths])

def pytest_terminal_summary(terminalreporter):
    if not metrics.rows:
        return
    terminalreporter.section("data metrics")
    for check, key, year, value, floor in sorted(metrics.rows, key=lambda r: (r[0], r[1], str(r[2]))):
        shown = f"{value:.4f}" if isinstance(value, float) else str(value)
        below = " BELOW FLOOR" if (floor is not None and value is not None and value < floor) else ""
        terminalreporter.write_line(f"{check:<24} {key:<40} {year!s:<5} {shown:>12}  floor {floor}{below}")

@pytest.fixture(scope="session", autouse=True)
def spark():
    '''The session of the batch job, built from its spark settings (overrides the local[2] one of tests/conftest.py).'''
    return (SparkSession.builder
            .appName("healthServicesData-data-tests")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate())

@pytest.fixture(scope="session")
def metricsTable():
    return metrics

@pytest.fixture(scope="session")
def years(request):
    return get_years(request.config)

@pytest.fixture(scope="session")
def yearList(years):
    return list(range(years[0], years[1] + 1))

@pytest.fixture(scope="session")
def lastObservableDay(years):
    return cmsUtilitiesF.get_lastObservableDay(years[1])

@pytest.fixture(scope="session")
def cmsFilenames(request):
    return get_cms_filenames(request.config)

@pytest.fixture(scope="session")
def externalFilenames(request):
    return get_external_filenames(request.config)

@pytest.fixture(scope="session")
def externalData(spark, request, years):
    '''utilities.get_data for the loaded years.'''
    return utilitiesF.get_data(*years, spark, **get_data_paths(request.config))

@pytest.fixture(scope="session")
def cmsRaw(spark, cmsFilenames, years):
    '''The CMS files as read_data types and unions them, with the date parts and ssaCounty that need no join.'''
    dataframes = cmsUtilitiesF.read_data(spark, cmsFilenames, *years)
    for claimTypePart, df in dataframes.items():
        (claimType, claimPart) = cmsUtilitiesF.get_claimType_claimPart(claimTypePart)
        if claimPart is not None:
            df = cmsUtilitiesF.add_through_date_info(df)
        if claimPart == "Base":
            df = baseF.add_ssaCounty(df)
            if claimType in ["ip", "snf", "hha", "hosp"]:
                df = baseF.add_admission_date_info(df, claimType=claimType)
            if claimType == "ip":
                df = baseF.add_discharge_date_info(df, claimType=claimType)
        if claimType == "mbsf":
            df = mbsfF.add_ssaCounty(mbsfF.add_death_date_info(df))
        dataframes[claimTypePart] = df
    return dataframes

@pytest.fixture(scope="session")
def cmsPreliminary(spark, cmsFilenames, years, externalData):
    '''(the dataframes add_preliminary_info takes, what it returns), the step of get_cms_data that joins the
    external data onto the claims.'''
    (yearI, yearF) = years
    before = cmsUtilitiesF.read_data(spark, cmsFilenames, yearI, yearF)
    before["mbsf"] = mbsfF.prep_mbsf(before["mbsf"])
    #add_preliminary_info replaces the dict's values, so it gets a copy
    after = cmsUtilitiesF.add_preliminary_info(dict(before), externalData, cmsUtilitiesF.get_lastObservableDay(yearF),
                                               firstObservableDay=cmsUtilitiesF.get_firstObservableDay(yearI))
    return before, after

@pytest.fixture(scope="session")
def cmsData(spark, request, years, externalData):
    '''get_cms_data with its defaults, what an analysis script starts from.'''
    return cmsUtilitiesF.get_cms_data(request.config.getoption("--cms-path"), *years, spark, externalData)

@pytest.fixture(scope="session")
def sampledCmsData(request, cmsData):
    '''cmsData restricted to the beneficiaries with DSYSRTKY % --beneficiary-sample == 0, for the enrichers that
    join a beneficiary's claims with each other.'''
    n = request.config.getoption("--beneficiary-sample")
    #computed once and cut from the get_cms_data lineage, which every enricher would otherwise re-plan and rerun
    #several times over (add_days_at_home_info reads ipBase four times)
    return {k: df.filter(F.col("DSYSRTKY") % n == 0).localCheckpoint() for k, df in cmsData.items()}
