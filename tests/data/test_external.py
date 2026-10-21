'''The external sources utilities.get_data loads: present, readable, keyed the way the joins onto the claims assume.'''
import os
from functools import reduce

import pytest
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

from tests.data.checks import get_duplicate_keys, get_counts, get_null_rates
from tests.data.thresholds import keyFormatExceptions

ccn = r"^[0-9A-Z]{6}$"
county = r"^\d{5}$"
zip5 = r"^\d{5}$"

#the columns the joins in cms/base.py and cms/mbsf.py read (Spark matches column names regardless of case)
expectedColumns = {
    "npi": ["NPI", "primaryTaxonomy", "gachAll", "rachAll", "cahAll", "rehabilitationAll", "pediatricHospital",
            "psychiatricHospital", "ltcHospital", "Provider Organization Name (Legal Business Name)",
            "Provider Business Practice Location Address Postal Code",
            "Provider Business Practice Location Address State Name"],
    "aha": ["MCRNUM", "year", "LAT", "LONG", "ahaCah", "ahaTotalHospitalBeds", "ahaSize", "ahaBedsIcu", "SYSID"],
    "sdoh": ["COUNTYFIPS", "year", "ACS_MEDIAN_HH_INC", "medianHhIncomeAdjusted", "POS_MEDIAN_DIST_ED",
             "POS_MEDIAN_DIST_MEDSURG_ICU", "POS_MEDIAN_DIST_TRAUMA"],
    "census": ["fipsCounty", "year", "medianHouseholdIncome", "totalPopulation"],
    "chspHosp": ["ccn", "year", "health_sys_id", "isVI"],
    "cmi": ["provider", "year", "casemixindex"],
    "maPenetration": ["FIPS", "Year", "Penetration"],
    "ersRucc": ["FIPS", "RUCC_2013", "ruccGroup"],
    "zipToHrr": ["zip", "year", "hsanum", "hrrnum", "hrrcity", "hrrstate"],
    "cbsa": ["ssacounty", "fipscounty", "countyname"],
    "pos": ["PRVDR_NUM", "providerFIPS", "providerStateFIPS", "GNRL_CNTL_TYPE_CD", "posCah", "posIsRural"],
    "hcris": ["PRVDR_NUM", "hcrisYear", "providerHcrisBedsIcu", "providerHcrisBedsCriticalCare", "providerHcrisBedsTotal",
              "providerHcrisBedDaysIcu", "providerHcrisBedDaysCriticalCare", "providerHcrisBedDaysTotal",
              "providerHcrisResidents", "providerHcrisResidentToBedRatio", "providerHcrisIsRural"],
    "ccnStrokeCertification": ["posCcn", "jcBestProgram", "jcCertificationConfidence", "jcSites"],
    "procedureClasses": ["ICD-10-PCS-CODE", "PROCEDURE-CLASS"],
    "adi": ["FIPS", "adiNatRank", "adiStaRank", "adiNatRankGroup"],
    "zipToCounty": ["zip", "county", "countyForZip"]}

#the year column of each source joined on the claim year
yearColumns = {"aha": "year", "cmi": "year", "chspHosp": "year", "sdoh": "year", "census": "year", "zipToHrr": "year",
               "maPenetration": "Year", "hcris": "hcrisYear"}

#(source, key, rows kept before the check): the join key of each source, which must not fan the claims out
uniqueKeys = {"npi": ("npi", ["NPI"], None),
              "pos": ("pos", ["PRVDR_NUM"], None),
              "aha": ("aha", ["MCRNUM", "year"], None),
              "chspHosp": ("chspHosp", ["ccn", "year"], None),
              "cmi": ("cmi", ["provider", "year"], None),
              "zipToHrr": ("zipToHrr", ["zip", "year"], None),
              "maPenetration": ("maPenetration", ["FIPS", "Year"], None),
              "ersRucc": ("ersRucc", ["FIPS"], None),
              "cbsaSsa": ("cbsa", ["ssacounty"], None),
              #base.add_providerCountyName takes the distinct (countyname, fipscounty) first
              "cbsaFips": ("cbsa", ["fipscounty"], lambda df: df.select("countyname", "fipscounty").distinct()),
              "sdoh": ("sdoh", ["COUNTYFIPS", "year"], None),
              "census": ("census", ["fipsCounty", "year"], None),
              "adi": ("adi", ["FIPS"], None),
              "hcris": ("hcris", ["PRVDR_NUM", "hcrisYear"], None),
              "ccnStrokeCertification": ("ccnStrokeCertification", ["posCcn"], None),
              "strokeCentersCamargo": ("strokeCentersCamargo", ["CCN"], None),
              "procedureClasses": ("procedureClasses", ["ICD-10-PCS-CODE"], None),
              "zipToCounty": ("zipToCounty", ["zip"], lambda df: df.filter(F.col("countyForZip") == 1))}

#(source, column, format): a key that is not in the format of the claims' side never matches
keyFormats = [("npi", "NPI", r"^\d{10}$"),
              ("pos", "PRVDR_NUM", ccn), ("aha", "MCRNUM", ccn), ("chspHosp", "ccn", ccn), ("cmi", "provider", ccn),
              ("hcris", "PRVDR_NUM", ccn), ("ccnStrokeCertification", "posCcn", ccn), ("strokeCentersCamargo", "CCN", ccn),
              ("pos", "providerFIPS", county), ("ersRucc", "FIPS", county), ("maPenetration", "FIPS", county),
              ("cbsa", "fipscounty", county), ("cbsa", "ssacounty", county), ("sdoh", "COUNTYFIPS", county),
              ("census", "fipsCounty", county), ("zipToCounty", "county", county),
              ("zipToCounty", "zip", zip5), ("zipToHrr", "zip", zip5),
              ("adi", "FIPS", r"^\d{12}$"),
              ("procedureClasses", "ICD-10-PCS-CODE", r"^[0-9A-Z]{7}$")]

#the AHA columns add_aha_info selects, and the first year each exists (see prep_ahaDF)
ahaColumnsSince = {"LAT": None, "LONG": None, "ahaCah": None, "ahaTotalHospitalBeds": None, "ahaSize": None,
                   "ahaOwner": None, "ahaCOTH": None, "ahaResidentToBedRatio": None, "ahaBedsIcu": None,
                   "ahaSystemMember": None, "STRCHOS": 2017, "STRCSYS": 2017, "STRCVEN": 2017,
                   "ahaTeleicuHos": 2017, "ahaTeleicuSys": 2017, "ahaTeleicuVen": 2017}

def test_file_exists(sourcePath):
    (source, path) = sourcePath
    assert os.path.exists(path), f"{source}: {path} missing"

def test_source_has_rows(externalData, externalSource):
    df = externalData[externalSource]
    assert df.head(1), f"{externalSource} has no rows"

def test_header_is_text(externalData, externalSource):
    #a binary file read as CSV (eg a .shp) comes back with a header of control characters
    bad = [c for c in externalData[externalSource].columns if not c.isprintable()]
    assert not bad, f"{externalSource}: {len(bad)} column names are not text, eg {bad[:3]!r}; is it a CSV?"

def test_no_quote_wrapped_values(externalData, externalSource):
    #a field quoted with ' keeps its quotes when read as CSV (eg the HCUP codes as 'value'), so it no longer
    #matches the unquoted claims
    df = externalData[externalSource]
    strCols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    if not strCols:
        return
    wrapped = reduce(lambda x, y: x | y, [F.col(f"`{c}`").rlike(r"^(['\"]).*\1$") for c in strCols])
    number = df.filter(wrapped).count()
    assert number == 0, f"{externalSource}: {number} rows with a value wrapped in quotes"

@pytest.mark.parametrize("source", list(expectedColumns))
def test_expected_columns(externalData, source):
    present = {c.lower() for c in externalData[source].columns}
    missing = [c for c in expectedColumns[source] if c.lower() not in present]
    assert not missing, f"{source}: missing {missing}"

@pytest.mark.parametrize("source", list(yearColumns))
def test_every_year_present(externalData, yearList, source):
    #a year-keyed source without the claim year gives that year's claims nulls
    yearCol = yearColumns[source]
    present = {int(r[0]) for r in externalData[source].select(yearCol).distinct().collect() if r[0] is not None}
    missing = sorted(set(yearList) - present)
    assert not missing, f"{source}: no rows for {missing}"

@pytest.mark.parametrize("keyName", list(uniqueKeys))
def test_join_key_unique(externalData, keyName):
    (source, keys, prepare) = uniqueKeys[keyName]
    df = externalData[source]
    df = (prepare(df) if prepare else df).dropna(subset=keys)
    (number, examples) = get_duplicate_keys(df, keys)
    assert number == 0, f"{source}: {number} {keys} on more than one row, each would multiply its claims, eg {examples}"

@pytest.mark.parametrize("source, column, pattern", keyFormats, ids=[f"{s}-{c}" for s, c, _ in keyFormats])
def test_key_format(externalData, source, column, pattern):
    df = externalData[source]
    value = F.col(f"`{column}`").cast("string")
    counts = get_counts(df, {"bad": value.isNotNull() & ~value.rlike(pattern)})
    allowed = keyFormatExceptions.get((source, column), 0)
    examples = [r[0] for r in df.filter(value.isNotNull() & ~value.rlike(pattern)).select(value).limit(5).collect()] \
               if counts["bad"] > allowed else []
    assert counts["bad"] <= allowed, f"{source}.{column}: {counts['bad']} values not {pattern}, eg {examples}"

def test_aha_columns_filled_every_year(externalData, yearList):
    aha = externalData["aha"]
    cols = [c for c in ahaColumnsSince if c in aha.columns]
    nullRates = get_null_rates(aha, cols, "year")
    empty = {year: [c for c in cols if rates[c] == 1.0 and (ahaColumnsSince[c] or 0) <= year]
             for year, rates in nullRates.items() if year in yearList}
    empty = {year: cols for year, cols in empty.items() if cols}
    absent = [c for c in ahaColumnsSince if c not in aha.columns and (ahaColumnsSince[c] or 0) <= max(yearList)]
    assert not empty and not absent, f"aha: all null {empty}, absent {absent}"
