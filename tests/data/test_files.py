'''The CMS files against cms/schemas.py and cms/longToShortXW.py: what read_data would silently drop, null or truncate.'''
import os

import pytest
import pyspark.sql.functions as F

import cms.utilities as cmsUtilitiesF
from cms.schemas import schemas
from cms.longToShortXW import longToShortXW
from tests.data.checks import get_counts
from tests.data.thresholds import castLossAllowed, columnsMissingSomeYears

#the widths pad_zeros left-pads to; lpad also cuts a longer value down to them
padWidths = {"STATE_CD": 2, "CNTY_CD": 3, "PRSTATE": 2, **{f"STATE_CNTY_FIPS_CD_{m:02d}": 5 for m in range(1, 13)}}

def read_named(spark, claimTypePart, path):
    '''The file as stored, with the short column names read_and_prep_dataframe gives it.'''
    df = spark.read.parquet(path)
    if "DESY_SORT_KEY" in df.columns:
        (claimType, claimPart) = cmsUtilitiesF.get_claimType_claimPart(claimTypePart)
        df = cmsUtilitiesF.enforce_short_names(df, claimType=claimType, claimPart=claimPart)
    return df

def test_file_has_rows(spark, cmsFile):
    (claimTypePart, year, path) = cmsFile
    assert os.path.exists(path), f"{path} missing"
    assert spark.read.parquet(path).head(1), f"{path} has no rows"

def test_long_names_have_a_crosswalk(spark, cmsFile):
    (claimTypePart, year, path) = cmsFile
    columns = spark.read.parquet(path).columns
    if "DESY_SORT_KEY" not in columns:
        return
    assert claimTypePart in longToShortXW, f"{claimTypePart} {year} has long names, longToShortXW has no {claimTypePart}"
    unmapped = sorted(set(columns) - set(longToShortXW[claimTypePart]))
    assert not unmapped, f"{claimTypePart} {year}: {len(unmapped)} long names not in longToShortXW, eg {unmapped[:5]}"

def test_columns_are_in_schema(spark, cmsFile):
    #enforce_schema keeps only the schema's columns, so any other column is dropped without a word
    (claimTypePart, year, path) = cmsFile
    extra = sorted(set(read_named(spark, claimTypePart, path).columns) - set(schemas[claimTypePart].fieldNames()))
    assert not extra, f"{claimTypePart} {year}: {len(extra)} columns not in schemas, dropped on read: {extra[:10]}"

def test_columns_present_every_year(spark, cmsFilenames, claimTypePart):
    #unionByName(allowMissingColumns=True) fills a column a year's file lacks with nulls, for every claim of that year
    paths = cmsFilenames[claimTypePart]
    if len(paths) < 2:
        pytest.skip("one year loaded")
    schemaColumns = set(schemas[claimTypePart].fieldNames())
    columns = {path: set(read_named(spark, claimTypePart, path).columns) & schemaColumns for path in paths}
    allColumns = set.union(*columns.values())
    allowed = columnsMissingSomeYears.get(claimTypePart, set())
    missing = {path.rsplit("/", 1)[-1]: sorted(allColumns - cols - allowed) for path, cols in columns.items()}
    missing = {k: v for k, v in missing.items() if v}
    assert not missing, f"{claimTypePart}: columns missing from some years, all null there: {missing}"

def test_no_value_lost_when_typed(spark, cmsFile):
    #with ANSI off a value that does not cast becomes null (eg an NPI over 2^31-1 cast to IntegerType)
    (claimTypePart, year, path) = cmsFile
    (claimType, claimPart) = cmsUtilitiesF.get_claimType_claimPart(claimTypePart)
    named = read_named(spark, claimTypePart, path)
    stored = named.select("*", *[F.col(c).alias(f"stored{c}") for c in named.columns])
    prepped = cmsUtilitiesF.fix_year(cmsUtilitiesF.pad_zeros(stored, claimType, claimPart), claimType, claimPart)
    fields = [f for f in schemas[claimTypePart].fields if f.name in named.columns]
    lost = get_counts(prepped, {f.name: F.col(f"stored{f.name}").isNotNull() & F.col(f.name).cast(f.dataType).isNull()
                                for f in fields})
    allowed = castLossAllowed.get((claimTypePart, year), {})
    lost = {c: n for c, n in lost.items() if n > allowed.get(c, 0)}
    assert not lost, f"{claimTypePart} {year}: rows that turn null when typed: {lost}"

def test_padded_codes_fit_their_width(spark, cmsFile):
    (claimTypePart, year, path) = cmsFile
    named = read_named(spark, claimTypePart, path)
    cols = [c for c in padWidths if c in named.columns]
    if not cols:
        return
    #pad_zeros casts to int first, so a value is cut when its digits outnumber the width
    cut = get_counts(named, {c: F.length(F.col(c).cast("int").cast("string")) > padWidths[c] for c in cols})
    cut = {c: n for c, n in cut.items() if n}
    assert not cut, f"{claimTypePart} {year}: values longer than pad_zeros keeps: {cut}"

def test_rows_belong_to_the_file_year(spark, cmsFile):
    #get_firstObservableDay and the year-keyed joins assume a year's file holds that through/reference year only
    (claimTypePart, year, path) = cmsFile
    df = cmsUtilitiesF.read_and_prep_dataframe(path, claimTypePart, spark)
    yearCol = F.col("RFRNC_YR") if claimTypePart == "mbsf" else F.substring(F.col("THRU_DT").cast("string"), 1, 4).cast("int")
    counts = get_counts(df, {"otherYear": yearCol != year, "noYear": yearCol.isNull()})
    assert counts == {"otherYear": 0, "noYear": 0}, f"{claimTypePart} {year}: {counts}"

def test_beneficiary_id_present(cmsRaw, claimTypePart):
    #the files from 2015 and earlier began with several all-zero DSYSRTKY rows (see the notes in cms/mbsf.py)
    counts = get_counts(cmsRaw[claimTypePart], {"null": F.col("DSYSRTKY").isNull(), "zero": F.col("DSYSRTKY") == 0})
    assert counts == {"null": 0, "zero": 0}, f"{claimTypePart}: DSYSRTKY {counts}"
