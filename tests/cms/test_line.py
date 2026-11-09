'''cms/line.py on rows of the carLine schema: each flag and its xInClaim column, the helpers, the claim summary.'''
import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

import cms.line as lineF
from cms.schemas import schemas
from tests.cms.test_invariants import assert_inClaim_is_any_line, mixed_claims

#the columns the flags read that the raw carLine file does not have
schema = StructType(schemas["carLine"].fields + [StructField("primaryTaxonomy", StringType()),
                                                 StructField("THRU_DT_YEAR", IntegerType())])

def make_raw_lines(spark, rows):
    return spark.createDataFrame([{name: row.get(name) for name in schema.fieldNames()} for row in rows], schema=schema)

def make_lines(spark, rows):
    '''The rows on the real schema, unset columns null, with the level1HCPCS_CD the visit flags read.'''
    return lineF.add_level1HCPCS_CD(make_raw_lines(spark, rows))

def specialty(*codes):
    return [{"HCFASPCL": code} for code in codes]

def taxonomy(*codes):
    return [{"primaryTaxonomy": code} for code in codes]

def hcpcs(*codes, year=2018):
    return [{"HCPCS_CD": code, "THRU_DT_YEAR": year} for code in codes]

def visit(specialtyCode, code, year=2018):
    return {"HCFASPCL": specialtyCode, "HCPCS_CD": code, "THRU_DT_YEAR": year}

#column -> (lines that set it, lines just outside its codes)
flags = {
    "pcp": (specialty("01", "08", "11"), specialty("00", "02", "07", "09", "10", "12", None)),
    "pcpFromTaxonomy": (taxonomy("207Q00000X", "208D00000X", "207R00000X"), taxonomy("207QG0300X", "2084N0400X", None)),
    "alfVisit": (hcpcs("99324", "99328", "99334", "99338"), hcpcs("99323", "99329", "99333", "99339", None)),
    "opVisit": (hcpcs("99201", "99205", "99211", "99215"), hcpcs("99200", "99206", "99210", "99216", None)),
    "neurology": (specialty("13"), specialty("12", "14", None)),
    "neurologyFromTaxonomy": (taxonomy("2084N0400X"), taxonomy("2084B0040X", None)),
    "neuropsychiatry": (specialty("62", "68", "86"), specialty("61", "63", "67", "69", "85", "87", None)),
    "neuropsychiatryFromTaxonomy": (taxonomy("2084B0040X"), taxonomy("2084N0400X", None)),
    "geriatric": (specialty("38"), specialty("37", "39", None)),
    "geriatricFromTaxonomy": (taxonomy("207QG0300X", "207RG0300X"), taxonomy("207Q00000X", None)),
    "ct": (hcpcs("70450", "70460", "70470"), hcpcs("70449", "70451", "70471", None)),
    #70552 (MRI brain with contrast) is left out here and 99341 (home visit) below: the code excludes both, which may be a gap
    "mri": (hcpcs("70551", "70553"), hcpcs("70550", "70554", None)),
    "homeVisit": (hcpcs("99342", "99350"), hcpcs("99340", "99351", None)),
    #the neuropsychological testing codes changed in 2019, 96116 is in both sets
    "nptesVisit": (hcpcs("96116", "96118", "96119") + hcpcs("96116", "96121", "96132", "96133", "96136", "96137", "96138", "96139",
                                                            year=2019),
                   hcpcs("96115", "96121", "96132") + hcpcs("96118", "96119", "96140", year=2019)),
    "neurologyOpVisit": ([visit("13", "99213")], [visit("13", "99342"), visit("14", "99213")]),
    "pcpOpVisit": ([visit("11", "99213")], [visit("11", "99342"), visit("13", "99213")]),
    "pcpHomeVisit": ([visit("11", "99342")], [visit("11", "99213"), visit("13", "99342")]),
    "pcpAlfVisit": ([visit("11", "99324")], [visit("11", "99213"), visit("13", "99324")]),
    "geriatricOpVisit": ([visit("38", "99213")], [visit("38", "99342"), visit("13", "99213")]),
    "neuropsychiatryOpVisit": ([visit("62", "99213")], [visit("62", "99342"), visit("13", "99213")]),
    "neuropsychiatryNptesVisit": ([visit("62", "96132", 2019)], [visit("62", "99213", 2019), visit("13", "96132", 2019)])}

def add_flag(column):
    return getattr(lineF, f"add_{column}")

#the two modes have their own copy of the condition
@pytest.mark.parametrize("inClaim", [False, True], ids=["line", "inClaim"])
@pytest.mark.parametrize("column", flags)
def test_flag_marks_its_lines(spark, column, inClaim):
    (hits, misses) = flags[column]
    #each line its own claim, so CLAIMNO says which one it is
    lines = [{**row, "DSYSRTKY": 1, "CLAIMNO": i, "THRU_DT": 20180101} for i, row in enumerate(hits + misses)]
    result = add_flag(column)(make_lines(spark, lines), inClaim=inClaim).collect()
    assert len(result) == len(lines)
    wrong = [lines[r["CLAIMNO"]] for r in result if r[column] != int(r["CLAIMNO"] < len(hits))]
    assert not wrong, f"{column} wrong on {wrong}"

@pytest.mark.parametrize("column", flags)
def test_inClaim_spreads_over_the_claim_only(spark, column):
    (hits, misses) = flags[column]
    lines = [{**hits[0], "DSYSRTKY": 1, "CLAIMNO": 1, "THRU_DT": 20180101},
             {**misses[0], "DSYSRTKY": 1, "CLAIMNO": 1, "THRU_DT": 20180101},
             {**misses[0], "DSYSRTKY": 1, "CLAIMNO": 2, "THRU_DT": 20180101},
             #CLAIMNO resets every year and belongs to a beneficiary, so these two are other claims
             {**misses[0], "DSYSRTKY": 1, "CLAIMNO": 1, "THRU_DT": 20190101},
             {**misses[0], "DSYSRTKY": 2, "CLAIMNO": 1, "THRU_DT": 20180101}]
    result = add_flag(column)(make_lines(spark, lines), inClaim=True).collect()
    assert sorted((r["DSYSRTKY"], r["CLAIMNO"], r["THRU_DT"], r[column], r[f"{column}InClaim"]) for r in result) == [
        (1, 1, 20180101, 0, 1), (1, 1, 20180101, 1, 1), (1, 1, 20190101, 0, 0), (1, 2, 20180101, 0, 0), (2, 1, 20180101, 0, 0)]

@pytest.mark.parametrize("column", flags)
def test_inClaim_is_any_line_of_the_claim_on_mixed_claims(spark, column):
    #many claims, each with its own mix of lines that set the flag and lines that do not
    lines = make_lines(spark, mixed_claims(*flags[column], seed=sorted(flags).index(column)))
    add = add_flag(column)
    assert_inClaim_is_any_line(lines, column, add(lines, inClaim=False), add(lines, inClaim=True), lineF.get_line_summary(add(lines, inClaim=True)))

@pytest.mark.parametrize("column", flags)
def test_no_inClaim_column_by_default(spark, column):
    lines = make_lines(spark, [{"DSYSRTKY": 1}])
    added = set(add_flag(column)(lines).columns) - set(lines.columns)
    assert column in added and f"{column}InClaim" not in added

@pytest.mark.parametrize("code,expected", [("99213", 99213), ("00100", 100), ("G0101", None), ("9921", None), ("992130", None),
                                           (None, None)])
def test_level1HCPCS_CD_is_five_digits_only(spark, code, expected):
    assert make_lines(spark, [{"HCPCS_CD": code}]).first()["level1HCPCS_CD"] == expected

@pytest.mark.parametrize("value,expected", [("A", 1), ("B", 0), (None, 0)])
def test_allowed(spark, value, expected):
    assert lineF.add_allowed(make_raw_lines(spark, [{"PRCNGIND": value}])).first()["allowed"] == expected

def test_filter_claims_needs_all_three_keys(spark):
    key = {"DSYSRTKY": 1, "CLAIMNO": 1, "THRU_DT": 20180101}
    lines = make_raw_lines(spark, [key, {**key, "DSYSRTKY": 2}, {**key, "CLAIMNO": 2}, {**key, "THRU_DT": 20190101}])
    base = make_raw_lines(spark, [key, key])
    result = lineF.filter_claims(lines, base)
    assert [(r["DSYSRTKY"], r["CLAIMNO"], r["THRU_DT"]) for r in result.collect()] == [(1, 1, 20180101)]
    assert set(result.columns) == set(lines.columns)

def test_primaryTaxonomy_keeps_lines_with_no_match(spark):
    lines = make_raw_lines(spark, [{"PRF_NPI": "1"}, {"PRF_NPI": "2"}]).drop("primaryTaxonomy")
    npi = spark.createDataFrame([("1", "207Q00000X", "ignored")], ["NPI", "primaryTaxonomy", "other"])
    result = lineF.add_primaryTaxonomy(lines, npi)
    assert {r["PRF_NPI"]: r["primaryTaxonomy"] for r in result.collect()} == {"1": "207Q00000X", "2": None}
    assert "other" not in result.columns

def test_line_summary_has_one_row_per_claim(spark):
    key = {"DSYSRTKY": 1, "CLAIMNO": 1, "THRU_DT": 20180101}
    lines = make_lines(spark, [{**key, **visit("11", "99342")}, {**key, **visit("13", "99213")},
                               {**key, **visit("13", "99342"), "THRU_DT": 20190101}])
    lines = lineF.add_opVisit(lineF.add_pcp(lines, inClaim=True), inClaim=True)
    summary = lineF.get_line_summary(lines)
    assert set(summary.columns) == {"DSYSRTKY", "CLAIMNO", "THRU_DT", "pcp", "opVisit"}
    assert sorted((r["THRU_DT"], r["pcp"], r["opVisit"]) for r in summary.collect()) == [(20180101, 1, 1), (20190101, 0, 0)]
