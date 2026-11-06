'''cms/revenue.py on rows of the ipRevenue schema: each flag and its xInClaim column, the claim summary.'''
import pytest

import cms.revenue as revenueF
from cms.schemas import schemas

key = {"DSYSRTKY": 1, "CLAIMNO": 1, "THRU_DT": 20180101}

def make_lines(spark, rows):
    '''The rows on the real schema, where REV_CNTR is an int, unset columns null.'''
    schema = schemas["ipRevenue"]
    return spark.createDataFrame([{name: row.get(name) for name in schema.fieldNames()} for row in rows], schema=schema)

def rev(*codes):
    return [{"REV_CNTR": code} for code in codes]

def hcpcs(*codes):
    return [{"HCPCS_CD": code} for code in codes]

#column -> (lines that set it, lines just outside its codes)
flags = {
    "icu": (rev(200, 201, 204, 206, 209), rev(199, 205, 210, None)),
    "ed": (rev(450, 455, 459, 981), rev(449, 460, 980, 982, None)),
    "mri": (rev(610, 619), rev(609, 620, None)),
    "ct": (rev(350, 359), rev(349, 360, None)),
    "echo": (hcpcs("93304", "93306", "93307", "93312", "93313", "93314", "93315", "93316", "93317", "93320", "93321"),
             hcpcs("93300", "93322", "A9304", None))}

def add_flag(column):
    return getattr(revenueF, f"add_{column}")

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
    lines = [{**hits[0], **key}, {**misses[0], **key}, {**misses[0], **key, "CLAIMNO": 2},
             #CLAIMNO resets every year and belongs to a beneficiary, so these two are other claims
             {**misses[0], **key, "THRU_DT": 20190101}, {**misses[0], **key, "DSYSRTKY": 2}]
    result = add_flag(column)(make_lines(spark, lines), inClaim=True).collect()
    assert sorted((r["DSYSRTKY"], r["CLAIMNO"], r["THRU_DT"], r[column], r[f"{column}InClaim"]) for r in result) == [
        (1, 1, 20180101, 0, 1), (1, 1, 20180101, 1, 1), (1, 1, 20190101, 0, 0), (1, 2, 20180101, 0, 0), (2, 1, 20180101, 0, 0)]

@pytest.mark.parametrize("column", flags)
def test_no_inClaim_column_by_default(spark, column):
    lines = make_lines(spark, [key])
    added = set(add_flag(column)(lines).columns) - set(lines.columns)
    assert added == {column}

def test_filter_claims_needs_all_three_keys(spark):
    lines = make_lines(spark, [key, {**key, "DSYSRTKY": 2}, {**key, "CLAIMNO": 2}, {**key, "THRU_DT": 20190101}])
    base = make_lines(spark, [key, key])
    result = revenueF.filter_claims(lines, base)
    assert [(r["DSYSRTKY"], r["CLAIMNO"], r["THRU_DT"]) for r in result.collect()] == [(1, 1, 20180101)]
    assert set(result.columns) == set(lines.columns)

def test_summary_takes_any_line_of_the_claim(spark):
    lines = make_lines(spark, [{**key, "REV_CNTR": 450}, {**key, "REV_CNTR": 610}, {**key, "REV_CNTR": 100},
                               {**key, "REV_CNTR": 100, "CLAIMNO": 2}])
    summary = revenueF.get_revenue_summary(revenueF.add_mri(revenueF.add_ed(lines)))
    assert sorted((r["CLAIMNO"], r["ed"], r["mri"]) for r in summary.collect()) == [(1, 1, 1), (2, 0, 0)]

def test_summary_finds_the_flags_by_case(spark):
    #a new flag flows in with no edit, raw columns and the xInClaim windows stay out
    lines = revenueF.add_echo(revenueF.add_ed(make_lines(spark, [key]), inClaim=True), inClaim=True)
    assert set(revenueF.get_revenue_summary(lines).columns) == {"DSYSRTKY", "CLAIMNO", "THRU_DT", "ed", "echo"}

def test_add_revenue_info_adds_the_four_flags(spark):
    lines = make_lines(spark, [{**key, "REV_CNTR": 200}])
    added = set(revenueF.add_revenue_info(lines).columns) - set(lines.columns)
    assert added == {"ed", "mri", "ct", "icu", "edInClaim", "mriInClaim", "ctInClaim", "icuInClaim"}
    added = set(revenueF.add_revenue_info(lines, inClaim=False).columns) - set(lines.columns)
    assert added == {"ed", "mri", "ct", "icu"}

def test_revenue_info_is_one_row_per_claim_with_inClaim(spark):
    lines = make_lines(spark, [{**key, "REV_CNTR": 200}, {**key, "REV_CNTR": 450}])
    assert [(r["icu"], r["ed"]) for r in revenueF.get_revenue_info(lines).collect()] == [(1, 1)]
    assert sorted((r["icu"], r["ed"]) for r in revenueF.get_revenue_info(lines, inClaim=False).collect()) == [(0, 1), (1, 0)]
