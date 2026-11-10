'''base.get_aggregate_summary, one row per unit (the beneficiary's county by default) with its claims (total) and, for each
flag, how many of them have it (xInUnit) and their share (xInUnitPerCent): no unit has more flagged claims than claims, no
count is missing or negative, the checks a print-only base.test_get_aggregate_summary used to make by hand.'''
import random

from cms.base import get_aggregate_summary

def claims(spark, rows):
    return spark.createDataFrame(rows, "ssaCounty string, state string, stroke int, died int")

def by_county(summary):
    rows = summary.collect()
    assert len(rows) == len({r["ssaCounty"] for r in rows}), "more than one row for a county"
    return {r["ssaCounty"]: r for r in rows}

def test_counts_and_shares_by_county(spark):
    rows = [("36250", "OH", 1, 0), ("36250", "OH", 1, 1), ("36250", "OH", 0, 0), ("36260", "OH", 0, 0)]
    out = by_county(get_aggregate_summary(claims(spark, rows), ["stroke", "died"]))
    franklin = out["36250"]
    assert (franklin["total"], franklin["strokeInUnit"], franklin["diedInUnit"]) == (3, 2, 1)
    assert (franklin["strokeInUnitPerCent"], franklin["diedInUnitPerCent"]) == (66.7, 33.3)
    assert (out["36260"]["total"], out["36260"]["strokeInUnit"], out["36260"]["strokeInUnitPerCent"]) == (1, 0, 0.0)

def test_by_several_columns_and_with_columns_carried(spark):
    rows = [("36250", "OH", 1, 0), ("36250", "OH", 0, 0), ("05010", "CA", 1, 1)]
    summary = get_aggregate_summary(claims(spark, rows), ["stroke"], aggBy=["state", "ssaCounty"])
    assert sorted((r["state"], r["ssaCounty"], r["total"], r["strokeInUnit"]) for r in summary.collect()) == \
           [("CA", "05010", 1, 1), ("OH", "36250", 2, 1)]
    carried = by_county(get_aggregate_summary(claims(spark, rows), ["stroke"], alsoReturnItems=["state"]))
    assert {c: r["state"] for c, r in carried.items()} == {"36250": "OH", "05010": "CA"}
    assert set(carried["36250"].asDict()) == {"ssaCounty", "total", "state", "strokeInUnit", "strokeInUnitPerCent"}

def test_no_unit_has_more_flagged_claims_than_claims_nor_a_missing_or_negative_count(spark):
    #on claims spread at random over counties
    rng = random.Random(1)
    rows = [(rng.choice(["36250", "36260", "36270"]), "OH", rng.randint(0, 1), rng.randint(0, 1)) for _ in range(200)]
    out = by_county(get_aggregate_summary(claims(spark, rows), ["stroke", "died"]))
    assert sum(r["total"] for r in out.values()) == 200
    for county, r in out.items():
        for flag in ("stroke", "died"):
            count = sum(1 for row in rows if row[0] == county and row[2 if flag == "stroke" else 3] == 1)
            assert r[f"{flag}InUnit"] == count and 0 <= count <= r["total"]
            #F.round rounds half up, python's round to even, so compare to the rounding step
            assert abs(r[f"{flag}InUnitPerCent"] - 100 * count / r["total"]) <= 0.05 + 1e-9

def test_claims_with_no_county_are_counted(spark):
    #they are a unit of their own, null, with every one of its claims in its total
    rows = [("36250", "OH", 1, 0), (None, "OH", 1, 0), (None, "OH", 1, 0), (None, "OH", 0, 0)]
    unknown = by_county(get_aggregate_summary(claims(spark, rows), ["stroke"]))[None]
    assert (unknown["total"], unknown["strokeInUnit"], unknown["strokeInUnitPerCent"]) == (3, 2, 66.7)
