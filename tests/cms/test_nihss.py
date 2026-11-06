'''base.add_nihss and base.add_nihssGroup: the stroke severity behind transfers.add_transfernihss.'''
import pytest

import cms.base as baseF
from cms.schemas import schemas

def make_claim(spark, codes):
    '''One ipBase claim with the codes in its diagnosis columns (slot -> code), with its dgnsCodeAll.'''
    schema = schemas["ipBase"]
    row = {name: None for name in schema.fieldNames()}
    row.update({f"ICD_DGNS_CD{slot}": code for slot, code in codes.items()})
    return baseF.add_dgnsCodeAll(spark.createDataFrame([row], schema=schema))

@pytest.mark.parametrize("codes,expected", [
    ({1: "R29700"}, 0),
    ({1: "R29709"}, 9),
    ({1: "R29710"}, 10),
    ({1: "R29742"}, 42),
    ({25: "R29715"}, 15),
    ({1: "I639", 2: "R29705", 3: "E119"}, 5),
    ({1: "I639"}, None),
    ({}, None),
    #more than one score on a claim says neither is the score
    ({1: "R29705", 2: "R29710"}, None),
    #the scale ends at 42
    ({1: "R29743"}, None)],
    ids=["0", "9", "10", "42", "lastSlot", "amongOthers", "noScore", "noCodes", "twoScores", "over42"])
def test_nihss(spark, codes, expected):
    result = baseF.add_nihss(make_claim(spark, codes))
    assert result.first()["nihss"] == expected
    assert "nihssList" not in result.columns

@pytest.mark.parametrize("nihss,expected", [(0, 0), (9, 0), (10, 1), (19, 1), (20, 2), (29, 2), (30, 3), (39, 3), (40, 4), (42, 4),
                                            (43, None), (-1, None), (None, None)])
def test_nihssGroup(spark, nihss, expected):
    claims = spark.createDataFrame([(nihss,)], "nihss int")
    assert baseF.add_nihssGroup(claims).first()["nihssGroup"] == expected

def test_nihss_info_adds_both(spark):
    claim = baseF.add_nihss_info(make_claim(spark, {1: "R29725"})).first()
    assert (claim["nihss"], claim["nihssGroup"]) == (25, 2)
