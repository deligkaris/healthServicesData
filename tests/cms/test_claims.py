'''cms/claims.py: the join of base and revenue summary, and the outpatient ED visits of non-PPS hospitals.'''
import pytest
from pyspark.sql.types import IntegerType, StructField, StructType

import cms.claims as claimsF
from cms.schemas import schemas

def make_df(spark, part, rows, added=()):
    '''The rows on the real schema plus the added int columns, unset columns null.'''
    schema = StructType(schemas[part].fields + [StructField(name, IntegerType()) for name in added])
    return spark.createDataFrame([{name: row.get(name) for name in schema.fieldNames()} for row in rows], schema=schema)

def test_get_claims_keeps_claims_with_no_summary(spark):
    key = {"DSYSRTKY": 1, "CLAIMNO": 1, "THRU_DT": 20180101}
    base = make_df(spark, "ipBase", [key, {**key, "THRU_DT": 20190101}])
    summary = spark.createDataFrame([(1, 1, 20180101, 1)], ["DSYSRTKY", "CLAIMNO", "THRU_DT", "ed"])
    result = claimsF.get_claims(base, summary).collect()
    #the same CLAIMNO a year later is another claim
    assert sorted((r["THRU_DT"], r["ed"]) for r in result) == [(20180101, 1), (20190101, None)]

hospital = 100
day = 5000

def ip_claim(pps=None, **flags):
    return {"DSYSRTKY": 1, "ORGNPINM": hospital, "ADMSN_DT_DAY": day, "PPS_IND": pps, "ed": 0, "mri": 0, "ct": 0, **flags}

def update(spark, ipClaim, visits):
    '''visits: (ORGNPINM, THRU_DT_DAY, [REV_CNTR of its lines]) of the outpatient claims of the same beneficiary, or
    of another beneficiary given as a 4th element.'''
    visits = [(*visit, 1)[:4] for visit in visits]
    claims = [{"DSYSRTKY": bene, "CLAIMNO": i, "THRU_DT": 20180101, "ORGNPINM": npi, "THRU_DT_DAY": thruDay}
              for i, (npi, thruDay, _, bene) in enumerate(visits)]
    lines = [{"DSYSRTKY": bene, "CLAIMNO": i, "THRU_DT": 20180101, "REV_CNTR": code}
             for i, (_, _, codes, bene) in enumerate(visits) for code in codes]
    return claimsF.update_nonPPS_revenue_info(make_df(spark, "ipBase", [ipClaim], ["ADMSN_DT_DAY", "ed", "mri", "ct"]),
                                              make_df(spark, "opBase", claims, ["THRU_DT_DAY"]),
                                              make_df(spark, "opRevenue", lines))

@pytest.mark.parametrize("ipClaim,visits,expected", [
    (ip_claim(), [(hospital, day, [450, 610, 350])], (1, 1, 1)),
    (ip_claim(ed=None), [(hospital, day, [450])], (1, 0, 0)),
    #two ED visits that day, each with its own imaging, count as one
    (ip_claim(), [(hospital, day, [450, 610]), (hospital, day, [450, 350])], (1, 1, 1)),
    (ip_claim(), [(hospital, day - 1, [450, 610, 350])], (0, 0, 0)),
    (ip_claim(), [(hospital + 1, day, [450, 610, 350])], (0, 0, 0)),
    #the imaging of an ED visit that differs in the day, the hospital or the beneficiary stays with that visit
    (ip_claim(), [(hospital, day, [450]), (hospital, day - 1, [450, 610, 350])], (1, 0, 0)),
    (ip_claim(), [(hospital, day, [450]), (hospital + 1, day, [450, 610, 350])], (1, 0, 0)),
    (ip_claim(), [(hospital, day, [450]), (hospital, day, [450, 610, 350], 2)], (1, 0, 0)),
    #imaging counts only on an outpatient claim with an ED visit
    (ip_claim(), [(hospital, day, [610, 350])], (0, 0, 0)),
    (ip_claim(ed=1), [], (1, 0, 0)),
    #PPS hospitals bundle the ED visit in the inpatient claim already
    (ip_claim(pps=2), [(hospital, day, [450, 610, 350])], (0, 0, 0))],
    ids=["nonPps", "nullEd", "twoVisits", "otherDay", "otherHospital", "imagingOtherDay", "imagingOtherHospital",
         "imagingOtherBeneficiary", "noEdVisit", "noVisit", "pps"])
def test_update_nonPPS_revenue_info(spark, ipClaim, visits, expected):
    result = update(spark, ipClaim, visits)
    assert [(r["ed"], r["mri"], r["ct"]) for r in result.collect()] == [expected]
    assert not {"oped", "opmri", "opct"} & set(result.columns)
