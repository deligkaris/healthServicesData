'''tests/data/benchmarks.py, the measures test_benchmarks.py compares with CMS's figures, on small frames: CMS's Original
Medicare population against the library's ffs, the Geographic Variation stays and ER visits, the July MA share, the
POS hospitals active in a year.'''
import pytest

import cms.mbsf as mbsfF
from tests.data import benchmarks
from tests.data.checks import Metrics, check_band, check_rates

def mbsf_row(bene, buyIn="3" * 12, hmo="0" * 12, deathMonth=None, state="39", county="39049", age=70, sex=2, dual=0, year=2019):
    row = {"DSYSRTKY": bene, "RFRNC_YR": year, "fipsState": state, "fipsCounty": county, "AGE": age, "SEX": sex,
           "medicaidEver": dual, "HMO_MO": sum(c not in ("0",) for c in hmo), "maPenetration": 40,
           "DEATH_DT_MONTH": deathMonth, "DEATH_DT_YEAR": year if deathMonth else None}
    row.update({f"BUYIN{m + 1}": buyIn[m] for m in range(12)})
    row.update({f"HMOIND{m + 1}": hmo[m] for m in range(12)})
    return row

def make_mbsf(spark, rows):
    columns = list(rows[0])
    types = {c: "string" if c.startswith(("BUYIN", "HMOIND", "fips")) else "int" for c in columns}
    return spark.createDataFrame([tuple(r[c] for c in columns) for r in rows], ", ".join(f"{c} {types[c]}" for c in columns))

def with_ffs(df):
    return mbsfF.add_ffs(mbsfF.add_hmo(mbsfF.add_allPartAB(df)))

@pytest.mark.parametrize("case, row, cmsOm, ffs", [
    ("Part A and B all year", mbsf_row(1), 1, 1),
    ("state buy-in of A and B", mbsf_row(1, buyIn="C" * 12), 1, 1),
    #the two definitions part here: the library starts at the first A and B month, CMS looks at every entitled month
    ("Part A only until April", mbsf_row(1, buyIn="111" + "3" * 9), 0, 1),
    ("aged in during the year", mbsf_row(1, buyIn="0000" + "3" * 8), 1, 1),
    ("died in June", mbsf_row(1, buyIn="3" * 6 + "0" * 6, deathMonth=6), 1, 1),
    ("an MA month", mbsf_row(1, hmo="0000C0000000"), 0, 0),
    #whether CMS's HMO_MO counts a code 4 month is not documented, so only CMS's side is checked
    ("a fee-for-service demonstration month", mbsf_row(1, hmo="000040000000"), 1, None),
    ("never entitled", mbsf_row(1, buyIn="0" * 12), 0, 0)])
def test_cms_original_medicare_against_the_library_ffs(spark, case, row, cmsOm, ffs):
    out = benchmarks.add_cmsOm(with_ffs(make_mbsf(spark, [row]))).first()
    assert out["cmsOm"] == cmsOm, case
    assert ffs is None or out["ffs"] == ffs, case

def test_mbsf_by_state(spark):
    rows = [mbsf_row(1, age=70, sex=2, dual=1), mbsf_row(2, age=80, sex=1), mbsf_row(3, buyIn="111" + "3" * 9),
            mbsf_row(4, hmo="C" * 12, age=20)]
    by = benchmarks.get_mbsf_by_state(with_ffs(make_mbsf(spark, rows)))[(2019, "39")]
    assert (by["cmsOm"], by["meanAge"], by["femaleShare"], by["dualShare"]) == (2, 75.0, 0.5, 0.5)
    #every cmsOm one is also ffs; of the three ffs, the Part A only one is not cmsOm
    assert (by["cmsOmAlsoFfs"], by["ffsNotCmsOm"]) == (1.0, pytest.approx(1 / 3))

@pytest.mark.parametrize("ccn, serviceType", [("360001", "acute"), ("361301", "acute"), ("364001", "ipf"), ("36S001", "ipf"),
                                              ("36M001", "ipf"), ("362001", None), ("363025", None), ("36T001", None)])
def test_service_type_of_the_ccn(spark, ccn, serviceType):
    df = spark.createDataFrame([(ccn,)], "PROVIDER string")
    assert benchmarks.add_serviceType(df).first()["serviceType"] == serviceType

def claims(spark, rows):
    return spark.createDataFrame(rows, "DSYSRTKY int, PROVIDER string, ADMSN_DT_DAY int, THRU_DT_DAY int, THRU_DT_YEAR int, UTIL_DAY int")

def test_ip_stays_are_continuous_periods_of_one_service_type(spark):
    rows = [(1, "360001", 100, 105, 2019, 5), (1, "360002", 105, 110, 2019, 5),   #a same day transfer: one stay
            (1, "360001", 111, 112, 2019, 1),                                     #the day after: another stay
            (1, "364001", 108, 120, 2019, 12),                                    #psychiatric, its own service type
            (1, "362001", 130, 140, 2019, 10),                                    #LTCH, counted apart
            (1, "360001", 150, 151, 2019, 0),                                     #no covered day
            (2, "361301", 200, 203, 2019, 3)]
    stays = {(r["DSYSRTKY"], r["year"]): r["ipStays"] for r in benchmarks.get_ip_stays(claims(spark, rows)).collect()}
    assert stays == {(1, 2019): 3, (2, 2019): 1}

def test_er_visits_are_claims_with_an_er_line(spark):
    schema = "DSYSRTKY int, CLAIMNO long, THRU_DT int, REV_CNTR int, HCPCS_CD string"
    ip = spark.createDataFrame([(1, 1, 20190105, 450, None), (1, 1, 20190105, 981, None), (1, 2, 20190205, 200, None)], schema)
    op = spark.createDataFrame([(1, 7, 20190301, 459, None), (2, 8, 20180301, 450, None)], schema)
    visits = {(r["DSYSRTKY"], r["year"]): r["erVisits"] for r in benchmarks.get_er_visits([ip, op]).collect()}
    assert visits == {(1, 2019): 2, (2, 2018): 1}

def test_rates_per_1000_cms_original_medicare(spark):
    mbsf = make_mbsf(spark, [mbsf_row(1), mbsf_row(2), mbsf_row(3, hmo="C" * 12)])
    stays = spark.createDataFrame([(1, 2019, 1), (3, 2019, 4)], "DSYSRTKY int, year int, ipStays long")
    visits = spark.createDataFrame([(1, 2019, 2), (2, 2019, 1)], "DSYSRTKY int, year int, erVisits long")
    rates = benchmarks.get_rates_by_state(mbsf, stays, visits)[(2019, "39")]
    assert (rates["ipStaysPer1000"], rates["erVisitsPer1000"]) == (500.0, 1500.0)

def test_the_geographic_variation_extract():
    gv = benchmarks.load_geographic_variation()
    assert gv[(2019, "39")]["BENES_OM_CNT"] == 1159406
    assert {year for (year, _) in gv} >= set(range(2014, 2025))
    assert len({state for (_, state) in gv}) >= 51 and "" not in {state for (_, state) in gv}

def test_ma_share_in_july(spark):
    rows = [mbsf_row(1, hmo="0" * 6 + "C" * 6), mbsf_row(2), mbsf_row(3, hmo="0" * 6 + "4" * 6),
            mbsf_row(4, buyIn="3" * 6 + "0" * 6, hmo="0" * 6 + "C" * 6, deathMonth=6), mbsf_row(5, county="39041")]
    shares = {r["fipsCounty"]: r for r in benchmarks.get_ma_share_by_county(make_mbsf(spark, rows), minBeneficiaries=2)}
    #the one who died in June is not entitled in July, the small county is left out
    assert set(shares) == {"39049"}
    assert (shares["39049"]["beneficiaries"], shares["39049"]["maShare"], shares["39049"]["cmsMaShare"]) == (3, pytest.approx(1 / 3), 0.4)

def pos(spark, rows):
    return spark.createDataFrame(rows, "PRVDR_NUM string, posHospital int, posHospitalType string, ORGNL_PRTCPTN_DT string, "
                                       "PGM_TRMNTN_CD string, TRMNTN_EXPRTN_DT string")

def test_pos_active_in_a_year(spark):
    df = pos(spark, [("360001", 1, "acute", "19660701", "00", None), ("360002", 1, "acute", "20190301", "00", None),
                     ("360003", 1, "acute", "19700101", "01", "20180630"), ("360004", 1, "acute", "19700101", "01", "20170630"),
                     ("360005", 1, "acute", "20190101", "00", None), ("365001", 0, None, "19900101", "00", None)])
    assert {r["PRVDR_NUM"] for r in benchmarks.get_pos_active(df, 2018).collect()} == {"360001", "360003"}

def test_hospital_coverage_both_ways(spark):
    df = pos(spark, [("360001", 1, "acute", "19660701", "00", None), ("360002", 1, "acute", "19660701", "00", None),
                     ("361301", 1, "cah", "20010101", "00", None)])
    ip = claims(spark, [(1, "360001", 1, 2, 2019, 1), (2, "360009", 1, 2, 2019, 1), (3, "361301", 1, 2, 2019, 1)])
    coverage = benchmarks.get_hospital_coverage(ip, df, [2019])
    assert coverage == {(2019, "acute"): (0.5, 0.5), (2019, "cah"): (1.0, 1.0)}

def test_check_band_records_the_extremes_and_names_what_is_outside():
    metrics = Metrics()
    values = {(2019, "OH"): 1.01, (2019, "AK"): 0.9, (2018, "OH"): 1.0, (2018, "PR"): None}
    message = check_band(metrics, "geographic variation", "omCountRatio", values, (0.98, 1.02))
    assert message.startswith("geographic variation omCountRatio: 1 outside") and "2019 AK 0.9" in message
    assert sorted(r[1:4] for r in metrics.rows) == [("omCountRatio highest", 2018, 1.0), ("omCountRatio highest", 2019, 1.01),
                                                    ("omCountRatio lowest", 2018, 1.0), ("omCountRatio lowest", 2019, 0.9)]
    assert check_band(Metrics(), "c", "k", {(2019, "OH"): 1.0}, (0.98, 1.02)) is None
    assert "nothing to compare" in check_band(Metrics(), "c", "k", {}, (0, 1))

def test_check_rates_fails_on_a_rate_of_none():
    #every claim of a year with no through date (the snf files of 2019, once) gave a rate of None, which raised TypeError
    with pytest.raises(AssertionError, match="None"):
        check_rates(Metrics(), "through <= death", "snfBase", {None: None, 2018: 0.99})

def test_check_band_takes_a_group_with_no_label():
    #beneficiaries with no state are a group of their own, labelled None
    message = check_band(Metrics(), "ffs definitions", "cmsOmAlsoFfs", {(2019, "39"): 0.999, (2019, None): 0.5}, (0.9, 1.0))
    assert "2019 None 0.5" in message
