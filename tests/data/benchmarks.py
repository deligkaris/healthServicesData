'''The measures test_benchmarks.py compares with figures CMS publishes, as functions of dataframes so they can be tested
on small frames: the Original Medicare population of the Geographic Variation PUF rebuilt from the mbsf and its rates by
state, the July MA share by county, the hospitals of the POS file active in a year and the share of them with a claim.
Sources:
  Geographic Variation: 2014-2024 Original Medicare Geographic Variation Data Dictionary and Methods Paper, data.cms.gov
  (see scripts/05_geographic_variation.py)
  mbsf monthly codes: BUYIN01 and HMO_IND_01, https://bluebutton.cms.gov/resources/variables/
  POS: the Provider of Services File - Quality Improvement and Evaluation System, data.cms.gov'''
import csv
from functools import reduce
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql.window import Window

import cms.revenue as revenueF
import utilities as utilitiesF

geographicVariationFile = Path(__file__).parent / "benchmarks" / "geographicVariationState.csv"
months = range(1, 13)
#BUYIN: 0 not entitled, 3 and C Part A and Part B (C a state buy-in); HMOIND: 0 not in an HMO, 4 a fee-for-service
#participant of a demonstration, every other code a Medicare Advantage (GHO) enrollment
partABCodes = ["3", "C"]
notMaCodes = ["0", "4"]
#the Geographic Variation service types of inpatient hospital care, from the CCN: acute (IPPS hospitals and CAHs) and
#psychiatric, the hospitals (utilities.add_posHospitalType) and the units of an acute hospital or CAH (S and M in the
#third place); LTCH and IRF are counted apart and left out
acuteTypes = ["acute", "cah"]
psychiatricUnits = ["S", "M"]

def _any(conditions):
    return reduce(lambda a, b: a | b, conditions)

def _all(conditions):
    return reduce(lambda a, b: a & b, conditions)

def add_cmsOm(mbsfDF):
    '''cmsOm: 1 for a beneficiary-year in the Original Medicare population of the Geographic Variation PUF, Part A and
    Part B in every month entitled to Medicare and no month in Medicare Advantage; new enrollees and decedents are in.'''
    #the methods paper excludes a beneficiary with Part A and B and an MA plan at any point in the year, and one enrolled
    #at any point in Part A only or Part B only; a month not entitled (BUYIN 0) counts for neither
    buyIn = [F.col(f"BUYIN{m}") for m in months]
    entitled = [b.isNotNull() & (b != "0") for b in buyIn]
    partAB = [b.isin(partABCodes) for b in buyIn]
    ma = [F.col(f"HMOIND{m}").isNotNull() & ~F.col(f"HMOIND{m}").isin(notMaCodes) for m in months]
    om = _any(entitled) & _all([~e | ab for (e, ab) in zip(entitled, partAB)]) & ~_any(ma)
    return mbsfDF.withColumn("cmsOm", F.when(om, 1).otherwise(0))

def get_mbsf_by_state(mbsfDF):
    '''Per (RFRNC_YR, fipsState) of the beneficiaries in cmsOm: their number, mean AGE, female and dual (medicaidEver)
    shares, and how the library's ffs relates to cmsOm: cmsOmAlsoFfs (the share of cmsOm with ffs) and ffsNotCmsOm (the
    share of ffs without cmsOm).'''
    df = add_cmsOm(mbsfDF)
    (om, ffs) = (F.col("cmsOm") == 1, F.col("ffs") == 1)
    rows = (df.groupBy("RFRNC_YR", "fipsState")
              .agg(F.sum(om.cast("int")).alias("cmsOm"),
                   F.avg(F.when(om, F.col("AGE"))).alias("meanAge"),
                   F.avg(F.when(om, (F.col("SEX") == 2).cast("int"))).alias("femaleShare"),
                   F.avg(F.when(om, F.col("medicaidEver"))).alias("dualShare"),
                   F.avg(F.when(om, ffs.cast("int"))).alias("cmsOmAlsoFfs"),
                   F.avg(F.when(ffs, (~om).cast("int"))).alias("ffsNotCmsOm"))
              .collect())
    return {(r["RFRNC_YR"], r["fipsState"]): r.asDict() for r in rows}

def add_hospital_type(claimsDF):
    '''posHospitalType of the claim's CCN (PROVIDER), as utilities.add_posHospitalType gives it to a POS hospital.'''
    typed = utilitiesF.add_posHospitalType(claimsDF.withColumn("PRVDR_NUM", F.col("PROVIDER")).withColumn("posHospital", F.lit(1)))
    return typed.drop("PRVDR_NUM", "posHospital")

def add_serviceType(claimsDF):
    '''serviceType of each claim from its CCN (PROVIDER): acute, ipf, or null for the inpatient care the Geographic
    Variation counts apart or not at all.'''
    typed = add_hospital_type(claimsDF)
    serviceType = (F.when(F.col("posHospitalType").isin(acuteTypes), "acute")
                    .when((F.col("posHospitalType") == "psychiatric") |
                          F.substring(F.col("PROVIDER"), 3, 1).isin(psychiatricUnits), "ipf"))
    return typed.withColumn("serviceType", serviceType).drop("posHospitalType")

def get_ip_stays(ipBaseDF):
    '''Inpatient hospital covered stays per (DSYSRTKY, year): the Geographic Variation stay, a continuous uninterrupted
    period of claims of one service type, here claims with covered days (UTIL_DAY > 0); the year of its first claim.'''
    claims = (add_serviceType(ipBaseDF.filter(F.col("UTIL_DAY") > 0))
              .filter(F.col("serviceType").isNotNull())
              .withColumn("start", F.coalesce(F.col("ADMSN_DT_DAY"), F.col("THRU_DT_DAY"))))
    eachBeneficiaryType = Window.partitionBy("DSYSRTKY", "serviceType").orderBy("start", "THRU_DT_DAY")
    before = eachBeneficiaryType.rowsBetween(Window.unboundedPreceding, -1)
    #a claim starts a stay unless an earlier claim of the same type runs up to or past its start
    claims = claims.withColumn("startsStay", (F.max("THRU_DT_DAY").over(before).isNull() |
                                              (F.col("start") > F.max("THRU_DT_DAY").over(before))).cast("int"))
    return (claims.filter(F.col("startsStay") == 1)
                  .groupBy("DSYSRTKY", F.col("THRU_DT_YEAR").alias("year")).count()
                  .withColumnRenamed("count", "ipStays"))

def get_er_visits(revenueDFs):
    '''ER visits per (DSYSRTKY, year): the inpatient and outpatient claims with an emergency room revenue line
    (revenue.add_ed: 0450-0459, 0981), one visit a claim.'''
    claims = [revenueF.get_revenue_info(df).filter(F.col("ed") == 1).select("DSYSRTKY", "CLAIMNO", "THRU_DT") for df in revenueDFs]
    return (reduce(lambda a, b: a.unionByName(b), claims)
              .groupBy("DSYSRTKY", F.floor(F.col("THRU_DT") / 10000).cast("int").alias("year")).count()
              .withColumnRenamed("count", "erVisits"))

def get_rates_by_state(mbsfDF, ipStaysDF, erVisitsDF):
    '''Per (RFRNC_YR, fipsState): ipStaysPer1000 and erVisitsPer1000 cmsOm beneficiaries, counting their stays and
    visits of that year.'''
    beneficiaries = add_cmsOm(mbsfDF).filter(F.col("cmsOm") == 1).select("DSYSRTKY", "RFRNC_YR", "fipsState")
    events = [df.withColumnRenamed("year", "RFRNC_YR") for df in (ipStaysDF, erVisitsDF)]
    rows = (reduce(lambda a, b: a.join(b, on=["DSYSRTKY", "RFRNC_YR"], how="left_outer"), events, beneficiaries)
              .groupBy("RFRNC_YR", "fipsState")
              .agg((1000 * F.sum(F.coalesce("ipStays", F.lit(0))) / F.count(F.lit(1))).alias("ipStaysPer1000"),
                   (1000 * F.sum(F.coalesce("erVisits", F.lit(0))) / F.count(F.lit(1))).alias("erVisitsPer1000"))
              .collect())
    return {(r["RFRNC_YR"], r["fipsState"]): r.asDict() for r in rows}

def load_geographic_variation(path=geographicVariationFile):
    '''{(year, state FIPS): {column: value}} of the extract, numbers as float, None where CMS suppressed or left blank.'''
    def number(text):
        try:
            return float(text)
        except ValueError:
            return None
    with open(path, newline="", encoding="utf-8") as f:
        return {(int(r["YEAR"]), r["BENE_GEO_CD"]): {k: number(v) for k, v in r.items() if k not in ("YEAR", "BENE_GEO_CD", "BENE_GEO_DESC")}
                for r in csv.DictReader(f) if r["BENE_GEO_CD"]}

def get_ma_share_by_county(mbsfDF, minBeneficiaries=1000):
    '''Per (RFRNC_YR, fipsCounty) with at least minBeneficiaries entitled in July: their number, the share of them in
    Medicare Advantage in July, and the county's maPenetration (CMS's July file, a whole percent) as a share.'''
    july = mbsfDF.filter(F.col("BUYIN7").isNotNull() & (F.col("BUYIN7") != "0"))
    rows = (july.groupBy("RFRNC_YR", "fipsCounty")
                .agg(F.count(F.lit(1)).alias("beneficiaries"),
                     F.avg((F.col("HMOIND7").isNotNull() & ~F.col("HMOIND7").isin(notMaCodes)).cast("int")).alias("maShare"),
                     (F.max("maPenetration") / 100).alias("cmsMaShare"))
                .filter(F.col("beneficiaries") >= minBeneficiaries)
                .collect())
    return [r.asDict() for r in rows]

def get_pos_active(posDF, year):
    '''The hospitals of the POS file participating in Medicare in year, (PRVDR_NUM, posHospitalType): participating
    from before its end (ORGNL_PRTCPTN_DT) and active or terminated after its start (PGM_TRMNTN_CD, TRMNTN_EXPRTN_DT).'''
    #the dates are yyyymmdd strings
    (start, end) = (f"{year}0101", f"{year}1231")
    return (posDF.filter((F.col("posHospital") == 1) & (F.col("ORGNL_PRTCPTN_DT") <= end) &
                         ((F.col("PGM_TRMNTN_CD") == "00") | (F.col("TRMNTN_EXPRTN_DT") >= start)))
                 .select("PRVDR_NUM", "posHospitalType"))

def get_hospital_coverage(ipBaseDF, posDF, years, types=("acute", "cah")):
    '''{(year, type): (posWithClaim, claimsInPos)}: the share of the POS hospitals of that type active that year with an
    ip claim through that year, and the share of the CCNs of that type with an ip claim that year that are such
    hospitals.'''
    claimCcns = (add_hospital_type(ipBaseDF.select("PROVIDER", "THRU_DT_YEAR").distinct())
                 .select(F.col("PROVIDER").alias("PRVDR_NUM"), "THRU_DT_YEAR", "posHospitalType"))
    coverage = {}
    for year in years:
        pos = get_pos_active(posDF, year).filter(F.col("posHospitalType").isin(list(types))).withColumn("inPos", F.lit(1))
        claims = (claimCcns.filter(F.col("THRU_DT_YEAR") == year).filter(F.col("posHospitalType").isin(list(types)))
                           .select("PRVDR_NUM", "posHospitalType").distinct().withColumn("withClaim", F.lit(1)))
        rows = (pos.join(claims, on=["PRVDR_NUM", "posHospitalType"], how="full_outer")
                   .groupBy("posHospitalType")
                   .agg(F.avg(F.when(F.col("inPos") == 1, F.coalesce("withClaim", F.lit(0)))).alias("posWithClaim"),
                        F.avg(F.when(F.col("withClaim") == 1, F.coalesce("inPos", F.lit(0)))).alias("claimsInPos"))
                   .collect())
        coverage.update({(year, r["posHospitalType"]): (r["posWithClaim"], r["claimsInPos"]) for r in rows})
    return coverage
