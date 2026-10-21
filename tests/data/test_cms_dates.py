'''The CMS dates against the day-number frame of utilities.py and the admission <= through <= death order the
length of stay, days at home and mortality code assumes.'''
import pytest
import pyspark.sql.functions as F

import cms.base as baseF
import cms.mbsf as mbsfF
from tests.data.checks import get_counts, get_examples, check_rates

#(claimTypePart, raw date, the day number derived from it): a date that does not parse as YYYYMMDD, or falls outside
#daysInYearsPriorDict, gets a null day number
dateDays = ([(part, "THRU_DT", "THRU_DT_DAY") for part in ["ipBase", "opBase", "snfBase", "hhaBase", "hospBase", "carBase",
                                                             "ipRevenue", "opRevenue", "carLine"]] +
            [("ipBase", "ADMSN_DT", "ADMSN_DT_DAY"), ("ipBase", "DSCHRGDT", "DSCHRGDT_DAY"),
             ("snfBase", "ADMSN_DT", "ADMSN_DT_DAY"), ("hhaBase", "HHSTRTDT", "ADMSN_DT_DAY"),
             ("hospBase", "HSPCSTRT", "ADMSN_DT_DAY")])
stayBases = ["ipBase", "snfBase", "hhaBase", "hospBase"]

@pytest.mark.parametrize("part, dateCol, dayCol", dateDays, ids=[f"{p}-{d}" for p, d, _ in dateDays])
def test_date_has_a_day_number(cmsRaw, part, dateCol, dayCol):
    df = cmsRaw[part]
    lost = F.col(dateCol).isNotNull() & F.col(dayCol).isNull()
    number = get_counts(df, {"lost": lost})["lost"]
    assert number == 0, f"{part}: {number} {dateCol} with no {dayCol}, eg {get_examples(df, lost, [dateCol])}"

def test_valid_death_date_has_a_day_number(cmsRaw):
    df = cmsRaw["mbsf"]
    lost = (F.col("V_DOD_SW") == "V") & F.col("DEATH_DT").isNotNull() & F.col("DEATH_DT_DAY").isNull()
    number = get_counts(df, {"lost": lost})["lost"]
    assert number == 0, f"mbsf: {number} valid DEATH_DT with no DEATH_DT_DAY, eg {get_examples(df, lost, ['DEATH_DT'])}"

def test_death_date_is_not_zero(cmsRaw):
    #mbsf.add_willDie counts any non-null DEATH_DT as a death
    number = get_counts(cmsRaw["mbsf"], {"zero": F.col("DEATH_DT") == 0})["zero"]
    assert number == 0, f"mbsf: {number} DEATH_DT == 0"

def test_part_ab_months_before_death(cmsRaw):
    #mbsf.add_allPartAB slices the months from the first Part A/B month to the death month and raises on a death
    #before that first month; evaluating it on every row is the test
    mbsf = mbsfF.add_allPartAB(cmsRaw["mbsf"])
    mbsf.agg(F.sum("allPartAB")).collect()

def test_one_death_date_per_beneficiary(cmsRaw, metricsTable):
    #base.add_death_date_info keeps the latest RFRNC_YR's date when the years disagree; reported, not failed
    deaths = cmsRaw["mbsf"].filter(F.col("V_DOD_SW") == "V").select("DSYSRTKY", "DEATH_DT").distinct()
    number = deaths.groupBy("DSYSRTKY").count().filter(F.col("count") > 1).count()
    metricsTable.add("death dates disagree", "mbsf", None, number)

@pytest.mark.parametrize("part", stayBases)
def test_admission_on_or_before_through(cmsRaw, metricsTable, part):
    #get_cms_data drops these (cleanThroughDates); a growing share points at a date field read wrongly
    rows = (cmsRaw[part].filter(F.col("ADMSN_DT_DAY").isNotNull())
            .groupBy("THRU_DT_YEAR")
            .agg(F.avg((F.col("ADMSN_DT_DAY") <= F.col("THRU_DT_DAY")).cast("int")).alias("rate"))
            .collect())
    check_rates(metricsTable, "admission <= through", part, {r["THRU_DT_YEAR"]: r["rate"] for r in rows})

@pytest.mark.parametrize("part", ["ipBase", "opBase", "snfBase", "hhaBase", "hospBase", "carBase"])
def test_through_on_or_before_death(cmsRaw, metricsTable, part):
    #clean_through_dates drops the ip, snf, hha and hospice claims dated after death, op and carrier are kept as they are
    df = baseF.add_death_date_info(cmsRaw[part].select("DSYSRTKY", "THRU_DT_YEAR", "THRU_DT_DAY"), cmsRaw["mbsf"])
    rows = (df.filter(F.col("DEATH_DT_DAY").isNotNull())
              .groupBy("THRU_DT_YEAR")
              .agg(F.avg((F.col("THRU_DT_DAY") <= F.col("DEATH_DT_DAY")).cast("int")).alias("rate"))
              .collect())
    check_rates(metricsTable, "through <= death", part, {r["THRU_DT_YEAR"]: r["rate"] for r in rows})

def test_hha_start_date_filled(cmsRaw, metricsTable):
    #HHSTRTDT is ADMSN_DT for hha (base.add_admission_date_info); null on almost every claim, which is why home health
    #is left out of stays.get_otherStays and base.add_days_at_home_info's losAtall misses it
    rows = (cmsRaw["hhaBase"].groupBy("THRU_DT_YEAR")
            .agg(F.avg(F.col("HHSTRTDT").isNotNull().cast("int")).alias("rate"))
            .collect())
    check_rates(metricsTable, "HHSTRTDT filled", "hhaBase", {r["THRU_DT_YEAR"]: r["rate"] for r in rows})
