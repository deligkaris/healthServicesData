'''The code values cms/*.py tests for, against the values in the CMS files: formats, the codes themselves, and how
often each flag fires from year to year.'''
import warnings
from functools import reduce

import pytest
import pyspark.sql.functions as F

import cms.base as baseF
import cms.revenue as revenueF
import cms.line as lineF
import cms.comorbidities as comorbiditiesF
from utilities import icd10Day
from tests.data.checks import get_counts, get_examples, check_rates
from tests.data.thresholds import keyFormatExceptions, prevalenceYearRatio

ccn = r"^[0-9A-Z]{6}$"
npi = r"^\d{10}$"
facilityBases = ["ipBase", "opBase", "snfBase", "hhaBase", "hospBase"]

#(claimTypePart, column, format) of the claim side of the joins and code lookups
claimFormats = ([(part, "PROVIDER", ccn) for part in facilityBases] +
                [(part, "ORGNPINM", npi) for part in facilityBases] +
                [(part, "ssaCounty", r"^\d{5}$") for part in facilityBases + ["carBase", "mbsf"]] +
                [("carLine", "PRF_NPI", npi), ("carLine", "HCPCS_CD", r"^[0-9A-Z]{5}$"),
                 ("ipRevenue", "HCPCS_CD", r"^[0-9A-Z]{5}$"), ("opRevenue", "HCPCS_CD", r"^[0-9A-Z]{5}$"),
                 ("ipRevenue", "REV_CNTR", r"^[1-9]\d{0,3}$"), ("opRevenue", "REV_CNTR", r"^[1-9]\d{0,3}$")])

#every code list matches by exact value or prefix, so a dot, a space or a lower case letter would match nothing
icdFormats = {**{f"ICD_DGNS_CD{x}": r"^[A-Z][0-9][0-9A-Z]{1,5}$" for x in range(1, 26)},
              "PRNCPAL_DGNS_CD": r"^[A-Z][0-9][0-9A-Z]{1,5}$",
              **{f"ICD_PRCDR_CD{x}": r"^[0-9A-Z]{7}$" for x in range(1, 26)}}

#(claimTypePart, column, values, where): each value the code compares the column with, written as the code writes
#it; a tuple is satisfied by any of its values
valuesTested = [("ipBase", "STUS_CD", [1, 2, 5, 20], "base.add_diedInVisit, add_transferToIn, get_homeDays"),
                ("ipBase", "SRC_ADMS", [4], "base.add_transferFromDifferentFacility"),
                ("ipBase", "FAC_TYPE", [1], "base.add_hospitalFromClaim"),
                ("ipBase", "DRG_CD", [61, 62, 63, 23, 24], "base.add_ishStrokeDrg, add_evtDrg"),
                ("carBase", "PMTDNLCD", [("0", "00")], "base.add_denied"),
                ("carLine", "PRCNGIND", ["A"], "line.add_allowed"),
                ("carLine", "HCFASPCL", ["01", "08", "11", "13", "38", "62", "68", "86"], "line.add_pcp ... add_geriatric"),
                ("mbsf", "V_DOD_SW", ["V"], "mbsf.add_death_date_info"),
                ("mbsf", "BUYIN1", ["3", "C"], "mbsf.add_allPartAB"),
                ("mbsf", "HMO_MO", [0], "mbsf.add_hmo"),
                ("mbsf", "DUAL_01", [(1, 2, 3, 4, 5, 6, 8)], "mbsf.add_medicaidEver"),
                ("mbsf", "ESRD_IND", ["Y"], "mbsf.add_anyEsrd"),
                ("mbsf", "OREC", [2, 3], "mbsf.add_anyEsrd"),
                ("mbsf", "MDCR_STUS_CD_01", [11, 21, 31], "mbsf.add_anyEsrd")]

#the flags whose prevalence is followed from year to year, by the file they are computed on
ipFlags = ["ishStroke", "ichStroke", "tiaStroke", "otherStroke", "septicShock", "shockDgns", "acuteRespiratoryFailure",
           "acuteNeurologicalFailure", "coagulopathy", "acuteHepaticInjuryFailure", "acuteRenalInjuryFailure",
           "acidosis", "imv", "ecmo", "rrt", "endoscopy", "intubation", "tracheostomy", "peg", "parkinsons"]
revenueFlags = {"ipRevenue": ["ed", "icu", "mri", "ct"], "opRevenue": ["ed", "mri", "ct", "echo"]}
lineFlags = ["pcp", "neurology", "neuropsychiatry", "geriatric", "opVisit", "alfVisit", "homeVisit", "ct", "mri",
             "nptesVisit"]

def by_col(claimTypePart):
    return "RFRNC_YR" if claimTypePart == "mbsf" else "THRU_DT_YEAR"

def icd10_claims(df):
    '''The claims coded in ICD-10, which started on October 1 2015.'''
    return df.filter(F.col("THRU_DT_DAY") >= icd10Day)

def matching(regexp):
    #F.exists reads the lambda's arity, so the pattern cannot ride in as a default argument
    return lambda x: x.rlike(regexp)

def check_prevalence(name, prevalence):
    '''prevalence is {year: {flag: share of rows}}; every flag fires every year and changes little between years.'''
    years = sorted(prevalence)
    never = {year: [f for f, p in prevalence[year].items() if not p] for year in years}
    never = {year: flags for year, flags in never.items() if flags}
    jumps = {}
    for (y0, y1) in zip(years, years[1:]):
        for f, p1 in prevalence[y1].items():
            p0 = prevalence[y0][f]
            if p0 and p1 and max(p0 / p1, p1 / p0) > prevalenceYearRatio:
                jumps[f"{f} {y0}->{y1}"] = f"{p0:.2e}->{p1:.2e}"
    assert not never and not jumps, f"{name}: never fire {never}; change over {prevalenceYearRatio}x {jumps}"

@pytest.mark.parametrize("part, column, pattern", claimFormats, ids=[f"{p}-{c}" for p, c, _ in claimFormats])
def test_claim_key_format(cmsRaw, part, column, pattern):
    df = cmsRaw[part]
    value = F.col(column).cast("string")
    bad = value.isNotNull() & ~value.rlike(pattern)
    number = get_counts(df, {"bad": bad})["bad"]
    allowed = keyFormatExceptions.get((part, column), 0)
    examples = get_examples(df, bad, [column]) if number > allowed else []
    assert number <= allowed, f"{part}.{column}: {number} values not {pattern}, eg {examples}"

@pytest.mark.parametrize("part", ["ipBase", "opBase"])
def test_icd_code_format(cmsRaw, part):
    df = icd10_claims(cmsRaw[part])
    cols = [c for c in icdFormats if c in df.columns]
    bad = get_counts(df, {c: F.col(c).isNotNull() & ~F.col(c).rlike(icdFormats[c]) for c in cols})
    bad = {c: n for c, n in bad.items() if n}
    first = next(iter(bad), None)
    examples = get_examples(df, F.col(first).isNotNull() & ~F.col(first).rlike(icdFormats[first]), [first]) if first else []
    assert not bad, f"{part}: ICD codes not in the unpunctuated upper case format: {bad}, eg {examples}"

@pytest.mark.parametrize("part", ["ipBase", "opBase"])
def test_principal_is_first_diagnosis(cmsRaw, metricsTable, part):
    #comorbidities.get_dayDgnsDF takes ICD_DGNS_CD1 as the principal diagnosis
    rows = (icd10_claims(cmsRaw[part]).filter(F.col("PRNCPAL_DGNS_CD").isNotNull())
            .groupBy("THRU_DT_YEAR")
            .agg(F.avg((F.col("PRNCPAL_DGNS_CD") == F.col("ICD_DGNS_CD1")).cast("int")).alias("rate"))
            .collect())
    check_rates(metricsTable, "principal is dgns 1", part, {r["THRU_DT_YEAR"]: r["rate"] for r in rows})

@pytest.mark.parametrize("part, column, values, where", valuesTested, ids=[f"{p}-{c}" for p, c, _, _ in valuesTested])
def test_tested_values_occur(cmsRaw, yearList, part, column, values, where):
    #a value the code tests for that never occurs is a code the data spells differently (eg "1" for "01")
    groups = [v if isinstance(v, tuple) else (v,) for v in values]
    by = by_col(part)
    #== as the code compares: a string column against an int casts the column, isin would cast the int
    rows = (cmsRaw[part].groupBy(by)
            .agg(*[F.sum(reduce(lambda x, y: x | y, [F.col(column) == v for v in g]).cast("int")).alias(f"g{i}")
                   for i, g in enumerate(groups)])
            .collect())
    never = {r[by]: [groups[i] for i in range(len(groups)) if not r[f"g{i}"]] for r in rows}
    never = {year: g for year, g in never.items() if g}
    assert not never, f"{part}.{column} never equals, by year: {never} (tested in {where})"

def test_ip_flag_prevalence(cmsRaw):
    df = baseF.add_prcdrCodeAll(baseF.add_dgnsCodeAll(icd10_claims(cmsRaw["ipBase"])))
    for flag in ipFlags:
        df = getattr(baseF, f"add_{flag}")(df)
    rows = df.groupBy("THRU_DT_YEAR").agg(*[F.avg(F.col(f)).alias(f) for f in ipFlags]).collect()
    check_prevalence("ipBase", {r["THRU_DT_YEAR"]: {f: r[f] for f in ipFlags} for r in rows})

@pytest.mark.parametrize("part", list(revenueFlags))
def test_revenue_flag_prevalence(cmsRaw, part):
    df = cmsRaw[part]
    for flag in revenueFlags[part]:
        df = getattr(revenueF, f"add_{flag}")(df)
    rows = df.groupBy("THRU_DT_YEAR").agg(*[F.avg(F.col(f)).alias(f) for f in revenueFlags[part]]).collect()
    check_prevalence(part, {r["THRU_DT_YEAR"]: {f: r[f] for f in revenueFlags[part]} for r in rows})

def test_line_flag_prevalence(cmsRaw):
    df = lineF.add_level1HCPCS_CD(cmsRaw["carLine"])
    for flag in lineFlags:
        df = getattr(lineF, f"add_{flag}")(df)
    rows = df.groupBy("THRU_DT_YEAR").agg(*[F.avg(F.col(f)).alias(f) for f in lineFlags]).collect()
    check_prevalence("carLine", {r["THRU_DT_YEAR"]: {f: r[f] for f in lineFlags} for r in rows})

def test_comorbidity_prevalence(cmsRaw):
    codesRegexp = comorbiditiesF.get_codes_regexp(comorbiditiesF.get_codes())
    df = baseF.add_dgnsCodeAll(icd10_claims(cmsRaw["ipBase"]))
    rows = (df.groupBy("THRU_DT_YEAR")
              .agg(*[F.avg(F.exists("dgnsCodeAll", matching(regexp)).cast("int")).alias(c)
                     for c, regexp in codesRegexp.items()])
              .collect())
    check_prevalence("ipBase comorbidities", {r["THRU_DT_YEAR"]: {c: r[c] for c in codesRegexp} for r in rows})

def test_comorbidity_codes_found(cmsRaw):
    #reported, not failed: a rare code can be absent from a few years of data and still be right
    dgnsCols = [f"ICD_DGNS_CD{x}" for x in range(1, 26)]
    codes = (icd10_claims(cmsRaw["ipBase"]).select(*dgnsCols)
             .unionByName(icd10_claims(cmsRaw["opBase"]).select(*dgnsCols))
             .select(F.explode(F.array(*dgnsCols)).alias("code"))
             .dropna()
             .distinct())
    found = {r["code"] for r in codes.collect()}
    unmatched = [f"{condition}:{code}" for condition, codeList in comorbiditiesF.get_codes().items()
                 for code in codeList if not any(c.startswith(code.rstrip("X")) for c in found)]
    if unmatched:
        warnings.warn(f"comorbidity codes matching no ip/op diagnosis: {unmatched}")
