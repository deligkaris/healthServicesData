'''The library run on the real data: its joins keep the row counts, its added columns get filled every year and follow
the naming convention of CLAUDE.md.'''
import re

import pytest

import cms.base as baseF
import cms.claims as claimsF
import cms.revenue as revenueF
import cms.stays as staysF
from cms.schemas import schemas
from tests.data.checks import get_null_rates, check_rates
from tests.data.thresholds import nullableAddedColumns, namingExceptions

rawColumns = {c for schema in schemas.values() for c in schema.fieldNames()}
#the date parts of a raw date keep its upper case name (see CLAUDE.md)
datePart = re.compile(r"^[A-Z0-9_]+_(YEAR|MONTH|DAY)$")

def on_ip_stays(enrich):
    '''An enricher of the ip stays built from the claims, see enrichers.'''
    def run(d, last):
        stays = staysF.get_stays(d["ipBase"], revenueF.get_revenue_info(d["ipRevenue"], inClaim=True), claimType="ip")
        return stays, enrich(stays, d)
    return run

#name -> f(cmsData sample, lastObservableDay) returning (input, output) of an enricher that must keep one row per input row
enrichers = {
    "base.add_los_at_X_info": lambda d, last: (d["ipBase"], baseF.add_los_at_X_info(d["ipBase"], d["hospBase"], last)),
    "base.add_days_at_home_info": lambda d, last: (d["ipBase"], baseF.add_days_at_home_info(
        d["ipBase"], d["snfBase"], d["hhaBase"], d["hospBase"], d["ipBase"], last)),
    "base.add_prior_hospitalization_info": lambda d, last: (d["ipBase"],
                                                            baseF.add_prior_hospitalization_info(d["ipBase"], d["ipBase"])),
    "claims.get_claims": lambda d, last: (d["ipBase"], claimsF.get_claims(
        d["ipBase"], revenueF.get_revenue_info(d["ipRevenue"], inClaim=True))),
    "stays.add_comorbidity_info": on_ip_stays(lambda s, d: staysF.add_comorbidity_info(s, d["ipBase"], d["opBase"])),
    "stays.add_hospiceIn12Months": on_ip_stays(lambda s, d: staysF.add_hospiceIn12Months(s, d["hospBase"])),
    "stays.add_source_and_destination_info": on_ip_stays(lambda s, d: staysF.add_source_and_destination_info(s, d))}

def test_preliminary_info_keeps_rows(cmsPreliminary, claimTypePart):
    #add_preliminary_info joins NPPES, POS, AHA, HCRIS, ..., the mbsf and the death dates onto the claims
    (before, after) = cmsPreliminary
    (initial, final) = (before[claimTypePart].count(), after[claimTypePart].count())
    assert final == initial, f"{claimTypePart}: {initial} rows before add_preliminary_info, {final} after"

@pytest.mark.parametrize("name", list(enrichers))
def test_enricher_keeps_rows(sampledCmsData, lastObservableDay, name):
    (before, after) = enrichers[name](sampledCmsData, lastObservableDay)
    (initial, final) = (before.count(), after.count())
    assert final == initial, f"{name}: {initial} rows in, {final} out"

def test_added_columns_filled_every_year(sampledCmsData, claimTypePart):
    #a source missing a year, or a key format that changed, leaves its columns all null for that year
    df = sampledCmsData[claimTypePart]
    added = [c for c in df.columns if c[0].islower()]
    if not added:
        return
    by = "RFRNC_YR" if claimTypePart == "mbsf" else "THRU_DT_YEAR"
    allowed = nullableAddedColumns.get(claimTypePart, set())
    empty = {year: [c for c, rate in rates.items() if rate == 1.0 and c not in allowed]
             for year, rates in get_null_rates(df, added, by).items()}
    empty = {year: cols for year, cols in empty.items() if cols}
    assert not empty, f"{claimTypePart}: columns all null in a year: {empty}"

def test_column_naming(cmsData, claimTypePart):
    #anything not a raw CMS field or a date part of one must start lower case, revenue.get_revenue_summary and
    #line.get_line_summary select the added columns by that
    bad = [c for c in cmsData[claimTypePart].columns
           if not (c[0].islower() or c in rawColumns or datePart.match(c) or c in namingExceptions)]
    assert not bad, f"{claimTypePart}: added columns not starting lower case: {bad}"

def test_ffs_share(cmsRaw, cmsData, metricsTable):
    #the share of the mbsf beneficiary-years get_cms_data keeps (FFS, continuous FFS and reference year)
    raw = {r["RFRNC_YR"]: r["count"] for r in cmsRaw["mbsf"].groupBy("RFRNC_YR").count().collect()}
    kept = {r["RFRNC_YR"]: r["count"] for r in cmsData["mbsf"].groupBy("RFRNC_YR").count().collect()}
    check_rates(metricsTable, "ffs kept", "mbsf", {year: kept.get(year, 0) / n for year, n in raw.items()})
