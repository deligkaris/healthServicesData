'''The library's numbers against figures CMS publishes: the Original Medicare population of the Geographic Variation PUF
and its rates by state, the July MA penetration by county, the hospitals of the POS file. Each comparison is recorded in
the metrics table and fails outside its band (thresholds.benchmarkBands); the measures are in benchmarks.py.

CMS and the library define fee-for-service differently: the library's ffs (mbsf.add_allPartAB, add_hmo) asks for Part A
and Part B from the first such month of the year through December or death and no MA month, CMS's Original Medicare for
Part A and Part B in every month entitled and no MA month (someone with Part A only before adding Part B is ffs here and
not in CMS's). So the Geographic Variation comparisons are made on CMS's definition rebuilt from the same mbsf fields
(benchmarks.add_cmsOm), where a gap can only come from the data or how it is read, and the overlap of the two
definitions is recorded apart (test_ffs_definitions). All on the claims before filter_FFS (cmsPreliminary).'''
import pytest

from tests.data import benchmarks
from tests.data.checks import check_band
from tests.data.thresholds import get_band

@pytest.fixture(scope="module")
def preliminary(cmsPreliminary):
    '''What add_preliminary_info returns: every beneficiary and claim, before filter_FFS.'''
    return cmsPreliminary[1]

@pytest.fixture(scope="module")
def mbsfByState(preliminary):
    return benchmarks.get_mbsf_by_state(preliminary["mbsf"])

@pytest.fixture(scope="module")
def geographicVariation():
    return benchmarks.load_geographic_variation()

def compare(metrics, check, measures):
    '''measures: {key: {(year, label): value}}; asserts once, after every key is recorded.'''
    failures = [check_band(metrics, check, key, values, get_band(check, key)) for key, values in measures.items()]
    failures = [f for f in failures if f is not None]
    assert not failures, "\n".join(failures)

def in_both(ours, theirs, column):
    '''The (year, state) of ours with a CMS value of column.'''
    return [k for k in ours if k in theirs and theirs[k][column] is not None]

def test_om_population_matches_geographic_variation(metricsTable, mbsfByState, geographicVariation):
    (ours, gv) = (mbsfByState, geographicVariation)
    compare(metricsTable, "geographic variation", {
        "omCountRatio": {k: ours[k]["cmsOm"] / gv[k]["BENES_OM_CNT"] for k in in_both(ours, gv, "BENES_OM_CNT")},
        "meanAgeDifference": {k: ours[k]["meanAge"] - gv[k]["BENE_AVG_AGE"] for k in in_both(ours, gv, "BENE_AVG_AGE")},
        "femaleShareDifference": {k: ours[k]["femaleShare"] - gv[k]["BENE_FEML_PCT"] for k in in_both(ours, gv, "BENE_FEML_PCT")},
        "dualShareDifference": {k: ours[k]["dualShare"] - gv[k]["BENE_DUAL_PCT"] for k in in_both(ours, gv, "BENE_DUAL_PCT")}})

def test_ip_stays_and_er_visits_match_geographic_variation(metricsTable, preliminary, geographicVariation):
    stays = benchmarks.get_ip_stays(preliminary["ipBase"])
    visits = benchmarks.get_er_visits([preliminary["ipRevenue"], preliminary["opRevenue"]])
    (ours, gv) = (benchmarks.get_rates_by_state(preliminary["mbsf"], stays, visits), geographicVariation)
    relative = lambda k, mine, column: ours[k][mine] / gv[k][column] - 1
    compare(metricsTable, "geographic variation", {
        "ipStaysRelativeDifference": {k: relative(k, "ipStaysPer1000", "IP_CVRD_STAYS_PER_1000_BENES")
                                      for k in in_both(ours, gv, "IP_CVRD_STAYS_PER_1000_BENES")},
        "erVisitsRelativeDifference": {k: relative(k, "erVisitsPer1000", "ER_VISITS_PER_1000_BENES")
                                       for k in in_both(ours, gv, "ER_VISITS_PER_1000_BENES")}})

def test_ffs_definitions(metricsTable, mbsfByState):
    #by design they differ; the loose bands only catch a sudden change of the library's ffs
    compare(metricsTable, "ffs definitions", {key: {k: v[key] for k, v in mbsfByState.items()}
                                              for key in ("cmsOmAlsoFfs", "ffsNotCmsOm")})

def test_ma_share_matches_cms_penetration(metricsTable, preliminary):
    rows = [r for r in benchmarks.get_ma_share_by_county(preliminary["mbsf"]) if r["cmsMaShare"] is not None]
    years = sorted({r["RFRNC_YR"] for r in rows})
    inYear = lambda year: [r for r in rows if r["RFRNC_YR"] == year]
    #only these two summaries of the counties are shown, never a county's own value
    meanAbsolute = {(y, "counties"): sum(r["beneficiaries"] * abs(r["maShare"] - r["cmsMaShare"]) for r in inYear(y)) /
                                     sum(r["beneficiaries"] for r in inYear(y)) for y in years}
    within = {(y, "counties"): sum(abs(r["maShare"] - r["cmsMaShare"]) <= 0.03 for r in inYear(y)) / len(inYear(y)) for y in years}
    compare(metricsTable, "ma penetration", {"meanAbsoluteDifference": meanAbsolute, "countiesWithin3Points": within})

def test_hospital_coverage_of_the_pos_file(metricsTable, preliminary, externalData, yearList):
    coverage = benchmarks.get_hospital_coverage(preliminary["ipBase"], externalData["pos"], yearList)
    compare(metricsTable, "hospital coverage", {
        f"{measure} {kind}": {(year, kind): coverage.get((year, kind), (None, None))[i] for year in yearList}
        for (i, measure) in enumerate(["posWithClaim", "claimsInPos"]) for kind in ("acute", "cah")})
