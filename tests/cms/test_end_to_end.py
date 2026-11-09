'''The whole chain on invented files (tests/cms/synthetic.py): get_data and get_cms_data read them from disk, then get_stays
and get_transfers. The unit tests check each function alone and the data tests the real files; this checks that the pieces
fit, with answers worked out by hand from this cohort, all on two hospitals, 1 sending and 2 receiving:

beneficiary  2018          2019
1            -             stroke at 1 with tPA, CT and NIHSS 12, moved the same day to 2 for EVT and MRI, dies 20 days after
2            -             sepsis at 2, then a SNF, then home health (a file with long column names), STATE_CD stored as 36.0
3            -             Medicare Advantage: dropped by the FFS filter
4            -             a stay at 1, hospice, dies, then a claim dated after the death: dropped
5            MI and HF     stroke at 1, moved the next day to 2
             stay at 1
6            -             ED visit at 1, admitted to 2 the same day; a carrier claim of a primary care visit'''
import functools
import inspect
import re
import time

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructField, StructType

import cms.base as baseF
import cms.claims as claimsF
import cms.comorbidities as comorbiditiesF
import cms.line as lineF
import cms.mbsf as mbsfF
import cms.revenue as revenueF
import cms.stays as staysF
import cms.transfers as transfersF
import cms.utilities as cmsUtilitiesF
import utilities as utilitiesF
from cms.transfers import TransferType
from tests.chain import get_chain, get_cms_data, get_ed_to_ip, get_ip_stays, get_ip_to_ip, get_op_stays
from tests.cms import synthetic
from tests.cms.test_contracts import get_dictionary_entries, is_documented
from tests.cms.test_invariants import comparable, differing_columns, otherStays, shuffle_partitions, unsortedSets
from tests.data import benchmarks, snapshots
from tests.data.test_pipeline import datePart, enrichers, rawColumns
from tests.data.thresholds import namingExceptions

(yearI, yearF) = (synthetic.yearI, synthetic.yearF)
(sending, receiving) = (synthetic.sending["npi"], synthetic.receiving["npi"])
lastObservableDay = cmsUtilitiesF.get_lastObservableDay(yearF)

@pytest.fixture(scope="session")
def paths(spark, tmp_path_factory):
    root = str(tmp_path_factory.mktemp("synthetic"))
    paths = {"cms": f"{root}/CMS", "data": f"{root}/DATA", "aha": f"{root}/AHA", "output": f"{root}/output"}
    synthetic.write_external_files(spark, paths["data"], paths["aha"])
    synthetic.write_cms_files(spark, paths["cms"])
    return paths

@pytest.fixture(scope="session")
def externalData(spark, paths):
    return utilitiesF.get_data(yearI, yearF, spark, pathToData=paths["data"], pathToAHAData=paths["aha"])

@pytest.fixture(scope="session")
def cmsData(spark, paths, externalData):
    return get_cms_data(spark, paths["cms"], (yearI, yearF), externalData, paths["output"])

@pytest.fixture(scope="session")
def ipStays(paths, cmsData):
    return get_ip_stays(cmsData, paths["output"])

@pytest.fixture(scope="session")
def opStays(paths, cmsData):
    return get_op_stays(cmsData, paths["output"])

@pytest.fixture(scope="session")
def ipToIp(paths, cmsData, ipStays):
    return get_ip_to_ip(cmsData, ipStays, lastObservableDay, paths["output"])

@pytest.fixture(scope="session")
def edToIp(paths, opStays, ipStays):
    return get_ed_to_ip(opStays, ipStays, paths["output"])

@pytest.fixture(scope="session")
def preliminary(spark, paths, externalData):
    '''What add_preliminary_info returns, before filter_FFS, as the cmsPreliminary fixture of the data tests.'''
    dataframes = cmsUtilitiesF.read_data(spark, cmsUtilitiesF.get_filenames(paths["cms"], yearI, yearF), yearI, yearF)
    dataframes["mbsf"] = mbsfF.prep_mbsf(dataframes["mbsf"])
    return cmsUtilitiesF.add_preliminary_info(dataframes, externalData, lastObservableDay,
                                              firstObservableDay=cmsUtilitiesF.get_firstObservableDay(yearI))

@pytest.fixture(scope="session")
def shuffledRun(spark, paths, externalData):
    '''The whole chain again on the same claims, written in another order and split into part files, with 7 shuffle
    partitions: every output, keyed as the fixtures above are.'''
    (pathCMS, output) = (paths["cms"] + "Shuffled", paths["output"] + "Shuffled")
    synthetic.write_cms_files(spark, pathCMS, seed=3)
    #each step is written out inside the block, so all of it runs with these settings
    with shuffle_partitions(spark, 7):
        return get_chain(spark, pathCMS, (yearI, yearF), externalData, lastObservableDay, output)

def get_claims(df):
    return {r["CLAIMNO"]: r for r in df.collect()}

def get_transfer(transfers, beneficiary):
    rows = [r for r in transfers.collect() if r["fromDSYSRTKY"] == beneficiary]
    assert len(rows) == 1
    return rows[0]

#what get_cms_data keeps

@pytest.mark.parametrize("part,keys", [
    ("ipBase", {11, 12, 21, 41, 51, 52, 53, 61}),   #31 Medicare Advantage, 42 after the death
    ("opBase", {62}), ("snfBase", {22}), ("hospBase", {43}), ("carBase", {63}), ("carLine", {63})])
def test_get_cms_data_keeps_these_claims(cmsData, part, keys):
    assert sorted(r["CLAIMNO"] for r in cmsData[part].collect()) == sorted(keys)

def test_long_column_names_are_read(cmsData):
    claim = cmsData["hhaBase"].first()
    assert (claim["DSYSRTKY"], claim["CLAIMNO"], claim["THRU_DT"]) == (2, 23, 20190530)

def test_revenue_of_a_medicare_advantage_claim_is_dropped(cmsData):
    assert 31 not in {r["CLAIMNO"] for r in cmsData["ipRevenue"].collect()}

def test_mbsf_keeps_the_ffs_beneficiaries(cmsData):
    assert sorted((r["DSYSRTKY"], r["RFRNC_YR"]) for r in cmsData["mbsf"].collect()) == [
        (beneficiary, year) for beneficiary in (1, 2, 4, 5, 6) for year in (2018, 2019)]

def test_mbsf_joins(cmsData):
    #beneficiary 2's state and county are stored as doubles, so these also show pad_zeros worked
    row = next(r for r in cmsData["mbsf"].collect() if (r["DSYSRTKY"], r["RFRNC_YR"]) == (2, 2019))
    assert (row["ssaCounty"], row["fipsCounty"], row["fipsState"], row["region"]) == ("36250", "39049", "39", 2)
    assert (row["rucc"], row["ruccGroup"], row["maPenetration"]) == (1, 0, 35)
    assert (row["medianHhIncome"], row["medianDistanceEd"]) == (60000, 3.5)
    assert (row["medianHouseholdIncome"], row["totalPopulation"]) == (61000, 1300000)
    #January of the first year loaded
    assert row["ffsFirstMonth"] == utilitiesF.monthsInYearsPriorDict[2018] + 1
    assert (row["continuousFfs"], row["continuousRfrncYr"]) == (1, 1)

@pytest.mark.parametrize("claim,expected", [
    (11, {"providerName": "Sending Hospital", "providerZip": "43201", "ahaBedsIcu": 19, "ahaBedsIcuPrior": 18,
          "ahaTotalHospitalBeds": 200, "providerHcrisBedsTotal": 200, "providerCmi": 1.5, "providerSysId": "SYS1", "providerIsVI": 1,
          "providerStrokeCertification": None}),
    (12, {"providerName": "Receiving Hospital", "providerZip": "43202", "ahaBedsIcu": 119, "ahaBedsIcuPrior": 118,
          "ahaTotalHospitalBeds": 500, "providerHcrisBedsTotal": 500, "providerCmi": 2.5, "providerSysId": None, "providerIsVI": 0,
          "providerStrokeCertification": "Comprehensive Stroke Center"}),
    #the first year's prior comes from the survey of the year before it
    (51, {"providerName": "Sending Hospital", "ahaBedsIcu": 18, "ahaBedsIcuPrior": 17})],
    ids=["sending", "receiving", "firstYear"])
def test_provider_joins_differ_by_hospital(cmsData, claim, expected):
    row = get_claims(cmsData["ipBase"])[claim]
    assert {k: row[k] for k in expected} == expected

def test_provider_joins_of_the_place(cmsData):
    row = get_claims(cmsData["ipBase"])[11]
    assert (row["providerFIPS"], row["providerStateFIPS"], row["providerCountyName"], row["providerRegion"]) == ("39049", "39", "FRANKLIN", 2)
    assert (row["providerRucc"], row["providerRuccGroup"], row["providerMaPenetration"]) == (1, 0, 35)
    assert (row["providerHrr"], row["providerHsa"], row["providerState"]) == (330, 36001, "OH")
    assert (row["gachPrimary"], row["shortTermInpatientOrganization"], row["posIsRural"]) == (1, 1, 0)

def test_op_claims_get_the_provider_and_beneficiary_joins(cmsData):
    row = cmsData["opBase"].first()
    assert (row["providerName"], row["ahaBedsIcu"], row["mbsfAge"], row["rucc"]) == ("Sending Hospital", 19, 75, 1)

def test_beneficiary_joins(cmsData):
    row = get_claims(cmsData["ipBase"])[11]
    assert (row["mbsfAge"], row["fipsCounty"], row["rucc"], row["region"], row["mbsfMaPenetration"]) == (75, "39049", 1, 2, 35)

@pytest.mark.parametrize("claim,dead30,dead365", [
    (11, 1, 1),        #dies 20 days after admission
    (41, 0, 1),        #dies 59 days after admission
    (61, 0, None)],    #the year after the admission ends past the data, alive so far
    ids=["diedInWindow", "diedLater", "censored"])
def test_mortality_flags(cmsData, claim, dead30, dead365):
    row = get_claims(cmsData["ipBase"])[claim]
    assert (row["30DaysAfterAdmissionDateDead"], row["365DaysAfterAdmissionDateDead"]) == (dead30, dead365)

#get_stays

def test_stays_have_the_treatments(ipStays):
    rows = get_claims(ipStays)
    assert sorted(rows) == [11, 12, 21, 41, 51, 52, 53, 61]
    assert (rows[11]["ed"], rows[11]["ct"], rows[11]["tpa"], rows[11]["evt"], rows[11]["nihss"]) == (1, 1, 1, 0, 12)
    assert (rows[12]["mri"], rows[12]["icu"], rows[12]["tpa"], rows[12]["evt"], rows[12]["nihss"]) == (1, 1, 0, 1, None)

@pytest.mark.parametrize("claim,source,destination", [
    (11, "home", "ipOther"),       #moved to the receiving hospital the same day
    (12, "ipOther", "home"),
    (21, "home", "snf"),
    (41, "home", "home"),
    (52, "home", "ipOther"),       #the receiving admission is the day after
    (53, "ipOther", "home")])
def test_stays_source_and_destination(ipStays, claim, source, destination):
    row = get_claims(ipStays)[claim]
    assert (row["admissionSource"], row["thruDestination"]) == (source, destination)

#get_transfers

def test_ip_transfers_are_the_two_moves(ipToIp):
    assert sorted((r["fromDSYSRTKY"], r["fromCLAIMNO"], r["toCLAIMNO"], r["fromORGNPINM"], r["toORGNPINM"]) for r in ipToIp.collect()) == [
        (1, 11, 12, sending, receiving), (5, 52, 53, sending, receiving)]

def test_ip_transfer_node_and_dyad_columns(ipToIp):
    row = get_transfer(ipToIp, 1)
    assert (row["transferType"], row["firstTransfer"]) == (int(TransferType.inpatientToInpatient), 1)
    assert (row["nodeOutVol"], row["nodeInVol"], row["dyadTransferVol"]) == (2, 2, 2)
    assert (row["dyadVi"], row["dyadAcrossCounties"], row["dyadAcrossStates"]) == (0, 0, 0)

@pytest.mark.parametrize("beneficiary,expected", [
    (1, {"transfertpa": 1, "transferct": 1, "transfermri": 1, "toevt": 1, "transferEvtOrTpa": 1, "transferEvtAndTpa": 1,
         "transfernihss": 12, "transfernihssGroup": 1}),
    (5, {"transfertpa": 0, "transferct": 0, "transfermri": 0, "toevt": 0, "transferEvtOrTpa": 0, "transferEvtAndTpa": 0,
         "transfernihss": None, "transfernihssGroup": None})])
def test_ip_transfer_stroke_flags(ipToIp, beneficiary, expected):
    row = get_transfer(ipToIp, beneficiary)
    assert {k: row[k] for k in expected} == expected

def test_ip_transfer_stroke_aggregates(ipToIp):
    #one of the hospital pair's two transfers got tPA and EVT
    row = get_transfer(ipToIp, 5)
    assert (row["nodeFromTpaVol"], row["nodeFromTpaMean"], row["nodeToEvtVol"], row["nodeToEvtMean"]) == (1, 0.5, 1, 0.5)
    assert (row["dyadTpaVol"], row["dyadEvtVol"], row["dyadIncludesTpa"], row["dyadIncludesEvt"]) == (1, 1, 1, 1)

def test_comorbidities_reach_back_to_the_year_before(ipToIp):
    row = get_transfer(ipToIp, 5)
    assert (row["myocardialInfraction"], row["congestiveHeartFailure"], row["comorbidityIndex"]) == (1, 1, 2)
    assert row["hospitalizationsIn12Months"] == 1

def test_comorbidities_leave_out_the_stay_itself(ipToIp):
    row = get_transfer(ipToIp, 1)
    assert (row["cerebrovascular"], row["comorbidityIndex"], row["hospitalizationsIn12Months"]) == (0, 0, 0)

def test_ed_transfer(edToIp):
    row = get_transfer(edToIp, 6)
    assert (row["fromCLAIMNO"], row["toCLAIMNO"], row["fromed"]) == (62, 61, 1)
    assert (row["transferType"], row["fromORGNPINM"], row["toORGNPINM"]) == (int(TransferType.edToInpatient), sending, receiving)
    assert edToIp.count() == 1

#the optimizer's work depends on the code, not the data, so a small cohort shows what stalled the OSC job for 23 hours: on
#localCheckpoints of get_cms_data, whose plan keeps its constraints, this runs out of the 1 GB heap here too; it takes 2 s on
#the parquet read back
optimizerSeconds = 30

def test_optimizing_get_transfers_stays_quick(cmsData):
    stays = staysF.get_stays(cmsData["ipBase"], revenueF.get_revenue_info(cmsData["ipRevenue"]), claimType="ip")
    transfers = transfersF.get_transfers(fromStaysDf=stays, toStaysDf=stays, transferType=TransferType.inpatientToInpatient)
    start = time.perf_counter()
    transfers._jdf.queryExecution().optimizedPlan()
    assert time.perf_counter() - start < optimizerSeconds

#carrier

def test_line_flags_on_the_loaded_lines(cmsData):
    line = cmsData["carLine"].first()
    assert (line["level1HCPCS_CD"], line["allowed"]) == (99213, 1)
    flagged = lineF.get_line_summary(lineF.add_pcpOpVisit(cmsData["carLine"], inClaim=True))
    assert [(r["CLAIMNO"], r["pcpOpVisit"]) for r in flagged.collect()] == [(63, 1)]

#the rules the data tests check, here on the columns the code really produces

@pytest.mark.parametrize("name", list(enrichers))
def test_enricher_keeps_every_row_and_raw_value(cmsData, name):
    #the same raw rows before and after, not only as many, so a row lost and another doubled, or a raw value overwritten, show
    (before, after) = enrichers[name](cmsData, lastObservableDay)
    raw = [c for c in before.columns if c in rawColumns]
    assert raw and not set(raw) - set(after.columns)
    assert after.count() == before.count()
    assert before.select(raw).exceptAll(after.select(raw)).count() == 0
    assert after.select(raw).exceptAll(before.select(raw)).count() == 0

def test_added_columns_start_lower_case(cmsData, ipStays):
    columns = {c for df in [*cmsData.values(), ipStays] for c in df.columns}
    bad = sorted(c for c in columns if not (c[0].islower() or c in rawColumns or datePart.match(c) or c in namingExceptions))
    assert not bad, f"added columns not starting lower case: {bad}"

#invariants, see tests/cms/test_invariants.py

outputs = ["mbsf", "ipBase", "opBase", "snfBase", "hhaBase", "hospBase", "carBase", "carLine", "ipRevenue", "opRevenue",
           "ipStays", "opStays", "ipToIp", "edToIp"]

@pytest.mark.parametrize("name", outputs)
def test_same_output_in_any_row_order(cmsData, ipStays, opStays, ipToIp, edToIp, shuffledRun, name):
    first = {**cmsData, "ipStays": ipStays, "opStays": opStays, "ipToIp": ipToIp, "edToIp": edToIp}[name]
    (columns, rows) = comparable(first, ignore=unsortedSets + otherStays)
    (shuffledColumns, shuffledRows) = comparable(shuffledRun[name], ignore=unsortedSets + otherStays)
    assert shuffledColumns == columns
    assert len(rows) > 0
    assert shuffledRows == rows, f"the shuffled run differs in {differing_columns(columns, shuffledRows, rows)}"

@pytest.mark.parametrize("name", outputs)
def test_same_snapshot_in_any_row_order(cmsData, ipStays, opStays, ipToIp, edToIp, shuffledRun, name):
    #the profile of tests/data/test_regression.py on every column type the chain makes: a rerun that only reorders rows
    #must not show as a change
    first = {**cmsData, "ipStays": ipStays, "opStays": opStays, "ipToIp": ipToIp, "edToIp": edToIp}[name]
    profile = lambda df: {row[1:4]: row[4] for row in snapshots.get_profile(name, df)}
    (before, after) = (profile(first), profile(shuffledRun[name]))
    assert len(before) > 10
    assert snapshots.diff_profiles(before, after) == []

@pytest.mark.parametrize("part", [k for k in outputs if k[0].islower() and k not in ("ipStays", "opStays", "ipToIp", "edToIp")])
def test_get_cms_data_keeps_the_raw_values(spark, paths, cmsData, part):
    #every row get_cms_data keeps holds the raw values read from its file, with the dates fixed and the codes padded by
    #read_data, the step before; a raw name the file does not have, written by the code (ADMSN_DT of hha, DEATH_DT), is not
    #compared
    read = cmsUtilitiesF.read_data(spark, cmsUtilitiesF.get_filenames(paths["cms"], yearI, yearF), yearI, yearF)[part]
    raw = [c for c in cmsData[part].columns if c in read.columns]
    if cmsData[part].select(raw).exceptAll(read.select(raw)).count():
        changed = [c for c in raw if cmsData[part].select(c).exceptAll(read.select(c)).count()]
        pytest.fail(f"get_cms_data changed {changed or 'how the raw values pair up in the rows'}")

#add_* functions whose column is named by their arguments (eg add_XDaysFromYDAY(X=90) adds 90DaysFromADMSN_DT_DAY), only
#checked for dropping none
parametrized = {"add_XDaysAfterYDateDead", "add_XDaysFromYDAY", "add_column_prior", "add_losDaysOverXUntilY", "add_losOverXUntilY"}
#add_x functions that add more than x: the columns they may also add, and why
addExceptions = {
    #scratch columns of the mbsf enrollment flags, which mbsf.drop_unused_columns drops at the end of get_cms_data
    "add_allPartAB": {"partABArray", "partABArraySliced", "partABArraySlicedFiltered", "partABFirstMonthOfYear",
                      "partABLastMonthOfYear"},
    "add_anyEsrd": {"anyEsrdInMdcrStus", "mdcrStusArray"},
    "add_continuousFfs": {"ffsSinceJanuary", "ffsSinceJanuaryDifference"},
    "add_continuousRfrncYr": {"rfrncYrDifference"},
    "add_ffsFirstMonth": {"ffsFirstMonthOfYear"},
    "add_probablyDead": {"lastYearWithClaim"},
    #the parts a flag is made of, which stay on the claims, stays and transfers, with no data dictionary entry
    "add_evt": {"evtDrg", "evtPrcdr", "ccvPrcdr"},
    "add_evtDrg": {"ccvPrcdr"},
    "add_ishStroke": {"ishStrokeDgns", "ishStrokeDrg"},
    "add_tpa": {"tpaDgns", "tpaDrg", "tpaPrcdr"},
    "add_rehabilitation": {"rehabilitationFromCCN"}}

def record_add_calls(monkeypatch):
    '''Wraps every add_* function of the library's modules so each call on a DataFrame records the function's name, the
    columns in and out, and its inClaim argument.'''
    calls = []
    for module in (baseF, claimsF, comorbiditiesF, lineF, mbsfF, revenueF, staysF, transfersF, cmsUtilitiesF, utilitiesF):
        for (name, fn) in list(vars(module).items()):
            if name.startswith("add_") and inspect.isfunction(fn):
                monkeypatch.setattr(module, name, recording(name, fn, calls))
    return calls

def recording(name, fn, calls):
    signature = inspect.signature(fn)
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        out = fn(*args, **kwargs)
        if args and isinstance(args[0], DataFrame) and isinstance(out, DataFrame):
            arguments = signature.bind(*args, **kwargs)
            arguments.apply_defaults()
            calls.append((name, tuple(args[0].columns), tuple(out.columns), arguments.arguments.get("inClaim")))
        return out
    return wrapper

def test_add_functions_add_their_column_and_drop_none(spark, paths, externalData, cmsData, ipStays, opStays, ipToIp, monkeypatch):
    #which enricher runs first can only matter if one leaves a scratch column, drops a column or writes over one, so every
    #add_* the chain calls is checked for that instead of pairs of them in both orders. Plans only, nothing runs, each step
    #on what the fixtures wrote of the step before, as a plan built on plans that were never written runs out of memory
    calls = record_add_calls(monkeypatch)
    get_cms_data(spark, paths["cms"], (yearI, yearF), externalData, output=None)
    (get_ip_stays(cmsData, None), get_op_stays(cmsData, None), get_ed_to_ip(opStays, ipStays, None))
    transfersF.add_stroke_info(transfersF.get_transfers(fromStaysDf=ipStays, toStaysDf=ipStays,
                                                        transferType=TransferType.inpatientToInpatient))
    transfers = spark.read.parquet(f"{paths['output']}/ipToIp")
    lookups = {k: spark.read.parquet(f"{paths['output']}/lookups/{k}") for k in transfersF.get_lookups(transfers, cmsData)}
    transfersF.add_comorbidity_and_days_at_home_info(transfers, lookups, lastObservableDay)
    for name in enrichers:
        enrichers[name](cmsData, lastObservableDay)
    assert len({c[0] for c in calls}) > 50
    wrong = {}
    for (name, before, after, inClaim) in calls:
        (added, dropped) = (set(after) - set(before), set(before) - set(after))
        #x may already be there: add_los_at_X_info redoes the losDays its input has, with the same formula
        x = None if name.endswith("_info") or name in parametrized else {name[4:]} | ({name[4:] + "InClaim"} if inClaim else set())
        if dropped or (x is not None and (added - x - addExceptions.get(name, set()) or x - set(after))):
            wrong.setdefault(name, set()).add((tuple(sorted(added)), tuple(sorted(dropped))))
    assert not wrong, "\n".join(f"{n}: added, dropped {sorted(v)}" for n, v in sorted(wrong.items()))

#every column an analysis keeps has a data dictionary entry: the added columns of each output, less the scratch the
#drop_unused_columns of stays drops (that of mbsf ran in get_cms_data), also when a transfer carries it as from/to

def get_dropped_by_stays(spark, names):
    names = sorted(names)
    empty = spark.createDataFrame([], StructType([StructField(n, StringType()) for n in names]))
    return set(names) - set(staysF.drop_unused_columns(empty).columns)

@pytest.mark.parametrize("name", outputs)
def test_added_columns_are_in_data_dictionary(spark, cmsData, ipStays, opStays, ipToIp, edToIp, name):
    df = {**cmsData, "ipStays": ipStays, "opStays": opStays, "ipToIp": ipToIp, "edToIp": edToIp}[name]
    unprefixed = {c: re.sub("^(from|to)", "", c) for c in df.columns}
    added = [c for c in df.columns if unprefixed[c][:1].islower()]
    dropped = get_dropped_by_stays(spark, {unprefixed[c] for c in added} | set(added))
    entries = get_dictionary_entries()
    missing = sorted(c for c in added if c not in dropped and unprefixed[c] not in dropped and not is_documented(c, entries))
    assert not missing, f"no entry in get_data_dictionary (r_utilities.R): {missing}"

#the measures of tests/data/test_benchmarks.py, on the columns the chain really has

def test_benchmark_measures_run_on_the_chain(preliminary, externalData):
    byState = benchmarks.get_mbsf_by_state(preliminary["mbsf"])
    assert set(byState) == {(yearI, "39"), (yearF, "39")}
    #every invented beneficiary but the MA one has Part A and B all year, so the two definitions agree here
    assert all(v["cmsOm"] > 0 and v["cmsOmAlsoFfs"] == 1.0 and v["ffsNotCmsOm"] == 0.0 for v in byState.values())
    stays = benchmarks.get_ip_stays(preliminary["ipBase"])
    visits = benchmarks.get_er_visits([preliminary["ipRevenue"], preliminary["opRevenue"]])
    rates = benchmarks.get_rates_by_state(preliminary["mbsf"], stays, visits)
    assert rates[(yearF, "39")]["ipStaysPer1000"] > 0 and rates[(yearF, "39")]["erVisitsPer1000"] > 0
    ma = benchmarks.get_ma_share_by_county(preliminary["mbsf"], minBeneficiaries=1)
    assert {(r["RFRNC_YR"], r["fipsCounty"], r["cmsMaShare"]) for r in ma} == {(yearI, "39049", 0.35), (yearF, "39049", 0.35)}
    coverage = benchmarks.get_hospital_coverage(preliminary["ipBase"], externalData["pos"], [yearI, yearF])
    #both acute hospitals have claims in 2019, the CAH closed in 2017
    assert coverage[(yearF, "acute")] == (1.0, 1.0) and (yearF, "cah") not in coverage
