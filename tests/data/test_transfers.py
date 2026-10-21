'''The IP->IP transfers of the sampled claims with their comorbidities read from the sending stay (the default) and from
the receiving one (transfers.add_comorbidity_info lookbackFromReceiving=True): both keep the transfers, and the receiving
lookback leaves the prior hospitalizations and the coverage gate as they are but changes flags. Run alone with
-k transfers.'''
from functools import reduce

import pytest
import pyspark.sql.functions as F

import cms.comorbidities as comorbiditiesF
import cms.revenue as revenueF
import cms.stays as staysF
import cms.transfers as transfersF
from cms.transfers import TransferType
from utilities import icd10Day
from tests.data.checks import get_examples, check_rates

#one transfer per row, see transfers.get_clean_transfers
keys = ["fromDSYSRTKY", "fromCLAIMNO", "fromTHRU_DT_DAY", "toCLAIMNO", "toTHRU_DT_DAY"]
priorHospitalizations = ["hospitalizationsIn12Months", "hospitalizedIn12Months",
                         "hospitalizationsIn6Months", "hospitalizedIn6Months"]
#get_conditions folds infectionOrCancerDueToAids into aids
conditions = [c for c in comorbiditiesF.get_codes() if c != "infectionOrCancerDueToAids"] + ["aids"]
outputs = priorHospitalizations + conditions + ["comorbidityIndex"]

def receiving(c):
    return "receiving" + c[0].upper() + c[1:]

def any_of(conditionColumns):
    return reduce(lambda a, b: a | b, conditionColumns)

@pytest.fixture(scope="module")
def ipToIp(sampledCmsData):
    '''{transfers, default, receiving, both}: the IP->IP transfers of the sample, add_comorbidity_info on them by
    default and with lookbackFromReceiving=True, and the two joined on the transfer, the receiving outputs renamed by
    receiving().'''
    d = sampledCmsData
    stays = staysF.get_stays(d["ipBase"], revenueF.get_revenue_info(d["ipRevenue"], inClaim=True), claimType="ip")
    transfers = transfersF.get_transfers(fromStaysDf=stays, toStaysDf=stays, transferType=TransferType.inpatientToInpatient)
    #only what add_comorbidity_info reads, cut from the get_transfers lineage that both runs would otherwise rerun
    read = ["DSYSRTKY", "CLAIMNO", "ADMSN_DT_DAY", "ADMSN_DT_MONTH", "THRU_DT_DAY", "ffsFirstMonth"]
    transfers = (transfers.select("transferType", "fromTHRU_DT_YEAR", *[side + c for side in ("from", "to") for c in read])
                          .localCheckpoint())
    lookups = {k: df.localCheckpoint() for k, df in transfersF.get_lookups(transfers, d).items() if k in ("ipBase", "opBase")}
    default = transfersF.add_comorbidity_info(transfers, lookups["ipBase"], lookups["opBase"]).localCheckpoint()
    receivingRun = transfersF.add_comorbidity_info(transfers, lookups["ipBase"], lookups["opBase"],
                                                   lookbackFromReceiving=True).localCheckpoint()
    both = (default.select(*keys, "fromTHRU_DT_YEAR", *outputs)
                   .join(receivingRun.select(*keys, *[F.col(c).alias(receiving(c)) for c in outputs]), on=keys, how="inner")
                   .localCheckpoint())
    return {"transfers": transfers, "default": default, "receiving": receivingRun, "both": both}

def test_comorbidity_keeps_rows(ipToIp):
    n = ipToIp["transfers"].count()
    assert n > 0, "no IP->IP transfers in the sample"
    counts = {k: ipToIp[k].count() for k in ("default", "receiving", "both")}
    assert counts == dict.fromkeys(counts, n), f"{n} transfers in, {counts} out"

def test_receiving_lookback_keeps_prior_hospitalizations(ipToIp):
    differ = any_of([~F.col(c).eqNullSafe(F.col(receiving(c))) for c in priorHospitalizations])
    n = ipToIp["both"].filter(differ).count()
    cols = keys + ["hospitalizationsIn12Months", receiving("hospitalizationsIn12Months")]
    assert n == 0, f"{n} transfers with other prior hospitalizations, eg {get_examples(ipToIp['both'], differ, cols)}"

def test_receiving_lookback_nulls_the_same_rows(ipToIp):
    #both windows past the ICD10 start, so only the FFS coverage gate, the same in both, nulls the flags
    both = ipToIp["both"].filter(F.col("fromTHRU_DT_DAY") - F.lit(icd10Day) >= 360)
    differ = any_of([F.col(c).isNull() != F.col(receiving(c)).isNull() for c in conditions + ["comorbidityIndex"]])
    n = both.filter(differ).count()
    cols = keys + ["comorbidityIndex", receiving("comorbidityIndex")]
    assert n == 0, f"{n} transfers null in one lookback only, eg {get_examples(both, differ, cols)}"

def test_receiving_lookback_changes_flags(ipToIp, metricsTable):
    both = ipToIp["both"]
    #per year, the share of the transfers with both flags non-null whose flags agree; a low cerebrovascular one
    #would mean the sending stay's principal dgns (eg the stroke) got into the receiving lookback
    agree = {c: F.avg(F.when(F.col(c).isNotNull() & F.col(receiving(c)).isNotNull(),
                             (F.col(c) == F.col(receiving(c))).cast("int"))).alias(c) for c in conditions}
    rows = both.groupBy("fromTHRU_DT_YEAR").agg(*agree.values()).collect()
    for c in conditions:
        check_rates(metricsTable, "receiving agreement", c,
                    {r["fromTHRU_DT_YEAR"]: r[c] for r in rows if r[c] is not None})
    changed = both.filter(~F.col("comorbidityIndex").eqNullSafe(F.col(receiving("comorbidityIndex")))).count()
    assert changed > 0, "lookbackFromReceiving=True changed no comorbidityIndex"
