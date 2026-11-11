'''How many claims each external source matches, per claim year: a drop flags a key format or coverage change.
Every rate goes into the metrics table; its floor is in thresholds.rateFloors.'''
import pytest
import pyspark.sql.functions as F

from tests.data.checks import get_match_rate, get_filled_rate, check_rates

#(claimTypePart, claim keys, source, source keys, by): the joins of add_provider_info/add_beneficiary_info whose key is
#a raw CMS field
keyMatches = [(part, ["ORGNPINM"], "npi", ["NPI"], "THRU_DT_YEAR") for part in ["ipBase", "opBase"]] + \
             [(part, ["PROVIDER"], "pos", ["PRVDR_NUM"], "THRU_DT_YEAR") for part in ["ipBase", "opBase"]] + \
             [(part, ["PROVIDER", "THRU_DT_YEAR"], source, refKeys, "THRU_DT_YEAR")
              for part in ["ipBase", "opBase"]
              for source, refKeys in [("aha", ["MCRNUM", "year"]), ("hcris", ["PRVDR_NUM", "hcrisYear"]),
                                      ("cmi", ["provider", "year"]), ("chspHosp", ["ccn", "year"])]] + \
             [(part, ["ssaCounty"], "cbsa", ["ssacounty"], "THRU_DT_YEAR") for part in ["ipBase", "opBase"]] + \
             [("mbsf", ["ssaCounty"], "cbsa", ["ssacounty"], "RFRNC_YR"),
              ("carLine", ["PRF_NPI"], "npi", ["NPI"], "THRU_DT_YEAR")]

#(claimTypePart, key column, column the join brings in, by): the joins whose key the pipeline derives first
filledMatches = [(part, key, value, "THRU_DT_YEAR")
                 for part in ["ipBase", "opBase"]
                 for key, value in [("providerZip", "providerHrr"), ("providerFIPS", "providerRucc"),
                                    ("providerFIPS", "providerMaPenetration"), ("ssaCounty", "fipsCounty"),
                                    ("fipsCounty", "rucc")]] + \
                [("mbsf", key, value, "RFRNC_YR")
                 for key, value in [("ssaCounty", "fipsCounty"), ("fipsCounty", "medianHhIncome"),
                                    ("fipsCounty", "totalPopulation"), ("fipsCounty", "maPenetration"),
                                    ("fipsCounty", "rucc")]]

@pytest.mark.parametrize("part, keys, source, refKeys, by", keyMatches,
                         ids=[f"{p}-{'-'.join(k)}-{s}" for p, k, s, _, _ in keyMatches])
def test_claim_key_in_source(cmsRaw, externalData, metricsTable, part, keys, source, refKeys, by):
    rates = get_match_rate(cmsRaw[part], keys, externalData[source], refKeys, by)
    check_rates(metricsTable, f"match {source}", f"{part}.{'+'.join(keys)}", rates)

@pytest.mark.parametrize("part, key, value, by", filledMatches, ids=[f"{p}-{k}-{v}" for p, k, v, _ in filledMatches])
def test_joined_column_filled(cmsPreliminary, metricsTable, part, key, value, by):
    (_, after) = cmsPreliminary
    rates = get_filled_rate(after[part], key, value, by)
    check_rates(metricsTable, f"filled {value}", f"{part}.{key}", rates)

#(claimTypePart, the county the claims join maPenetration on, by), see mbsf.add_maPenetration and
#base.add_providerMaPenetration
maPenetrationJoins = [("mbsf", "fipsCounty", "RFRNC_YR"), ("ipBase", "providerFIPS", "THRU_DT_YEAR"),
                      ("opBase", "providerFIPS", "THRU_DT_YEAR")]

@pytest.mark.parametrize("part, key, by", maPenetrationJoins, ids=[p for p, _, _ in maPenetrationJoins])
def test_ma_penetration_matches_without_padding(cmsPreliminary, externalData, metricsTable, part, key, by):
    #if maPenetration.FIPS lost the leading 0 of states 01-09 (test_external.py), the rows of those states match it only
    #once FIPS is 0-padded, and the other states the same either way; the share matched as stored and 0-padded, by year
    #and state group, goes into the metrics table
    (_, after) = cmsPreliminary
    claims = (after[part].select(key, by)
                         .filter(F.col(key).isNotNull())
                         .withColumn("states01to09", F.col(key).startswith("0")))
    ma = (externalData["maPenetration"].select(F.col("FIPS").cast("string").alias("FIPS"),
                                               F.col("Year").cast("int").alias(by))
                                       .dropna())
    asStored = ma.select(F.col("FIPS").alias(key), by).distinct().withColumn("asStored", F.lit(1))
    padded = ma.select(F.lpad(F.col("FIPS"), 5, "0").alias(key), by).distinct().withColumn("padded", F.lit(1))
    rows = (claims.join(asStored, on=[key, by], how="left_outer")
                  .join(padded, on=[key, by], how="left_outer")
                  .groupBy(by, "states01to09")
                  .agg(*[F.avg(F.coalesce(F.col(c), F.lit(0))).alias(c) for c in ["asStored", "padded"]])
                  .collect())
    for r in rows:
        states = "states 01-09" if r["states01to09"] else "other states"
        for how in ["asStored", "padded"]:
            metricsTable.add("maPenetration match", f"{part} {states} {how}", r[by], r[how])
    gains = {(r[by], "states 01-09" if r["states01to09"] else "other states"):
             (round(r["asStored"], 4), round(r["padded"], 4)) for r in rows if r["padded"] > r["asStored"]}
    assert not gains, f"{part}: more rows match maPenetration once its FIPS is 0-padded, as stored -> padded: {gains}"
