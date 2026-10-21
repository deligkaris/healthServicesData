'''How many claims each external source matches, per claim year: a drop flags a key format or coverage change.
Every rate goes into the metrics table; its floor is in thresholds.rateFloors.'''
import pytest

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
