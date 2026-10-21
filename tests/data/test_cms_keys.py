'''Uniqueness of the keys the code joins and windows on, and integrity between the CMS files.'''
import pytest

from tests.data.checks import get_duplicate_keys, get_match_rate, check_rates

claimKeys = ["DSYSRTKY", "CLAIMNO", "THRU_DT"]

#each key and the code that relies on it being unique
baseKeys = {"claim": (claimKeys, "claims.get_claims and revenue/line filter_claims"),
            "claimnoYear": (["CLAIMNO", "THRU_DT_YEAR"], "get_cms_data's old runTests check"),
            "claimnoDay": (["CLAIMNO", "THRU_DT_DAY"], "the transfers.get_clean_transfers windows, no DSYSRTKY"),
            "beneficiaryClaimno": (["DSYSRTKY", "CLAIMNO"], "base.add_los_at_X_info join back, no THRU_DT")}

linesOfBase = {"ipRevenue": "ipBase", "opRevenue": "opBase", "snfRevenue": "snfBase", "hhaRevenue": "hhaBase",
               "hospRevenue": "hospBase", "carLine": "carBase"}

#the mbsf fields a duplicate (DSYSRTKY, RFRNC_YR) row could disagree on and change what prep_mbsf keeps
mbsfFieldsThatMatter = (["DEATH_DT", "V_DOD_SW", "STATE_CD", "CNTY_CD", "HMO_MO", "BUYIN_MO", "AGE", "SEX", "RACE"] +
                        [f"BUYIN{m}" for m in range(1, 13)] + [f"HMOIND{m}" for m in range(1, 13)])

@pytest.mark.parametrize("keyName", list(baseKeys))
def test_base_key_unique(cmsRaw, baseClaimTypePart, keyName):
    (keys, usedBy) = baseKeys[keyName]
    (number, examples) = get_duplicate_keys(cmsRaw[baseClaimTypePart], keys)
    assert number == 0, f"{baseClaimTypePart}: {number} {keys} on more than one claim (used by {usedBy}), eg {examples}"

def test_mbsf_duplicates_agree(cmsRaw, metricsTable):
    #prep_mbsf keeps an arbitrary row of a duplicated (DSYSRTKY, RFRNC_YR), which is harmless only if the rows agree
    mbsf = cmsRaw["mbsf"]
    keys = ["DSYSRTKY", "RFRNC_YR"]
    (number, _) = get_duplicate_keys(mbsf, keys)
    metricsTable.add("mbsf duplicates", "DSYSRTKY RFRNC_YR", None, number)
    fields = [c for c in mbsfFieldsThatMatter if c in mbsf.columns]
    (disagreeing, examples) = get_duplicate_keys(mbsf.select(*keys, *fields).distinct(), keys)
    assert disagreeing == 0, f"mbsf: {disagreeing} duplicated (DSYSRTKY, RFRNC_YR) whose rows differ in {fields}, eg {examples}"

@pytest.mark.parametrize("linePart", list(linesOfBase))
def test_lines_have_a_base_claim(cmsRaw, metricsTable, linePart):
    basePart = linesOfBase[linePart]
    rates = get_match_rate(cmsRaw[linePart], claimKeys, cmsRaw[basePart], claimKeys, "THRU_DT_YEAR")
    check_rates(metricsTable, "line in base", linePart, rates)

@pytest.mark.parametrize("linePart", list(linesOfBase))
def test_base_claims_have_lines(cmsRaw, metricsTable, linePart):
    #claims.get_claims gives a claim without lines null flags rather than 0
    basePart = linesOfBase[linePart]
    rates = get_match_rate(cmsRaw[basePart], claimKeys, cmsRaw[linePart], claimKeys, "THRU_DT_YEAR")
    check_rates(metricsTable, "base with lines", basePart, rates)

def test_base_claims_have_an_mbsf_row(cmsRaw, metricsTable, baseClaimTypePart):
    #base.add_mbsf_info and filter_beneficiaries join on (DSYSRTKY, THRU_DT_YEAR = RFRNC_YR)
    rates = get_match_rate(cmsRaw[baseClaimTypePart], ["DSYSRTKY", "THRU_DT_YEAR"], cmsRaw["mbsf"],
                           ["DSYSRTKY", "RFRNC_YR"], "THRU_DT_YEAR")
    check_rates(metricsTable, "base in mbsf", baseClaimTypePart, rates)
