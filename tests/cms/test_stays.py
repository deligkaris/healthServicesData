import pytest
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType


# ============================================================
# Helper to build a DataFrame using the real cms.schemas ipBase schema,
# padding unspecified fields with NULL. (Mirrors the helper used in
# test_base.py — duplicated here to keep test files self-contained.)
# ============================================================

def make_real_claim_df(spark, claim_type, rows):
    from cms.schemas import schemas
    schema = schemas[claim_type]
    field_names = [f.name for f in schema.fields]
    padded = [{name: r.get(name) for name in field_names} for r in rows]
    return spark.createDataFrame(padded, schema=schema)


# ============================================================
# End-to-end pipeline helper for septic-shock annual volume:
#   real ipBase claims (THRU_DT + ICD_DGNS_CD1..25)
#   -> add_through_date_info                         (derives THRU_DT_YEAR)
#   -> add_dgnsCodeAll                               (collapses 25 dgns cols -> array)
#   -> add_septicShock                               (R6521-derived septicShock flag)
#   -> add_providerAnnualVolume(col="septicShock")   (window sum per ORGNPINM x year)
# ============================================================

def _run_septic_shock_vol_pipeline(spark, rows):
    """rows: list of dicts. Each must specify CLAIMNO, ORGNPINM, THRU_DT, and
    optionally ICD_DGNS_CD1 (= 'R6521' for a septic-shock case; any other
    value or omission for a non-septic-shock claim)."""
    from cms.utilities import add_through_date_info
    from cms.base import add_dgnsCodeAll, add_septicShock
    from cms.stays import add_providerAnnualVolume
    df = make_real_claim_df(spark, "ipBase", rows)
    df = add_through_date_info(df)
    df = add_dgnsCodeAll(df)
    df = add_septicShock(df)
    df = add_providerAnnualVolume(df, col="septicShock")
    return df


def _row(claimno, orgnpinm, thru_dt, is_septic_shock):
    """Build a single ipBase row dict. R6521 is the ICD-10 code for septic shock
    that add_septicShockDgns matches on."""
    return {
        "DSYSRTKY": claimno,  # one beneficiary per claim is fine for these tests
        "CLAIMNO": claimno,
        "ORGNPINM": orgnpinm,
        "THRU_DT": thru_dt,
        "ICD_DGNS_CD1": "R6521" if is_septic_shock else "I10",
    }


# ============================================================
# End-to-end tests for septic-shock annual volume via add_providerAnnualVolume
# ============================================================

class TestAddProviderSepticShockAnnualVolume:

    def test_single_provider_single_year_all_septic_shock(self, spark):
        rows = [_row(i, 100, 20200115, True) for i in range(1, 5)]  # 4 claims, all R6521
        df = _run_septic_shock_vol_pipeline(spark, rows)
        result = df.collect()
        # Sanity: pipeline produced a septicShock=1 on every row
        assert all(r["septicShock"] == 1 for r in result)
        assert all(r["providerSepticShockAnnualVolume"] == 4 for r in result)

    def test_single_provider_single_year_mixed(self, spark):
        # 3 of 5 claims for provider 100 in 2020 have R6521.
        rows = [
            _row(1, 100, 20200115, True),
            _row(2, 100, 20200120, False),
            _row(3, 100, 20200201, True),
            _row(4, 100, 20200305, False),
            _row(5, 100, 20200410, True),
        ]
        df = _run_septic_shock_vol_pipeline(spark, rows)
        result = df.collect()
        assert all(r["providerSepticShockAnnualVolume"] == 3 for r in result)

    def test_all_non_septic_shock_returns_zero(self, spark):
        rows = [_row(i, 100, 20200115, False) for i in range(1, 5)]
        df = _run_septic_shock_vol_pipeline(spark, rows)
        result = df.collect()
        assert all(r["septicShock"] == 0 for r in result)
        assert all(r["providerSepticShockAnnualVolume"] == 0 for r in result)

    def test_two_providers_same_year_counted_independently(self, spark):
        # Provider 100 has 2 septic-shock cases, provider 200 has 1.
        rows = [
            _row(1, 100, 20200115, True),
            _row(2, 100, 20200120, True),
            _row(3, 100, 20200201, False),
            _row(4, 200, 20200115, True),
            _row(5, 200, 20200120, False),
            _row(6, 200, 20200201, False),
        ]
        df = _run_septic_shock_vol_pipeline(spark, rows)
        by_claim = {r["CLAIMNO"]: r["providerSepticShockAnnualVolume"] for r in df.collect()}
        for cn in (1, 2, 3):
            assert by_claim[cn] == 2
        for cn in (4, 5, 6):
            assert by_claim[cn] == 1

    def test_same_provider_different_years_counted_independently(self, spark):
        # 2020: 2 septic shock; 2021: 1 septic shock.
        rows = [
            _row(1, 100, 20200115, True),
            _row(2, 100, 20200201, True),
            _row(3, 100, 20200305, False),
            _row(4, 100, 20210110, True),
            _row(5, 100, 20210410, False),
        ]
        df = _run_septic_shock_vol_pipeline(spark, rows)
        by_claim = {r["CLAIMNO"]: r["providerSepticShockAnnualVolume"] for r in df.collect()}
        assert by_claim[1] == 2 and by_claim[2] == 2 and by_claim[3] == 2  # 2020
        assert by_claim[4] == 1 and by_claim[5] == 1                       # 2021

    def test_value_broadcast_to_every_row_in_partition(self, spark):
        # Window function: the volume is attached to every row in the partition,
        # not just the rows with septicShock=1.
        rows = [
            _row(1, 100, 20200115, False),
            _row(2, 100, 20200120, False),
            _row(3, 100, 20200201, True),
        ]
        df = _run_septic_shock_vol_pipeline(spark, rows)
        vols = [r["providerSepticShockAnnualVolume"] for r in df.collect()]
        assert vols == [1, 1, 1]

    def test_r6521_in_later_dgns_position_still_counted(self, spark):
        # add_septicShockDgns inspects all 25 ICD_DGNS_CD columns via add_dgnsCodeAll.
        # Putting R6521 in ICD_DGNS_CD7 should still derive septicShock=1.
        rows = [
            {"DSYSRTKY": 1, "CLAIMNO": 1, "ORGNPINM": 100, "THRU_DT": 20200115,
             "ICD_DGNS_CD1": "I10", "ICD_DGNS_CD7": "R6521"},
            {"DSYSRTKY": 2, "CLAIMNO": 2, "ORGNPINM": 100, "THRU_DT": 20200120,
             "ICD_DGNS_CD1": "I10"},
        ]
        df = _run_septic_shock_vol_pipeline(spark, rows)
        by_claim = {r["CLAIMNO"]: r for r in df.collect()}
        assert by_claim[1]["septicShock"] == 1
        assert by_claim[2]["septicShock"] == 0
        assert by_claim[1]["providerSepticShockAnnualVolume"] == 1
        assert by_claim[2]["providerSepticShockAnnualVolume"] == 1  # window broadcasts

    def test_full_grid_multiple_providers_and_years(self, spark):
        # 100/2020 -> 3 septic shock, 100/2021 -> 1, 200/2020 -> 0, 200/2021 -> 4.
        rows = (
            [_row(i, 100, 20200115, True) for i in range(1, 4)]                  # 1,2,3
            + [_row(4, 100, 20210110, True), _row(5, 100, 20210410, False)]      # 4,5
            + [_row(6, 200, 20200115, False), _row(7, 200, 20200201, False)]     # 6,7
            + [_row(i, 200, 20210110, True) for i in range(8, 12)]               # 8,9,10,11
        )
        df = _run_septic_shock_vol_pipeline(spark, rows)
        by_claim = {r["CLAIMNO"]: r["providerSepticShockAnnualVolume"] for r in df.collect()}
        for cn in (1, 2, 3):
            assert by_claim[cn] == 3
        for cn in (4, 5):
            assert by_claim[cn] == 1
        for cn in (6, 7):
            assert by_claim[cn] == 0
        for cn in (8, 9, 10, 11):
            assert by_claim[cn] == 4

    def test_aggregate_septic_shock_rate(self, spark):
        # End-to-end aggregate check: in a 10-claim cohort with 4 R6521 cases,
        # sum(septicShock) == 4 and the volume on those rows reflects the
        # per-(provider,year) totals computed from the same flag.
        rows = [
            _row(1, 100, 20200115, True),
            _row(2, 100, 20200120, True),
            _row(3, 100, 20200201, False),
            _row(4, 100, 20200305, True),
            _row(5, 100, 20200410, False),
            _row(6, 200, 20200115, True),
            _row(7, 200, 20200120, False),
            _row(8, 200, 20200201, False),
            _row(9, 200, 20200305, False),
            _row(10, 200, 20200410, False),
        ]
        df = _run_septic_shock_vol_pipeline(spark, rows)
        assert df.agg(F.sum("septicShock")).collect()[0][0] == 4
        # Provider 100: 3 septic shock; provider 200: 1 septic shock.
        by_claim = {r["CLAIMNO"]: r["providerSepticShockAnnualVolume"] for r in df.collect()}
        for cn in (1, 2, 3, 4, 5):
            assert by_claim[cn] == 3
        for cn in (6, 7, 8, 9, 10):
            assert by_claim[cn] == 1

    def test_column_added_and_inputs_preserved(self, spark):
        rows = [_row(1, 100, 20200115, True)]
        df = _run_septic_shock_vol_pipeline(spark, rows)
        # New column from the function under test.
        assert "providerSepticShockAnnualVolume" in df.columns
        # Upstream-derived columns survived.
        for col in ["THRU_DT_YEAR", "dgnsCodeAll", "septicShock"]:
            assert col in df.columns
        # Originals from the real ipBase schema preserved.
        for col in ["DSYSRTKY", "CLAIMNO", "ORGNPINM", "THRU_DT"]:
            assert col in df.columns


# ============================================================
# End-to-end pipeline helper for add_providerAnnualCapability:
#   real ipBase claims (THRU_DT + ICD_PRCDR_CD1..25)
#   -> add_through_date_info       (derives THRU_DT_YEAR)
#   -> add_prcdrCodeAll            (collapses 25 prcdr cols -> array)
#   -> add_<capability>            (derives the binary flag from procedure codes)
#   -> add_providerAnnualCapability(df, col=<flag>)
# ============================================================

# Procedure codes that trigger each binary capability flag in cms.base.
_CAPABILITY_PROCEDURES = {
    "imv":  "5A1935Z",  # invasive mechanical ventilation
    "rrt":  "5A1D90Z",  # renal replacement therapy (any 5A1D*)
    "ecmo": "5A1522F",  # extracorporeal membrane oxygenation
}

# Maps the capability flag name to the upstream function that derives it.
def _capability_deriver(flag):
    from cms.base import add_imv, add_rrt, add_ecmo
    return {"imv": add_imv, "rrt": add_rrt, "ecmo": add_ecmo}[flag]


def _run_capability_pipeline(spark, rows, flag, capability_fn=None):
    """rows: list of dicts. Each must specify CLAIMNO, ORGNPINM, THRU_DT, and
    optionally ICD_PRCDR_CD1 (= the capability's procedure code for a positive
    claim, anything else / omission for a negative claim).
    flag: one of 'imv', 'rrt', 'ecmo'.
    capability_fn: the cms.stays function that derives the provider capability
    column; defaults to add_providerAnnualCapability."""
    from cms.utilities import add_through_date_info
    from cms.base import add_prcdrCodeAll
    from cms.stays import add_providerAnnualCapability
    if capability_fn is None:
        capability_fn = add_providerAnnualCapability
    df = make_real_claim_df(spark, "ipBase", rows)
    df = add_through_date_info(df)
    df = add_prcdrCodeAll(df)
    df = _capability_deriver(flag)(df)
    df = capability_fn(df, col=flag)
    return df


def _cap_row(claimno, orgnpinm, thru_dt, flag, has_capability):
    """Build a single ipBase row. Sets ICD_PRCDR_CD1 to the capability's
    procedure code when has_capability is True; otherwise to an unrelated code."""
    return {
        "DSYSRTKY": claimno,
        "CLAIMNO": claimno,
        "ORGNPINM": orgnpinm,
        "THRU_DT": thru_dt,
        "ICD_PRCDR_CD1": _CAPABILITY_PROCEDURES[flag] if has_capability else "00H00MZ",
    }


# ============================================================
# End-to-end tests for add_providerAnnualCapability
# ============================================================

class TestAddProviderAnnualCapability:

    @pytest.mark.parametrize("flag", ["imv", "rrt", "ecmo"])
    def test_capability_column_name_follows_convention(self, spark, flag):
        # providerImvAnnualCapability / providerRrtAnnualCapability / providerEcmoAnnualCapability
        df = _run_capability_pipeline(spark, [_cap_row(1, 100, 20200115, flag, True)], flag)
        expected = "provider" + flag[0].upper() + flag[1:] + "AnnualCapability"
        assert expected in df.columns

    @pytest.mark.parametrize("flag", ["imv", "rrt", "ecmo"])
    def test_provider_with_at_least_one_positive_row_has_capability_1(self, spark, flag):
        # 3 claims for the same provider/year; only one carries the procedure code.
        rows = [
            _cap_row(1, 100, 20200115, flag, False),
            _cap_row(2, 100, 20200120, flag, True),   # the positive one
            _cap_row(3, 100, 20200201, flag, False),
        ]
        df = _run_capability_pipeline(spark, rows, flag)
        cap_col = "provider" + flag[0].upper() + flag[1:] + "AnnualCapability"
        # The window-max broadcasts capability=1 to every row in (100, 2020).
        for r in df.collect():
            assert r[cap_col] == 1

    @pytest.mark.parametrize("flag", ["imv", "rrt", "ecmo"])
    def test_provider_with_no_positive_rows_has_capability_0(self, spark, flag):
        rows = [_cap_row(i, 100, 20200115, flag, False) for i in range(1, 4)]
        df = _run_capability_pipeline(spark, rows, flag)
        cap_col = "provider" + flag[0].upper() + flag[1:] + "AnnualCapability"
        for r in df.collect():
            assert r[cap_col] == 0

    def test_capability_set_independently_per_year(self, spark):
        # Provider 100 has IMV in 2020 but not 2021.
        rows = [
            _cap_row(1, 100, 20200115, "imv", True),
            _cap_row(2, 100, 20200201, "imv", False),
            _cap_row(3, 100, 20210110, "imv", False),
            _cap_row(4, 100, 20210410, "imv", False),
        ]
        df = _run_capability_pipeline(spark, rows, "imv")
        by_claim = {r["CLAIMNO"]: r["providerImvAnnualCapability"] for r in df.collect()}
        # 2020 partition: capability=1 (at least one row had it)
        assert by_claim[1] == 1 and by_claim[2] == 1
        # 2021 partition: capability=0 (no rows had it)
        assert by_claim[3] == 0 and by_claim[4] == 0

    def test_capability_set_independently_per_provider(self, spark):
        # Provider 100 has IMV, provider 200 does not (same year).
        rows = [
            _cap_row(1, 100, 20200115, "imv", True),
            _cap_row(2, 100, 20200201, "imv", False),
            _cap_row(3, 200, 20200115, "imv", False),
            _cap_row(4, 200, 20200201, "imv", False),
        ]
        df = _run_capability_pipeline(spark, rows, "imv")
        by_claim = {r["CLAIMNO"]: r["providerImvAnnualCapability"] for r in df.collect()}
        assert by_claim[1] == 1 and by_claim[2] == 1
        assert by_claim[3] == 0 and by_claim[4] == 0

    def test_capability_broadcast_to_every_row_in_partition(self, spark):
        # Even rows with the flag=0 carry the partition's max.
        rows = [
            _cap_row(1, 100, 20200115, "rrt", False),
            _cap_row(2, 100, 20200120, "rrt", False),
            _cap_row(3, 100, 20200305, "rrt", True),
        ]
        df = _run_capability_pipeline(spark, rows, "rrt")
        caps = [r["providerRrtAnnualCapability"] for r in df.collect()]
        assert caps == [1, 1, 1]

    def test_full_grid_imv_capability(self, spark):
        # 100/2020 has IMV; 100/2021 does not; 200/2020 does not; 200/2021 has IMV.
        rows = [
            _cap_row(1, 100, 20200115, "imv", True),
            _cap_row(2, 100, 20200201, "imv", False),
            _cap_row(3, 100, 20210110, "imv", False),
            _cap_row(4, 100, 20210410, "imv", False),
            _cap_row(5, 200, 20200115, "imv", False),
            _cap_row(6, 200, 20200201, "imv", False),
            _cap_row(7, 200, 20210110, "imv", True),
            _cap_row(8, 200, 20210410, "imv", False),
        ]
        df = _run_capability_pipeline(spark, rows, "imv")
        by_claim = {r["CLAIMNO"]: r["providerImvAnnualCapability"] for r in df.collect()}
        # 100/2020 -> 1
        assert by_claim[1] == 1 and by_claim[2] == 1
        # 100/2021 -> 0
        assert by_claim[3] == 0 and by_claim[4] == 0
        # 200/2020 -> 0
        assert by_claim[5] == 0 and by_claim[6] == 0
        # 200/2021 -> 1
        assert by_claim[7] == 1 and by_claim[8] == 1

    def test_capability_in_later_prcdr_position_still_counted(self, spark):
        # add_imvPrcdr inspects all 25 ICD_PRCDR_CD columns via add_prcdrCodeAll.
        # Putting the IMV code in ICD_PRCDR_CD9 should still derive imv=1.
        rows = [
            {"DSYSRTKY": 1, "CLAIMNO": 1, "ORGNPINM": 100, "THRU_DT": 20200115,
             "ICD_PRCDR_CD1": "00H00MZ", "ICD_PRCDR_CD9": "5A1935Z"},
            {"DSYSRTKY": 2, "CLAIMNO": 2, "ORGNPINM": 100, "THRU_DT": 20200201,
             "ICD_PRCDR_CD1": "00H00MZ"},
        ]
        df = _run_capability_pipeline(spark, rows, "imv")
        by_claim = {r["CLAIMNO"]: r for r in df.collect()}
        assert by_claim[1]["imv"] == 1
        assert by_claim[2]["imv"] == 0
        # Capability is the partition max -> both rows show capability=1.
        assert by_claim[1]["providerImvAnnualCapability"] == 1
        assert by_claim[2]["providerImvAnnualCapability"] == 1

    def test_input_flag_column_preserved(self, spark):
        # The original binary column (imv) is untouched; capability is a new column.
        df = _run_capability_pipeline(spark, [_cap_row(1, 100, 20200115, "imv", True)], "imv")
        assert "imv" in df.columns
        assert "providerImvAnnualCapability" in df.columns
        row = df.collect()[0]
        assert row["imv"] == 1
        assert row["providerImvAnnualCapability"] == 1


# ============================================================
# End-to-end tests for add_providerEverCapability
#   Forward-propagating (cumulative) variant: once a provider performs the
#   procedure in any year, every year from then on is flagged 1; it never
#   resets, but does NOT propagate backward to earlier years.
# ============================================================

def _run_ever_capability_pipeline(spark, rows, flag):
    from cms.stays import add_providerEverCapability
    return _run_capability_pipeline(spark, rows, flag, capability_fn=add_providerEverCapability)


class TestAddProviderEverCapability:

    @pytest.mark.parametrize("flag", ["imv", "rrt", "ecmo"])
    def test_capability_column_name_follows_convention(self, spark, flag):
        # providerImvEverCapability / providerRrtEverCapability / providerEcmoEverCapability
        df = _run_ever_capability_pipeline(spark, [_cap_row(1, 100, 20200115, flag, True)], flag)
        expected = "provider" + flag[0].upper() + flag[1:] + "EverCapability"
        assert expected in df.columns

    @pytest.mark.parametrize("flag", ["imv", "rrt", "ecmo"])
    def test_capability_propagates_forward_to_later_years(self, spark, flag):
        # Provider 100 performs the procedure in 2018 only; 2019 and 2020 are idle.
        rows = [
            _cap_row(1, 100, 20180115, flag, True),
            _cap_row(2, 100, 20190201, flag, False),
            _cap_row(3, 100, 20200305, flag, False),
        ]
        df = _run_ever_capability_pipeline(spark, rows, flag)
        cap_col = "provider" + flag[0].upper() + flag[1:] + "EverCapability"
        by_claim = {r["CLAIMNO"]: r[cap_col] for r in df.collect()}
        # 2018 sets it; 2019 and 2020 carry it forward.
        assert by_claim[1] == 1
        assert by_claim[2] == 1
        assert by_claim[3] == 1

    @pytest.mark.parametrize("flag", ["imv", "rrt", "ecmo"])
    def test_capability_does_not_propagate_backward(self, spark, flag):
        # Provider 100 is idle in 2018, first performs the procedure in 2019.
        rows = [
            _cap_row(1, 100, 20180115, flag, False),
            _cap_row(2, 100, 20190201, flag, True),
            _cap_row(3, 100, 20200305, flag, False),
        ]
        df = _run_ever_capability_pipeline(spark, rows, flag)
        cap_col = "provider" + flag[0].upper() + flag[1:] + "EverCapability"
        by_claim = {r["CLAIMNO"]: r[cap_col] for r in df.collect()}
        # 2018 predates the first capable year -> 0
        assert by_claim[1] == 0
        # 2019 sets it; 2020 carries it forward.
        assert by_claim[2] == 1
        assert by_claim[3] == 1

    @pytest.mark.parametrize("flag", ["imv", "rrt", "ecmo"])
    def test_provider_never_capable_stays_0(self, spark, flag):
        rows = [
            _cap_row(1, 100, 20180115, flag, False),
            _cap_row(2, 100, 20190201, flag, False),
        ]
        df = _run_ever_capability_pipeline(spark, rows, flag)
        cap_col = "provider" + flag[0].upper() + flag[1:] + "EverCapability"
        for r in df.collect():
            assert r[cap_col] == 0

    def test_capability_independent_per_provider(self, spark):
        # Provider 100 capable in 2018; provider 200 never capable.
        rows = [
            _cap_row(1, 100, 20180115, "imv", True),
            _cap_row(2, 100, 20190201, "imv", False),
            _cap_row(3, 200, 20180115, "imv", False),
            _cap_row(4, 200, 20190201, "imv", False),
        ]
        df = _run_ever_capability_pipeline(spark, rows, "imv")
        by_claim = {r["CLAIMNO"]: r["providerImvEverCapability"] for r in df.collect()}
        assert by_claim[1] == 1 and by_claim[2] == 1
        assert by_claim[3] == 0 and by_claim[4] == 0

    def test_multiple_rows_same_year_all_carry_flag(self, spark):
        # Two claims share the first capable year; both, plus a later year, are flagged.
        rows = [
            _cap_row(1, 100, 20180115, "imv", False),
            _cap_row(2, 100, 20180310, "imv", True),
            _cap_row(3, 100, 20190201, "imv", False),
        ]
        df = _run_ever_capability_pipeline(spark, rows, "imv")
        caps = [r["providerImvEverCapability"] for r in df.collect()]
        assert caps == [1, 1, 1]

    def test_input_flag_column_preserved(self, spark):
        df = _run_ever_capability_pipeline(spark, [_cap_row(1, 100, 20200115, "imv", True)], "imv")
        assert "imv" in df.columns
        assert "providerImvEverCapability" in df.columns
        row = df.collect()[0]
        assert row["imv"] == 1
        assert row["providerImvEverCapability"] == 1


# ============================================================
# End-to-end pipeline helper for stroke-volume / treatment functions.
#
# Builds real ipBase rows and runs the full upstream pipeline that the
# add_provider*Vol / Mean / Capability functions in cms.stays depend on:
#
#   ipBase rows (ADMSN_DT, THRU_DT, PRNCPAL_DGNS_CD, ICD_PRCDR_CD1)
#     -> add_admission_date_info("ip")    (ADMSN_DT_DAY)
#     -> add_through_date_info             (THRU_DT_YEAR, THRU_DT_DAY)
#     -> add_dgnsCodeAll                   (dgnsCodeAll array)
#     -> add_prcdrCodeAll                  (prcdrCodeAll array)
#     -> add_ishStroke / add_otherStroke / add_ichStroke / add_tiaStroke
#     -> add_anyStroke                     (OR of the four stroke flags)
#     -> add_tpa(inpatient=...)            (tpa from DRG/DGNS/PRCDR codes)
#     -> add_evt (inpatient only)          (evt from DRG/PRCDR codes)
#     -> add_los                           (THRU_DT_DAY - ADMSN_DT_DAY + 1)
# ============================================================

# Codes that trigger each per-claim flag the stays functions consume.
_ISH_STROKE_DGNS_CODE = "I639"   # add_ishStrokeDgns matches /^I63\d*/
_OTHER_STROKE_CODE = "I64"       # add_otherStroke
_TPA_PRCDR_CODE = "3E03317"      # add_tpaPrcdr
_EVT_PRCDR_CODE = "03CG3ZZ"      # add_evtPrcdr


def _stroke_row(claimno, orgnpinm, admsn_dt, thru_dt, *,
                dsysrtky=None, dgns=None, prcdr=None, provider="P1"):
    """Build a single ipBase row with the fields the stroke/treatment
    pipelines read. Anything else in the schema will be padded with NULL."""
    return {
        "DSYSRTKY": dsysrtky if dsysrtky is not None else claimno,
        "CLAIMNO": claimno,
        "ORGNPINM": orgnpinm,
        "PROVIDER": provider,
        "ADMSN_DT": admsn_dt,
        "THRU_DT": thru_dt,
        "PRNCPAL_DGNS_CD": dgns,
        "ICD_PRCDR_CD1": prcdr,
    }


def _run_stroke_pipeline(spark, rows, inpatient=True):
    from cms.utilities import add_through_date_info
    from cms.base import (
        add_admission_date_info,
        add_dgnsCodeAll, add_prcdrCodeAll,
        add_ishStroke, add_otherStroke, add_ichStroke, add_tiaStroke,
        add_anyStroke, add_tpa, add_evt, add_los,
    )
    df = make_real_claim_df(spark, "ipBase", rows)
    df = add_admission_date_info(df, "ip")
    df = add_through_date_info(df)
    df = add_dgnsCodeAll(df)
    df = add_prcdrCodeAll(df)
    df = add_ishStroke(df, inpatient=inpatient)
    df = add_otherStroke(df)
    df = add_ichStroke(df)
    df = add_tiaStroke(df)
    df = add_anyStroke(df)
    df = add_tpa(df, inpatient=inpatient)
    if inpatient:
        df = add_evt(df)
    df = add_los(df)
    return df


# ============================================================
# End-to-end tests for stroke annual volume via add_providerAnnualVolume
# (col="anyStroke" / col="ishStroke")
# ============================================================

class TestAddProviderStrokeAnnualVolume:

    def test_anyStroke_sums_per_provider_year(self, spark):
        from cms.stays import add_providerAnnualVolume
        # 3 ish-stroke + 1 other-stroke claims for provider 100/2020; the 5th claim is no stroke.
        rows = [
            _stroke_row(1, 100, 20200115, 20200120, dgns=_ISH_STROKE_DGNS_CODE),
            _stroke_row(2, 100, 20200201, 20200205, dgns=_ISH_STROKE_DGNS_CODE),
            _stroke_row(3, 100, 20200305, 20200310, dgns=_ISH_STROKE_DGNS_CODE),
            _stroke_row(4, 100, 20200410, 20200412, dgns=_OTHER_STROKE_CODE),
            _stroke_row(5, 100, 20200515, 20200520, dgns="I10"),
        ]
        df = _run_stroke_pipeline(spark, rows)
        df = add_providerAnnualVolume(df, col="anyStroke")
        by_claim = {r["CLAIMNO"]: r["providerAnyStrokeAnnualVolume"] for r in df.collect()}
        assert all(v == 4 for v in by_claim.values())

    def test_partition_independent_per_year_and_provider(self, spark):
        from cms.stays import add_providerAnnualVolume
        # 100/2020 -> 2 strokes; 100/2021 -> 1; 200/2020 -> 3.
        rows = [
            _stroke_row(1, 100, 20200115, 20200120, dgns=_ISH_STROKE_DGNS_CODE),
            _stroke_row(2, 100, 20200201, 20200205, dgns=_ISH_STROKE_DGNS_CODE),
            _stroke_row(3, 100, 20200410, 20200412, dgns="I10"),
            _stroke_row(4, 100, 20210110, 20210115, dgns=_ISH_STROKE_DGNS_CODE),
            _stroke_row(5, 200, 20200115, 20200120, dgns=_ISH_STROKE_DGNS_CODE),
            _stroke_row(6, 200, 20200201, 20200205, dgns=_OTHER_STROKE_CODE),
            _stroke_row(7, 200, 20200305, 20200310, dgns=_ISH_STROKE_DGNS_CODE),
        ]
        df = _run_stroke_pipeline(spark, rows)
        df = add_providerAnnualVolume(df, col="anyStroke")
        by_claim = {r["CLAIMNO"]: r["providerAnyStrokeAnnualVolume"] for r in df.collect()}
        for cn in (1, 2, 3):
            assert by_claim[cn] == 2
        assert by_claim[4] == 1
        for cn in (5, 6, 7):
            assert by_claim[cn] == 3

    def test_ish_stroke_only_counts_ischemic(self, spark):
        from cms.stays import add_providerAnnualVolume
        # col="ishStroke" sums ishStroke, so only ish-stroke rows count.
        rows = [
            _stroke_row(1, 100, 20200115, 20200120, dgns=_ISH_STROKE_DGNS_CODE),
            _stroke_row(2, 100, 20200201, 20200205, dgns=_ISH_STROKE_DGNS_CODE),
            _stroke_row(3, 100, 20200410, 20200412, dgns=_OTHER_STROKE_CODE),  # other, not ish
        ]
        df = _run_stroke_pipeline(spark, rows)
        df = add_providerAnnualVolume(df, col="ishStroke")
        for r in df.collect():
            assert r["providerIshStrokeAnnualVolume"] == 2

    def test_no_strokes_returns_zero(self, spark):
        from cms.stays import add_providerAnnualVolume
        rows = [_stroke_row(i, 100, 20200115, 20200120, dgns="I10") for i in range(1, 4)]
        df = _run_stroke_pipeline(spark, rows)
        df = add_providerAnnualVolume(df, col="anyStroke")
        for r in df.collect():
            assert r["anyStroke"] == 0
            assert r["providerAnyStrokeAnnualVolume"] == 0

    def test_value_broadcast_to_every_row(self, spark):
        from cms.stays import add_providerAnnualVolume
        # Window-sum: the volume attaches to every row in the partition,
        # including non-stroke claims.
        rows = [
            _stroke_row(1, 100, 20200115, 20200120, dgns=_ISH_STROKE_DGNS_CODE),
            _stroke_row(2, 100, 20200201, 20200205, dgns="I10"),
            _stroke_row(3, 100, 20200305, 20200310, dgns="I10"),
        ]
        df = _run_stroke_pipeline(spark, rows)
        df = add_providerAnnualVolume(df, col="anyStroke")
        for r in df.collect():
            assert r["providerAnyStrokeAnnualVolume"] == 1


# ============================================================
# End-to-end tests for add_provider_stroke_treatment_info
# ============================================================

class TestAddProviderStrokeTreatmentInfo:

    def test_inpatient_adds_all_four_columns(self, spark):
        from cms.stays import add_provider_stroke_treatment_info
        rows = [
            _stroke_row(1, 100, 20200115, 20200120,
                        dgns=_ISH_STROKE_DGNS_CODE, prcdr=_TPA_PRCDR_CODE),
            _stroke_row(2, 100, 20200201, 20200205,
                        dgns=_ISH_STROKE_DGNS_CODE, prcdr=_EVT_PRCDR_CODE),
            _stroke_row(3, 100, 20200305, 20200310,
                        dgns=_ISH_STROKE_DGNS_CODE),
            _stroke_row(4, 100, 20200410, 20200412, dgns="I10"),
        ]
        df = _run_stroke_pipeline(spark, rows, inpatient=True)
        df = add_provider_stroke_treatment_info(df, inpatient=True)
        for col in ("providerTpaMean", "providerTpaVol", "providerEvtMean", "providerEvtVol"):
            assert col in df.columns
        rows_out = df.collect()
        # 1 of 4 claims has tpa=1, 1 of 4 has evt=1.
        for r in rows_out:
            assert r["providerTpaVol"] == 1
            assert r["providerEvtVol"] == 1
            assert r["providerTpaMean"] == pytest.approx(0.25)
            assert r["providerEvtMean"] == pytest.approx(0.25)

    def test_outpatient_does_not_add_evt_columns(self, spark):
        from cms.stays import add_provider_stroke_treatment_info
        rows = [
            _stroke_row(1, 100, 20200115, 20200120,
                        dgns=_ISH_STROKE_DGNS_CODE, prcdr=_TPA_PRCDR_CODE),
            _stroke_row(2, 100, 20200201, 20200205, dgns="I10"),
        ]
        # inpatient=False: add_tpa needs only the procedure code, add_evt is skipped.
        df = _run_stroke_pipeline(spark, rows, inpatient=False)
        df = add_provider_stroke_treatment_info(df, inpatient=False)
        assert "providerTpaMean" in df.columns
        assert "providerTpaVol" in df.columns
        assert "providerEvtMean" not in df.columns
        assert "providerEvtVol" not in df.columns
        for r in df.collect():
            assert r["providerTpaVol"] == 1
            assert r["providerTpaMean"] == pytest.approx(0.5)

    def test_partition_independent_per_provider_year(self, spark):
        from cms.stays import add_provider_stroke_treatment_info
        # 100/2020 -> 2 tpa; 100/2021 -> 0 tpa; 200/2020 -> 1 tpa.
        rows = [
            _stroke_row(1, 100, 20200115, 20200120,
                        dgns=_ISH_STROKE_DGNS_CODE, prcdr=_TPA_PRCDR_CODE),
            _stroke_row(2, 100, 20200201, 20200205,
                        dgns=_ISH_STROKE_DGNS_CODE, prcdr=_TPA_PRCDR_CODE),
            _stroke_row(3, 100, 20210110, 20210115, dgns=_ISH_STROKE_DGNS_CODE),
            _stroke_row(4, 200, 20200115, 20200120,
                        dgns=_ISH_STROKE_DGNS_CODE, prcdr=_TPA_PRCDR_CODE),
            _stroke_row(5, 200, 20200201, 20200205, dgns="I10"),
        ]
        df = _run_stroke_pipeline(spark, rows, inpatient=True)
        df = add_provider_stroke_treatment_info(df, inpatient=True)
        by_claim = {r["CLAIMNO"]: r for r in df.collect()}
        for cn in (1, 2):
            assert by_claim[cn]["providerTpaVol"] == 2
            assert by_claim[cn]["providerTpaMean"] == pytest.approx(1.0)
        assert by_claim[3]["providerTpaVol"] == 0
        assert by_claim[3]["providerTpaMean"] == pytest.approx(0.0)
        for cn in (4, 5):
            assert by_claim[cn]["providerTpaVol"] == 1
            assert by_claim[cn]["providerTpaMean"] == pytest.approx(0.5)


# ============================================================
# End-to-end tests for add_provider_stroke_info
# ============================================================

class TestAddProviderStrokeInfo:

    def test_inpatient_adds_treatment_and_volume_columns(self, spark):
        from cms.stays import add_provider_stroke_info
        rows = [
            _stroke_row(1, 100, 20200115, 20200120,
                        dgns=_ISH_STROKE_DGNS_CODE, prcdr=_TPA_PRCDR_CODE),
            _stroke_row(2, 100, 20200201, 20200205,
                        dgns=_ISH_STROKE_DGNS_CODE, prcdr=_EVT_PRCDR_CODE),
            _stroke_row(3, 100, 20200305, 20200310, dgns=_OTHER_STROKE_CODE),
            _stroke_row(4, 100, 20200410, 20200412, dgns="I10"),
        ]
        df = _run_stroke_pipeline(spark, rows, inpatient=True)
        df = add_provider_stroke_info(df, inpatient=True, stroke="anyStroke")
        for col in ("providerTpaMean", "providerTpaVol",
                    "providerEvtMean", "providerEvtVol",
                    "providerAnyStrokeAnnualVolume"):
            assert col in df.columns
        row = df.collect()[0]
        # 3 strokes (claims 1,2,3) across 4 claims in 100/2020.
        assert row["providerAnyStrokeAnnualVolume"] == 3
        assert row["providerTpaVol"] == 1
        assert row["providerEvtVol"] == 1

    def test_outpatient_skips_evt_columns(self, spark):
        from cms.stays import add_provider_stroke_info
        rows = [
            _stroke_row(1, 100, 20200115, 20200120,
                        dgns=_ISH_STROKE_DGNS_CODE, prcdr=_TPA_PRCDR_CODE),
            _stroke_row(2, 100, 20200201, 20200205, dgns="I10"),
        ]
        df = _run_stroke_pipeline(spark, rows, inpatient=False)
        df = add_provider_stroke_info(df, inpatient=False, stroke="anyStroke")
        assert "providerTpaMean" in df.columns
        assert "providerTpaVol" in df.columns
        assert "providerAnyStrokeAnnualVolume" in df.columns
        assert "providerEvtMean" not in df.columns
        assert "providerEvtVol" not in df.columns

    def test_custom_stroke_column_used_for_volume(self, spark):
        from cms.stays import add_provider_stroke_info
        rows = [
            _stroke_row(1, 100, 20200115, 20200120, dgns=_ISH_STROKE_DGNS_CODE),
            _stroke_row(2, 100, 20200201, 20200205, dgns=_OTHER_STROKE_CODE),
        ]
        df = _run_stroke_pipeline(spark, rows, inpatient=True)
        df = add_provider_stroke_info(df, inpatient=True, stroke="ishStroke")
        for r in df.collect():
            # Only the ish-stroke claim is counted (custom stroke column).
            assert r["providerIshStrokeAnnualVolume"] == 1


# ============================================================
# End-to-end tests for add_provider_septic_shock_info (thin wrapper)
# ============================================================

class TestAddProviderSepticShockInfo:

    def test_adds_provider_septic_shock_vol(self, spark):
        # Same pipeline as TestAddProviderSepticShockAnnualVolume but invokes the wrapper.
        from cms.stays import add_provider_septic_shock_info
        rows = [
            _row(1, 100, 20200115, True),
            _row(2, 100, 20200120, True),
            _row(3, 100, 20200201, False),
            _row(4, 200, 20200115, False),
        ]
        from cms.utilities import add_through_date_info
        from cms.base import add_dgnsCodeAll, add_septicShock
        df = make_real_claim_df(spark, "ipBase", rows)
        df = add_through_date_info(df)
        df = add_dgnsCodeAll(df)
        df = add_septicShock(df)
        df = add_provider_septic_shock_info(df)
        assert "providerSepticShockAnnualVolume" in df.columns
        by_claim = {r["CLAIMNO"]: r["providerSepticShockAnnualVolume"] for r in df.collect()}
        for cn in (1, 2, 3):
            assert by_claim[cn] == 2
        assert by_claim[4] == 0


# ============================================================
# End-to-end tests for add_provider_capability_and_volume_info (thin wrapper)
# ============================================================

class TestAddProviderCapabilityAndVolumeInfo:

    def test_adds_all_three_columns_with_correct_values(self, spark):
        # Provider 100 is capable in 2019 but not 2020; provider 200 is never capable.
        # This separates the three columns: AnnualCapability is per-year, EverCapability
        # propagates forward, AnnualVolume is the per-year count.
        from cms.stays import add_provider_capability_and_volume_info
        rows = [
            _cap_row(1, 100, 20190115, "imv", True),
            _cap_row(2, 100, 20200115, "imv", False),
            _cap_row(3, 200, 20190115, "imv", False),
        ]
        df = _run_capability_pipeline(
            spark, rows, "imv", capability_fn=add_provider_capability_and_volume_info
        )
        for col in ("providerImvAnnualCapability",
                    "providerImvEverCapability",
                    "providerImvAnnualVolume"):
            assert col in df.columns
        by_claim = {r["CLAIMNO"]: r for r in df.collect()}
        # provider 100, 2019: capable this year, ever capable, one EVT
        assert by_claim[1]["providerImvAnnualCapability"] == 1
        assert by_claim[1]["providerImvEverCapability"] == 1
        assert by_claim[1]["providerImvAnnualVolume"] == 1
        # provider 100, 2020: not capable this year, but ever capable carries forward
        assert by_claim[2]["providerImvAnnualCapability"] == 0
        assert by_claim[2]["providerImvEverCapability"] == 1
        assert by_claim[2]["providerImvAnnualVolume"] == 0
        # provider 200: never capable
        assert by_claim[3]["providerImvAnnualCapability"] == 0
        assert by_claim[3]["providerImvEverCapability"] == 0
        assert by_claim[3]["providerImvAnnualVolume"] == 0


# ============================================================
# End-to-end pipeline helper for add_provider_revenue_info:
# build opBase + opRevenue, summarize revenue, join with claims, and
# then compute provider-level ed/ct/mri means and volumes.
# ============================================================

def _run_provider_revenue_pipeline(spark, base_rows, rev_rows):
    """base_rows: list of opBase dicts (must specify DSYSRTKY, CLAIMNO,
    ORGNPINM, THRU_DT). rev_rows: list of opRevenue dicts (must specify
    DSYSRTKY, CLAIMNO, THRU_DT, REV_CNTR)."""
    from cms.utilities import add_through_date_info
    from cms.revenue import get_revenue_info
    from cms.claims import get_claims
    from cms.stays import add_provider_revenue_info
    baseDF = make_real_claim_df(spark, "opBase", base_rows)
    baseDF = add_through_date_info(baseDF)
    revDF = make_real_claim_df(spark, "opRevenue", rev_rows)
    summary = get_revenue_info(revDF, inClaim=True)
    claimsDF = get_claims(baseDF, summary)
    return add_provider_revenue_info(claimsDF)


def _base(claimno, orgnpinm, thru_dt, dsysrtky=None):
    return {
        "DSYSRTKY": dsysrtky if dsysrtky is not None else claimno,
        "CLAIMNO": claimno,
        "ORGNPINM": orgnpinm,
        "PROVIDER": "P1",
        "THRU_DT": thru_dt,
    }


def _rev(claimno, thru_dt, rev_cntr, dsysrtky=None):
    return {
        "DSYSRTKY": dsysrtky if dsysrtky is not None else claimno,
        "CLAIMNO": claimno,
        "THRU_DT": thru_dt,
        "REV_CNTR": rev_cntr,
    }


# ============================================================
# End-to-end tests for add_provider_revenue_info
# ============================================================

class TestAddProviderRevenueInfo:

    def test_adds_all_six_columns(self, spark):
        # One claim each with ed/ct/mri; the fourth has none.
        base_rows = [
            _base(1, 100, 20200115),
            _base(2, 100, 20200201),
            _base(3, 100, 20200305),
            _base(4, 100, 20200410),
        ]
        rev_rows = [
            _rev(1, 20200115, 455),   # ED (450-459)
            _rev(2, 20200201, 355),   # CT (350-359)
            _rev(3, 20200305, 612),   # MRI (610-619)
            _rev(4, 20200410, 100),   # unrelated
        ]
        df = _run_provider_revenue_pipeline(spark, base_rows, rev_rows)
        for col in ("providerEdMean", "providerEdVol",
                    "providerCtMean", "providerCtVol",
                    "providerMriMean", "providerMriVol"):
            assert col in df.columns
        for r in df.collect():
            assert r["providerEdVol"] == 1
            assert r["providerCtVol"] == 1
            assert r["providerMriVol"] == 1
            assert r["providerEdMean"] == pytest.approx(0.25)
            assert r["providerCtMean"] == pytest.approx(0.25)
            assert r["providerMriMean"] == pytest.approx(0.25)

    def test_partition_independent_per_provider_year(self, spark):
        # 100/2020 -> 2 ed of 3 claims; 200/2020 -> 0 ed of 2 claims; 100/2021 -> 1 ed of 1 claim.
        base_rows = [
            _base(1, 100, 20200115),
            _base(2, 100, 20200201),
            _base(3, 100, 20200305),
            _base(4, 200, 20200115),
            _base(5, 200, 20200201),
            _base(6, 100, 20210110),
        ]
        rev_rows = [
            _rev(1, 20200115, 450),  # ed
            _rev(2, 20200201, 451),  # ed
            _rev(3, 20200305, 100),  # neither
            _rev(4, 20200115, 100),
            _rev(5, 20200201, 100),
            _rev(6, 20210110, 459),  # ed
        ]
        df = _run_provider_revenue_pipeline(spark, base_rows, rev_rows)
        by_claim = {r["CLAIMNO"]: r for r in df.collect()}
        for cn in (1, 2, 3):
            assert by_claim[cn]["providerEdVol"] == 2
            assert by_claim[cn]["providerEdMean"] == pytest.approx(2 / 3)
        for cn in (4, 5):
            assert by_claim[cn]["providerEdVol"] == 0
            assert by_claim[cn]["providerEdMean"] == pytest.approx(0.0)
        assert by_claim[6]["providerEdVol"] == 1
        assert by_claim[6]["providerEdMean"] == pytest.approx(1.0)

    def test_zero_volume_when_no_revenue_matches(self, spark):
        base_rows = [_base(1, 100, 20200115), _base(2, 100, 20200201)]
        rev_rows = [_rev(1, 20200115, 100), _rev(2, 20200201, 100)]
        df = _run_provider_revenue_pipeline(spark, base_rows, rev_rows)
        for r in df.collect():
            assert r["providerEdVol"] == 0
            assert r["providerCtVol"] == 0
            assert r["providerMriVol"] == 0


# ============================================================
# End-to-end pipeline helper for propagate_stay_info / get_unique_stays.
# Builds ipBase rows that share (DSYSRTKY, PROVIDER, ORGNPINM, ADMSN_DT_DAY)
# so multiple claims map to the same facility stay, then runs the full
# upstream pipeline needed by the columnsToPropagate list.
# ============================================================

def _stay_row(claimno, *, dsysrtky, orgnpinm, admsn_dt, thru_dt,
              provider="P1", dgns=None, prcdr=None):
    return {
        "DSYSRTKY": dsysrtky,
        "CLAIMNO": claimno,
        "PROVIDER": provider,
        "ORGNPINM": orgnpinm,
        "ADMSN_DT": admsn_dt,
        "THRU_DT": thru_dt,
        "PRNCPAL_DGNS_CD": dgns,
        "ICD_PRCDR_CD1": prcdr,
        "STUS_CD": None,
    }


def _run_ip_stay_pipeline(spark, rows):
    """Run all upstream functions whose columns appear in
    propagate_stay_info.columnsToPropagate (those that exist on ipBase)."""
    from cms.utilities import add_through_date_info
    from cms.base import (
        add_admission_date_info,
        add_dgnsCodeAll, add_prcdrCodeAll,
        add_ishStroke, add_tpa, add_evt, add_diedInVisit,
    )
    df = make_real_claim_df(spark, "ipBase", rows)
    df = add_admission_date_info(df, "ip")
    df = add_through_date_info(df)
    df = add_dgnsCodeAll(df)
    df = add_prcdrCodeAll(df)
    df = add_ishStroke(df, inpatient=True)
    df = add_tpa(df, inpatient=True)
    df = add_evt(df)
    df = add_diedInVisit(df)
    return df


# ============================================================
# End-to-end tests for propagate_stay_info
# ============================================================

class TestPropagateStayInfo:

    def test_two_claims_same_ip_stay_propagate_max(self, spark):
        # Two claims in the same stay (same DSYSRTKY/PROVIDER/ORGNPINM/ADMSN_DT_DAY).
        # Only the first carries a tPA procedure code; after propagation, both rows
        # should have tpa=1 / tpaPrcdr=1.
        from cms.stays import propagate_stay_info
        rows = [
            _stay_row(10, dsysrtky=1, orgnpinm=100, admsn_dt=20200115,
                      thru_dt=20200120, dgns=_ISH_STROKE_DGNS_CODE, prcdr=_TPA_PRCDR_CODE),
            _stay_row(11, dsysrtky=1, orgnpinm=100, admsn_dt=20200115,
                      thru_dt=20200125, dgns=_ISH_STROKE_DGNS_CODE),
        ]
        df = _run_ip_stay_pipeline(spark, rows)
        df = propagate_stay_info(df, claimType="ip")
        by_claim = {r["CLAIMNO"]: r for r in df.collect()}
        assert by_claim[10]["tpa"] == 1 and by_claim[11]["tpa"] == 1
        assert by_claim[10]["tpaPrcdr"] == 1 and by_claim[11]["tpaPrcdr"] == 1
        assert by_claim[10]["ishStroke"] == 1 and by_claim[11]["ishStroke"] == 1

    def test_different_admission_days_are_separate_stays(self, spark):
        # Different ADMSN_DT_DAY values -> different partitions; no propagation.
        from cms.stays import propagate_stay_info
        rows = [
            _stay_row(10, dsysrtky=1, orgnpinm=100, admsn_dt=20200115,
                      thru_dt=20200120, prcdr=_TPA_PRCDR_CODE,
                      dgns=_ISH_STROKE_DGNS_CODE),
            _stay_row(11, dsysrtky=1, orgnpinm=100, admsn_dt=20200201,
                      thru_dt=20200205, dgns="I10"),
        ]
        df = _run_ip_stay_pipeline(spark, rows)
        df = propagate_stay_info(df, claimType="ip")
        by_claim = {r["CLAIMNO"]: r for r in df.collect()}
        assert by_claim[10]["tpa"] == 1
        assert by_claim[11]["tpa"] == 0

    def test_different_beneficiaries_are_separate_stays(self, spark):
        # Same admission day, same provider, but different DSYSRTKY -> no propagation.
        from cms.stays import propagate_stay_info
        rows = [
            _stay_row(10, dsysrtky=1, orgnpinm=100, admsn_dt=20200115,
                      thru_dt=20200120, prcdr=_TPA_PRCDR_CODE,
                      dgns=_ISH_STROKE_DGNS_CODE),
            _stay_row(11, dsysrtky=2, orgnpinm=100, admsn_dt=20200115,
                      thru_dt=20200120, dgns="I10"),
        ]
        df = _run_ip_stay_pipeline(spark, rows)
        df = propagate_stay_info(df, claimType="ip")
        by_claim = {r["CLAIMNO"]: r for r in df.collect()}
        assert by_claim[10]["tpa"] == 1
        assert by_claim[11]["tpa"] == 0

    def test_op_partition_uses_thru_dt_day(self, spark):
        # For op claims, the stay partition is keyed by THRU_DT_DAY instead of
        # ADMSN_DT_DAY. Build minimal op-style rows (ipBase schema works because
        # we just need DSYSRTKY/PROVIDER/ORGNPINM/THRU_DT_DAY + one propagatable
        # column).
        from cms.stays import propagate_stay_info
        from cms.utilities import add_through_date_info
        from cms.base import add_dgnsCodeAll, add_prcdrCodeAll, add_tpa
        rows = [
            # Same DSYSRTKY/PROVIDER/ORGNPINM/THRU_DT_DAY -> same op stay.
            {"DSYSRTKY": 1, "CLAIMNO": 10, "PROVIDER": "P1", "ORGNPINM": 100,
             "THRU_DT": 20200115, "ICD_PRCDR_CD1": _TPA_PRCDR_CODE},
            {"DSYSRTKY": 1, "CLAIMNO": 11, "PROVIDER": "P1", "ORGNPINM": 100,
             "THRU_DT": 20200115},
        ]
        df = make_real_claim_df(spark, "opBase", rows)
        df = add_through_date_info(df)
        df = add_dgnsCodeAll(df)
        df = add_prcdrCodeAll(df)
        df = add_tpa(df, inpatient=False)  # op: only PRCDR-based tpa
        df = propagate_stay_info(df, claimType="op")
        by_claim = {r["CLAIMNO"]: r["tpa"] for r in df.collect()}
        assert by_claim[10] == 1 and by_claim[11] == 1


# ============================================================
# End-to-end tests for get_unique_stays
# ============================================================

class TestGetUniqueStays:

    def test_keeps_latest_thru_row_per_stay(self, spark):
        # Two claims in the same ip stay -> the one with the latest THRU_DT survives
        # (it represents the final billed portion of the stay). The smaller CLAIMNO
        # belongs to the earlier interim bill and is dropped.
        from cms.stays import get_unique_stays
        rows = [
            _stay_row(5, dsysrtky=1, orgnpinm=100, admsn_dt=20200115,
                      thru_dt=20200120, prcdr=_TPA_PRCDR_CODE,
                      dgns=_ISH_STROKE_DGNS_CODE),
            _stay_row(10, dsysrtky=1, orgnpinm=100, admsn_dt=20200115,
                       thru_dt=20200125, dgns=_ISH_STROKE_DGNS_CODE),
        ]
        df = _run_ip_stay_pipeline(spark, rows)
        out = get_unique_stays(df, claimType="ip").collect()
        assert len(out) == 1
        assert out[0]["CLAIMNO"] == 10  # latest THRU_DT, not min(CLAIMNO)
        assert out[0]["THRU_DT"] == 20200125
        # Information was propagated before the dedup, so the surviving row
        # carries the tpa=1 from the dropped sibling claim.
        assert out[0]["tpa"] == 1

    def test_interim_billed_ip_stay_keeps_latest_discharge_across_years(self, spark):
        # Interim institutional billing: every interim claim repeats the original
        # ADMSN_DT and advances THRU_DT. CLAIMNO resets every year, so the 2021
        # final bill can carry a smaller CLAIMNO than the 2017 first bill.
        # Old rule (min CLAIMNO) would pick whichever claim happened to have the
        # smallest raw CLAIMNO across years; new rule picks the row with the
        # latest THRU_DT_DAY regardless of CLAIMNO, so the stay's surviving
        # discharge is genuinely the final discharge.
        from cms.stays import get_unique_stays
        rows = [
            # 2017 first interim bill: large CLAIMNO from late in 2017's sequence.
            _stay_row(9999, dsysrtky=1, orgnpinm=100, admsn_dt=20170315,
                      thru_dt=20170415, dgns="I10"),
            # 2021 final bill: small CLAIMNO from early in 2021's sequence.
            _stay_row(50, dsysrtky=1, orgnpinm=100, admsn_dt=20170315,
                      thru_dt=20210315, dgns="I10"),
        ]
        df = _run_ip_stay_pipeline(spark, rows)
        out = get_unique_stays(df, claimType="ip").collect()
        assert len(out) == 1
        # Latest THRU wins -- CLAIMNO=50 here -- so the old min(CLAIMNO) rule
        # would coincidentally agree on this row but for the wrong reason.
        # The discriminating assertions are on the dates: the surviving row
        # carries the 2021 final discharge, not the 2017 first one.
        assert out[0]["CLAIMNO"] == 50
        assert out[0]["THRU_DT"] == 20210315
        assert out[0]["THRU_DT_YEAR"] == 2021

    def test_max_thru_row_kept_even_when_its_claimno_is_larger(self, spark):
        # Discriminating test: under the old min(CLAIMNO) rule the 2017 row would
        # win (smaller CLAIMNO), under the new max(THRU_DT_DAY) rule the 2021
        # row wins. The two rules give different survivors here.
        from cms.stays import get_unique_stays
        rows = [
            _stay_row(50, dsysrtky=1, orgnpinm=100, admsn_dt=20170315,
                      thru_dt=20170415, dgns="I10"),
            _stay_row(9999, dsysrtky=1, orgnpinm=100, admsn_dt=20170315,
                      thru_dt=20210315, dgns="I10"),
        ]
        df = _run_ip_stay_pipeline(spark, rows)
        out = get_unique_stays(df, claimType="ip").collect()
        assert len(out) == 1
        assert out[0]["CLAIMNO"] == 9999
        assert out[0]["THRU_DT_YEAR"] == 2021

    def test_tiebreak_uses_min_claimno_when_thru_dt_day_ties(self, spark):
        # When two claims in a stay share THRU_DT_DAY (e.g. OP claims, where THRU
        # is the partition key), the desc(THRU_DT_DAY) clause produces a tie and
        # the asc(CLAIMNO) tiebreaker picks the smaller CLAIMNO. Both tied rows
        # share THRU_DT_YEAR so the choice is safe across years.
        from cms.stays import get_unique_stays
        rows = [
            _stay_row(10, dsysrtky=1, orgnpinm=100, admsn_dt=20200115,
                      thru_dt=20200120, dgns="I10"),
            _stay_row(5, dsysrtky=1, orgnpinm=100, admsn_dt=20200115,
                      thru_dt=20200120, dgns="I10"),
        ]
        df = _run_ip_stay_pipeline(spark, rows)
        out = get_unique_stays(df, claimType="ip").collect()
        assert len(out) == 1
        assert out[0]["CLAIMNO"] == 5

    def test_distinct_stays_all_preserved(self, spark):
        from cms.stays import get_unique_stays
        rows = [
            _stay_row(1, dsysrtky=1, orgnpinm=100, admsn_dt=20200115,
                      thru_dt=20200120, dgns="I10"),
            _stay_row(2, dsysrtky=2, orgnpinm=100, admsn_dt=20200115,
                      thru_dt=20200120, dgns="I10"),
            _stay_row(3, dsysrtky=3, orgnpinm=100, admsn_dt=20200201,
                      thru_dt=20200205, dgns="I10"),
        ]
        df = _run_ip_stay_pipeline(spark, rows)
        out = get_unique_stays(df, claimType="ip").collect()
        assert sorted(r["CLAIMNO"] for r in out) == [1, 2, 3]

    def test_drops_intermediate_helper_column(self, spark):
        from cms.stays import get_unique_stays
        rows = [
            _stay_row(1, dsysrtky=1, orgnpinm=100, admsn_dt=20200115,
                      thru_dt=20200120, dgns="I10"),
        ]
        df = _run_ip_stay_pipeline(spark, rows)
        result_df = get_unique_stays(df, claimType="ip")
        assert "stayRowNumber" not in result_df.columns


# ============================================================
# End-to-end tests for get_stays (op pipeline: base + revenue summary)
# ============================================================

class TestGetStays:

    def test_op_pipeline_joins_revenue_and_dedups(self, spark):
        from cms.utilities import add_through_date_info
        from cms.revenue import get_revenue_info
        from cms.stays import get_stays
        # Two op claims for the same beneficiary/provider on the same THRU_DT
        # are treated as the same op stay. For op stays THRU_DT_DAY is the
        # partition key so the desc(THRU_DT_DAY) clause ties; the asc(CLAIMNO)
        # tiebreaker keeps the smaller CLAIMNO after the propagate + dedup
        # inside get_stays.
        base_rows = [
            _base(10, 100, 20200115, dsysrtky=1),
            _base(5,  100, 20200115, dsysrtky=1),
            _base(7,  100, 20200201, dsysrtky=2),
        ]
        baseDF = make_real_claim_df(spark, "opBase", base_rows)
        baseDF = add_through_date_info(baseDF)
        rev_rows = [
            _rev(10, 20200115, 455, dsysrtky=1),  # ed in the larger-CLAIMNO sibling
            _rev(7,  20200201, 612, dsysrtky=2),  # mri on the standalone stay
        ]
        revDF = make_real_claim_df(spark, "opRevenue", rev_rows)
        summary = get_revenue_info(revDF, inClaim=True)
        out = get_stays(baseDF, summary, claimType="op").collect()
        by_claim = {r["CLAIMNO"]: r for r in out}
        # Only the min-CLAIMNO claim per stay survives.
        assert set(by_claim.keys()) == {5, 7}
        # ed was on CLAIMNO=10 originally; propagate_stay_info pulled it onto CLAIMNO=5.
        assert by_claim[5]["ed"] == 1
        assert by_claim[7]["mri"] == 1

    def test_revenue_columns_present(self, spark):
        from cms.utilities import add_through_date_info
        from cms.revenue import get_revenue_info
        from cms.stays import get_stays
        base_rows = [_base(1, 100, 20200115)]
        baseDF = add_through_date_info(make_real_claim_df(spark, "opBase", base_rows))
        rev_rows = [_rev(1, 20200115, 455)]
        revDF = make_real_claim_df(spark, "opRevenue", rev_rows)
        summary = get_revenue_info(revDF, inClaim=True)
        out_df = get_stays(baseDF, summary, claimType="op")
        for col in ("ed", "mri", "ct", "icu"):
            assert col in out_df.columns


# ============================================================
# End-to-end pipeline helper for first-stay functions.
# Builds ipBase rows with ADMSN_DT + THRU_DT and runs the date/los
# pipeline that add_onDayOfFirstStay, add_onDayOfFirstStaySum, and
# add_first_stay_info consume.
# ============================================================

def _admsn_row(claimno, *, dsysrtky, admsn_dt, thru_dt):
    return {
        "DSYSRTKY": dsysrtky,
        "CLAIMNO": claimno,
        "ADMSN_DT": admsn_dt,
        "THRU_DT": thru_dt,
    }


def _run_first_stay_pipeline(spark, rows):
    from cms.utilities import add_through_date_info
    from cms.base import add_admission_date_info, add_los
    df = make_real_claim_df(spark, "ipBase", rows)
    df = add_admission_date_info(df, "ip")
    df = add_through_date_info(df)
    df = add_los(df)
    return df


# ============================================================
# Tests for add_onDayOfFirstStay
# ============================================================

class TestAddOnDayOfFirstStay:

    def test_marks_earliest_admission_day_per_beneficiary(self, spark):
        from cms.stays import add_onDayOfFirstStay
        # Beneficiary 1: earliest admission is 2020-01-15.
        # Beneficiary 2: earliest admission is 2020-03-01.
        rows = [
            _admsn_row(1, dsysrtky=1, admsn_dt=20200115, thru_dt=20200120),
            _admsn_row(2, dsysrtky=1, admsn_dt=20200401, thru_dt=20200405),
            _admsn_row(3, dsysrtky=2, admsn_dt=20200301, thru_dt=20200305),
            _admsn_row(4, dsysrtky=2, admsn_dt=20200601, thru_dt=20200605),
        ]
        df = _run_first_stay_pipeline(spark, rows)
        df = add_onDayOfFirstStay(df)
        by_claim = {r["CLAIMNO"]: r["onDayOfFirstStay"] for r in df.collect()}
        assert by_claim == {1: 1, 2: 0, 3: 1, 4: 0}

    def test_ties_on_same_day_both_marked_1(self, spark):
        # Two stays for the same beneficiary that admit on the same earliest day.
        from cms.stays import add_onDayOfFirstStay
        rows = [
            _admsn_row(1, dsysrtky=1, admsn_dt=20200115, thru_dt=20200115),
            _admsn_row(2, dsysrtky=1, admsn_dt=20200115, thru_dt=20200118),
            _admsn_row(3, dsysrtky=1, admsn_dt=20200301, thru_dt=20200305),
        ]
        df = _run_first_stay_pipeline(spark, rows)
        df = add_onDayOfFirstStay(df)
        by_claim = {r["CLAIMNO"]: r["onDayOfFirstStay"] for r in df.collect()}
        assert by_claim == {1: 1, 2: 1, 3: 0}

    def test_single_stay_is_marked_1(self, spark):
        from cms.stays import add_onDayOfFirstStay
        rows = [_admsn_row(1, dsysrtky=1, admsn_dt=20200115, thru_dt=20200115)]
        df = _run_first_stay_pipeline(spark, rows)
        df = add_onDayOfFirstStay(df).collect()
        assert df[0]["onDayOfFirstStay"] == 1


# ============================================================
# Tests for add_onDayOfFirstStaySum
# ============================================================

class TestAddOnDayOfFirstStaySum:

    def test_sums_per_beneficiary(self, spark):
        from cms.stays import add_onDayOfFirstStay, add_onDayOfFirstStaySum
        # Beneficiary 1: 2 stays admit on the same earliest day -> sum=2.
        # Beneficiary 2: 1 stay -> sum=1.
        rows = [
            _admsn_row(1, dsysrtky=1, admsn_dt=20200115, thru_dt=20200115),
            _admsn_row(2, dsysrtky=1, admsn_dt=20200115, thru_dt=20200118),
            _admsn_row(3, dsysrtky=1, admsn_dt=20200301, thru_dt=20200305),
            _admsn_row(4, dsysrtky=2, admsn_dt=20200401, thru_dt=20200401),
        ]
        df = _run_first_stay_pipeline(spark, rows)
        df = add_onDayOfFirstStay(df)
        df = add_onDayOfFirstStaySum(df)
        by_claim = {r["CLAIMNO"]: r["onDayOfFirstStaySum"] for r in df.collect()}
        # All beneficiary-1 rows see the same window sum.
        assert by_claim[1] == 2 and by_claim[2] == 2 and by_claim[3] == 2
        assert by_claim[4] == 1


# ============================================================
# Tests for add_first_stay_info
# ============================================================

class TestAddFirstStayInfo:

    def test_unambiguous_first_stay_flagged_1(self, spark):
        # Only one stay admits on the beneficiary's earliest day -> firstStay=1.
        from cms.stays import add_first_stay_info
        rows = [
            _admsn_row(1, dsysrtky=1, admsn_dt=20200115, thru_dt=20200120),
            _admsn_row(2, dsysrtky=1, admsn_dt=20200301, thru_dt=20200305),
        ]
        df = _run_first_stay_pipeline(spark, rows)
        df = add_first_stay_info(df)
        by_claim = {r["CLAIMNO"]: r["firstStay"] for r in df.collect()}
        assert by_claim == {1: 1, 2: 0}

    def test_tied_first_day_single_day_stay_resolves_to_1(self, spark):
        # When two stays admit on the same earliest day, the los=1 stay is
        # marked as firstStay=1 (under the rule that the patient was discharged
        # the same day and the second admission must therefore have come later).
        # Only one of the two tied stays has los=1, so singleDayStaySum==1.
        from cms.stays import add_first_stay_info
        rows = [
            _admsn_row(1, dsysrtky=1, admsn_dt=20200115, thru_dt=20200115),  # los=1
            _admsn_row(2, dsysrtky=1, admsn_dt=20200115, thru_dt=20200118),  # los=4
        ]
        df = _run_first_stay_pipeline(spark, rows)
        df = add_first_stay_info(df)
        by_claim = {r["CLAIMNO"]: r["firstStay"] for r in df.collect()}
        assert by_claim == {1: 1, 2: 0}

    def test_tied_first_day_no_single_day_stay_both_flagged_0(self, spark):
        # Both stays admit on the same earliest day, neither has los=1 -> ambiguous,
        # so firstStay falls through to .otherwise(0) for both.
        from cms.stays import add_first_stay_info
        rows = [
            _admsn_row(1, dsysrtky=1, admsn_dt=20200115, thru_dt=20200120),
            _admsn_row(2, dsysrtky=1, admsn_dt=20200115, thru_dt=20200125),
        ]
        df = _run_first_stay_pipeline(spark, rows)
        df = add_first_stay_info(df)
        for r in df.collect():
            assert r["firstStay"] == 0

    def test_tied_first_day_two_single_day_stays_both_flagged_0(self, spark):
        # Two los=1 stays on the same day -> singleDayStaySum==2, condition fails.
        from cms.stays import add_first_stay_info
        rows = [
            _admsn_row(1, dsysrtky=1, admsn_dt=20200115, thru_dt=20200115),
            _admsn_row(2, dsysrtky=1, admsn_dt=20200115, thru_dt=20200115),
        ]
        df = _run_first_stay_pipeline(spark, rows)
        df = add_first_stay_info(df)
        for r in df.collect():
            assert r["firstStay"] == 0

    def test_columns_added(self, spark):
        from cms.stays import add_first_stay_info
        rows = [_admsn_row(1, dsysrtky=1, admsn_dt=20200115, thru_dt=20200115)]
        df = _run_first_stay_pipeline(spark, rows)
        df = add_first_stay_info(df)
        for col in ("onDayOfFirstStay", "onDayOfFirstStaySum",
                    "singleDayStay", "singleDayStaySum", "firstStay"):
            assert col in df.columns


# ============================================================
# Tests for add_column_prior and add_orgnpinm_column_prior_year
#
# Window setup: Window.partitionBy(who).orderBy(when). For each (who) the
# lag picks up the previous row's column value; the F.when(when-prior==1)
# guard nulls out same-year and >1-year gaps. A final max-over-(who,when)
# broadcasts the prior value to every row in the current-year partition.
# ============================================================

def _prior_schema():
    return StructType([
        StructField("ORGNPINM", IntegerType(), True),
        StructField("THRU_DT_YEAR", IntegerType(), True),
        StructField("providerSepticShockAnnualVolume", IntegerType(), True),
    ])


def _make_prior_df(spark, rows):
    """rows: list of (ORGNPINM, THRU_DT_YEAR, providerSepticShockAnnualVolume)."""
    data = [{"ORGNPINM": o, "THRU_DT_YEAR": y, "providerSepticShockAnnualVolume": v}
            for o, y, v in rows]
    return spark.createDataFrame(data, schema=_prior_schema())


class TestAddColumnPrior:

    def test_adds_named_prior_column(self, spark):
        from cms.stays import add_column_prior
        df = _make_prior_df(spark, [(100, 2020, 5), (100, 2021, 10)])
        result = add_column_prior(df)
        assert "providerSepticShockAnnualVolumePrior" in result.columns
        assert "prior" not in result.columns  # scratch column is dropped before returning

    def test_contiguous_years_return_prior_value(self, spark):
        # 100/2020 has no predecessor -> null; 100/2021 -> 5; 100/2022 -> 10.
        from cms.stays import add_column_prior
        df = _make_prior_df(spark, [(100, 2020, 5), (100, 2021, 10), (100, 2022, 15)])
        out = {r["THRU_DT_YEAR"]: r["providerSepticShockAnnualVolumePrior"]
               for r in add_column_prior(df).collect()}
        assert out == {2020: None, 2021: 5, 2022: 10}

    def test_earliest_year_has_null_prior(self, spark):
        from cms.stays import add_column_prior
        df = _make_prior_df(spark, [(100, 2020, 5)])
        out = add_column_prior(df).collect()
        assert out[0]["providerSepticShockAnnualVolumePrior"] is None

    def test_year_gap_nulls_prior(self, spark):
        # 100 reports 2020 and 2022 but not 2021; the 2022 row should have null,
        # not the 2020 value (when - prior == 2, not 1).
        from cms.stays import add_column_prior
        df = _make_prior_df(spark, [(100, 2020, 5), (100, 2022, 15)])
        out = {r["THRU_DT_YEAR"]: r["providerSepticShockAnnualVolumePrior"]
               for r in add_column_prior(df).collect()}
        assert out == {2020: None, 2022: None}

    def test_multiple_rows_same_year_broadcast_to_all(self, spark):
        # When the input has multiple rows per (provider, year) -- as it does
        # when staysDF carries a window-aggregated provider column -- every row
        # in the current year should see the same prior value.
        from cms.stays import add_column_prior
        df = _make_prior_df(spark, [
            (100, 2020, 5), (100, 2020, 5),
            (100, 2021, 10), (100, 2021, 10), (100, 2021, 10),
        ])
        rows = add_column_prior(df).collect()
        priors_2020 = [r["providerSepticShockAnnualVolumePrior"] for r in rows if r["THRU_DT_YEAR"] == 2020]
        priors_2021 = [r["providerSepticShockAnnualVolumePrior"] for r in rows if r["THRU_DT_YEAR"] == 2021]
        assert priors_2020 == [None, None]
        assert priors_2021 == [5, 5, 5]

    def test_providers_are_independent(self, spark):
        # Two providers each with two contiguous years; lag must not bleed
        # across providers.
        from cms.stays import add_column_prior
        df = _make_prior_df(spark, [
            (100, 2020, 5), (100, 2021, 10),
            (200, 2020, 50), (200, 2021, 100),
        ])
        out = {(r["ORGNPINM"], r["THRU_DT_YEAR"]): r["providerSepticShockAnnualVolumePrior"]
               for r in add_column_prior(df).collect()}
        assert out == {
            (100, 2020): None, (100, 2021): 5,
            (200, 2020): None, (200, 2021): 50,
        }

    def test_custom_column_who_when(self, spark):
        from cms.stays import add_column_prior
        # Mirrors the transfers.py call: who=fromORGNPINM, when=fromTHRU_DT_YEAR.
        schema = StructType([
            StructField("fromORGNPINM", IntegerType(), True),
            StructField("fromTHRU_DT_YEAR", IntegerType(), True),
            StructField("nodeHhi", IntegerType(), True),
        ])
        df = spark.createDataFrame(
            [{"fromORGNPINM": 1, "fromTHRU_DT_YEAR": 2020, "nodeHhi": 50},
             {"fromORGNPINM": 1, "fromTHRU_DT_YEAR": 2021, "nodeHhi": 75}],
            schema=schema,
        )
        result = add_column_prior(df, column="nodeHhi",
                                  who="fromORGNPINM", when="fromTHRU_DT_YEAR")
        assert "nodeHhiPrior" in result.columns
        out = {r["fromTHRU_DT_YEAR"]: r["nodeHhiPrior"] for r in result.collect()}
        assert out == {2020: None, 2021: 50}

    def test_gapfill_zero_fills_year_gap_but_not_first_year(self, spark):
        # 100 reports 2020 and 2022 but not 2021. With gapFill=0 the 2022 prior
        # becomes 0 (2021 provably had no claims -> 0 volume), while the 2020 row
        # -- the provider's first observed year -- stays null (unobserved, not 0).
        from cms.stays import add_column_prior
        df = _make_prior_df(spark, [(100, 2020, 5), (100, 2022, 15)])
        out = {r["THRU_DT_YEAR"]: r["providerSepticShockAnnualVolumePrior"]
               for r in add_column_prior(df, gapFill=0).collect()}
        assert out == {2020: None, 2022: 0}

    def test_gapfill_none_preserves_null_on_gap(self, spark):
        # Passing gapFill=None explicitly matches the default: the gap stays null.
        from cms.stays import add_column_prior
        df = _make_prior_df(spark, [(100, 2020, 5), (100, 2022, 15)])
        out = {r["THRU_DT_YEAR"]: r["providerSepticShockAnnualVolumePrior"]
               for r in add_column_prior(df, gapFill=None).collect()}
        assert out == {2020: None, 2022: None}

    def test_gapfill_zero_does_not_override_contiguous_real_value(self, spark):
        # gapFill must only affect >1-year gaps. With contiguous years present,
        # the real prior value still wins -- even when that real value is 0 and
        # could be confused with the fill, and even across a later gap.
        # 100: 2020 -> null (first year), 2021 -> 5, 2022 -> 0 (real 2021->2022
        # is contiguous, prior=0 because 2021's value is 0), then a gap to 2024
        # whose prior is the gapFill 0.
        from cms.stays import add_column_prior
        df = _make_prior_df(spark, [
            (100, 2020, 5), (100, 2021, 0), (100, 2022, 9), (100, 2024, 7),
        ])
        out = {r["THRU_DT_YEAR"]: r["providerSepticShockAnnualVolumePrior"]
               for r in add_column_prior(df, gapFill=0).collect()}
        assert out == {2020: None, 2021: 5, 2022: 0, 2024: 0}

    def test_gapfill_zero_broadcasts_to_all_rows_of_gap_year(self, spark):
        # When the gap year has multiple rows (the usual stays-grain shape), the
        # filled 0 must reach every row, not just the lag-firing one.
        from cms.stays import add_column_prior
        df = _make_prior_df(spark, [
            (100, 2020, 5),
            (100, 2023, 15), (100, 2023, 15), (100, 2023, 15),
        ])
        priors_2023 = [r["providerSepticShockAnnualVolumePrior"]
                       for r in add_column_prior(df, gapFill=0).collect()
                       if r["THRU_DT_YEAR"] == 2023]
        assert priors_2023 == [0, 0, 0]

    def test_gapfill_zero_independent_per_provider(self, spark):
        # gapFill applies within each provider's own gap; providers don't bleed.
        from cms.stays import add_column_prior
        df = _make_prior_df(spark, [
            (100, 2020, 5), (100, 2022, 15),   # provider 100 has a gap
            (200, 2020, 50), (200, 2021, 100),  # provider 200 contiguous
        ])
        out = {(r["ORGNPINM"], r["THRU_DT_YEAR"]): r["providerSepticShockAnnualVolumePrior"]
               for r in add_column_prior(df, gapFill=0).collect()}
        assert out == {
            (100, 2020): None, (100, 2022): 0,
            (200, 2020): None, (200, 2021): 50,
        }


class TestAddOrgnpinmColumnPriorYear:

    def test_uses_orgnpinm_and_thru_dt_year(self, spark):
        # Delegates to add_column_prior with who=ORGNPINM, when=THRU_DT_YEAR.
        from cms.stays import add_orgnpinm_column_prior_year
        df = _make_prior_df(spark, [(100, 2020, 5), (100, 2021, 10), (100, 2022, 15)])
        out = {r["THRU_DT_YEAR"]: r["providerSepticShockAnnualVolumePrior"]
               for r in add_orgnpinm_column_prior_year(df).collect()}
        assert out == {2020: None, 2021: 5, 2022: 10}

    def test_custom_column_argument_renames_output(self, spark):
        from cms.stays import add_orgnpinm_column_prior_year
        schema = StructType([
            StructField("ORGNPINM", IntegerType(), True),
            StructField("THRU_DT_YEAR", IntegerType(), True),
            StructField("providerAnyStrokeAnnualVolume", IntegerType(), True),
        ])
        df = spark.createDataFrame(
            [{"ORGNPINM": 100, "THRU_DT_YEAR": 2020, "providerAnyStrokeAnnualVolume": 3},
             {"ORGNPINM": 100, "THRU_DT_YEAR": 2021, "providerAnyStrokeAnnualVolume": 7}],
            schema=schema,
        )
        result = add_orgnpinm_column_prior_year(df, column="providerAnyStrokeAnnualVolume")
        assert "providerAnyStrokeAnnualVolumePrior" in result.columns
        out = {r["THRU_DT_YEAR"]: r["providerAnyStrokeAnnualVolumePrior"] for r in result.collect()}
        assert out == {2020: None, 2021: 3}

    def test_gapfill_zero_on_stroke_vol_gap(self, spark):
        # providerAnyStrokeAnnualVolume is a count like providerSepticShockAnnualVolume: with gapFill=0
        # a >1-year gap records 0 (provider existed, no stroke claims that year),
        # the first observed year stays null.
        from cms.stays import add_orgnpinm_column_prior_year
        schema = StructType([
            StructField("ORGNPINM", IntegerType(), True),
            StructField("THRU_DT_YEAR", IntegerType(), True),
            StructField("providerAnyStrokeAnnualVolume", IntegerType(), True),
        ])
        df = spark.createDataFrame(
            [{"ORGNPINM": 100, "THRU_DT_YEAR": 2020, "providerAnyStrokeAnnualVolume": 3},
             {"ORGNPINM": 100, "THRU_DT_YEAR": 2022, "providerAnyStrokeAnnualVolume": 7}],
            schema=schema,
        )
        out = {r["THRU_DT_YEAR"]: r["providerAnyStrokeAnnualVolumePrior"]
               for r in add_orgnpinm_column_prior_year(df, column="providerAnyStrokeAnnualVolume",
                                                        gapFill=0).collect()}
        assert out == {2020: None, 2022: 0}

    def test_gapfill_zero_threads_through_to_volume_column(self, spark):
        # The wrapper forwards gapFill: a >1-year gap on a volume column records 0,
        # the first observed year stays null.
        from cms.stays import add_orgnpinm_column_prior_year
        df = _make_prior_df(spark, [(100, 2020, 5), (100, 2022, 15)])
        out = {r["THRU_DT_YEAR"]: r["providerSepticShockAnnualVolumePrior"]
               for r in add_orgnpinm_column_prior_year(df, column="providerSepticShockAnnualVolume",
                                                        gapFill=0).collect()}
        assert out == {2020: None, 2022: 0}

    def test_gapfill_defaults_to_none(self, spark):
        # Without gapFill the wrapper leaves gap years null (proportion/index-safe default).
        from cms.stays import add_orgnpinm_column_prior_year
        df = _make_prior_df(spark, [(100, 2020, 5), (100, 2022, 15)])
        out = {r["THRU_DT_YEAR"]: r["providerSepticShockAnnualVolumePrior"]
               for r in add_orgnpinm_column_prior_year(df, column="providerSepticShockAnnualVolume").collect()}
        assert out == {2020: None, 2022: None}


# ============================================================
# End-to-end pipeline helpers for add_source_and_destination_info.
#
# The function expects:
#   - staysDF post-get_unique_stays with DSYSRTKY, PROVIDER, ORGNPINM,
#     ADMSN_DT_DAY, THRU_DT_DAY.
#   - cmsDFS dict mapping "ipBase"/"snfBase"/"hhaBase"/"hospBase" to base DFs
#     with DSYSRTKY, PROVIDER, ORGNPINM, ADMSN_DT_DAY, THRU_DT_DAY, losDays.
#     cmsDFS["ipBase"] additionally needs rehabilitation and ltcHospital.
#
# Upstream chain per base DF:
#   make_real_claim_df -> add_admission_date_info -> add_through_date_info
#     -> add_losDays
#
# For ipBase we additionally simulate the NPI provider join by left-joining
# (CLAIMNO, rehabilitationFromTaxonomyPrimary, ltcHospital) and then running
# the real add_rehabilitation, which OR's the taxonomy flag with the
# CCN-derived rehabilitationFromCCN. This mirrors the MBSF death-join
# simulation pattern called out in the project's end-to-end testing
# convention.
# ============================================================

# Default non-rehab CCN: substring(3,4) = "0001" which is < 3025.
_NON_REHAB_CCN = "OH0001"
# CCN-derived rehab: substring(3,4) = "3050" which is in [3025, 3099].
_REHAB_CCN = "OH3050"


def _ip_src_dest_row(claimno, *, dsysrtky, orgnpinm, admsn_dt, thru_dt,
                     provider=_NON_REHAB_CCN, rehab_taxonomy=0, ltc=0):
    """ipBase-shaped row. _rehab_taxonomy/_ltc are stripped before schema
    load and injected post-load to simulate add_provider_npi_info."""
    return {
        "DSYSRTKY": dsysrtky, "CLAIMNO": claimno, "ORGNPINM": orgnpinm,
        "PROVIDER": provider, "ADMSN_DT": admsn_dt, "THRU_DT": thru_dt,
        "_rehab_taxonomy": rehab_taxonomy, "_ltc": ltc,
    }


def _snf_src_dest_row(claimno, *, dsysrtky, orgnpinm, admsn_dt, thru_dt,
                      provider=_NON_REHAB_CCN):
    return {
        "DSYSRTKY": dsysrtky, "CLAIMNO": claimno, "ORGNPINM": orgnpinm,
        "PROVIDER": provider, "ADMSN_DT": admsn_dt, "THRU_DT": thru_dt,
    }


def _hha_src_dest_row(claimno, *, dsysrtky, orgnpinm, hhstrtdt, thru_dt,
                      provider=_NON_REHAB_CCN):
    return {
        "DSYSRTKY": dsysrtky, "CLAIMNO": claimno, "ORGNPINM": orgnpinm,
        "PROVIDER": provider, "HHSTRTDT": hhstrtdt, "THRU_DT": thru_dt,
    }


def _hosp_src_dest_row(claimno, *, dsysrtky, orgnpinm, hspcstrt, thru_dt,
                       provider=_NON_REHAB_CCN):
    return {
        "DSYSRTKY": dsysrtky, "CLAIMNO": claimno, "ORGNPINM": orgnpinm,
        "PROVIDER": provider, "HSPCSTRT": hspcstrt, "THRU_DT": thru_dt,
    }


def _prep_ipBase(spark, rows):
    """Build ipBase upstream chain incl. simulated NPI join. Empty rows yield
    a schema-valid empty DF for cmsDFS slots that should have no IP claims."""
    from cms.utilities import add_through_date_info
    from cms.base import add_admission_date_info, add_losDays, add_rehabilitation
    schema_rows = [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]
    df = make_real_claim_df(spark, "ipBase", schema_rows)
    df = add_admission_date_info(df, "ip")
    df = add_through_date_info(df)
    df = add_losDays(df)
    if rows:
        npi_sim = spark.createDataFrame(
            [(r["CLAIMNO"], r["_rehab_taxonomy"], r["_ltc"]) for r in rows],
            "CLAIMNO int, rehabilitationFromTaxonomyPrimary int, ltcHospital int")
        df = df.join(npi_sim, on="CLAIMNO", how="left")
    else:
        df = (df.withColumn("rehabilitationFromTaxonomyPrimary", F.lit(0))
                .withColumn("ltcHospital", F.lit(0)))
    df = add_rehabilitation(df)
    return df


def _prep_snfBase(spark, rows):
    from cms.utilities import add_through_date_info
    from cms.base import add_admission_date_info, add_losDays
    df = make_real_claim_df(spark, "snfBase", rows)
    df = add_admission_date_info(df, "snf")
    df = add_through_date_info(df)
    df = add_losDays(df)
    return df


def _prep_hhaBase(spark, rows):
    from cms.utilities import add_through_date_info
    from cms.base import add_admission_date_info, add_losDays
    df = make_real_claim_df(spark, "hhaBase", rows)
    df = add_admission_date_info(df, "hha")
    df = add_through_date_info(df)
    df = add_losDays(df)
    return df


def _prep_hospBase(spark, rows):
    from cms.utilities import add_through_date_info
    from cms.base import add_admission_date_info, add_losDays
    df = make_real_claim_df(spark, "hospBase", rows)
    df = add_admission_date_info(df, "hosp")
    df = add_through_date_info(df)
    df = add_losDays(df)
    return df


def _build_cmsDFS(spark, ip_rows=None, snf_rows=None, hha_rows=None, hosp_rows=None):
    return {
        "ipBase":   _prep_ipBase(spark,   ip_rows   or []),
        "snfBase":  _prep_snfBase(spark,  snf_rows  or []),
        "hhaBase":  _prep_hhaBase(spark,  hha_rows  or []),
        "hospBase": _prep_hospBase(spark, hosp_rows or []),
    }


def _ip_staysDF_from(spark, ip_rows):
    """IP-cohort staysDF: prep ipBase + collapse via get_unique_stays."""
    from cms.stays import get_unique_stays
    return get_unique_stays(_prep_ipBase(spark, ip_rows), claimType="ip")


def _snf_staysDF_from(spark, snf_rows):
    """SNF-cohort staysDF for the claimType="snf" tests."""
    from cms.stays import get_unique_stays
    return get_unique_stays(_prep_snfBase(spark, snf_rows), claimType="snf")


def _op_src_dest_row(claimno, *, dsysrtky, orgnpinm, thru_dt, provider=_NON_REHAB_CCN):
    """opBase-shaped row. op has no admission date (the opBase schema has no
    ADMSN_DT), so an op visit is a single day: its through date."""
    return {
        "DSYSRTKY": dsysrtky, "CLAIMNO": claimno, "ORGNPINM": orgnpinm,
        "PROVIDER": provider, "THRU_DT": thru_dt,
    }


def _prep_opBase(spark, rows):
    """Build opBase upstream chain: no add_admission_date_info / add_losDays
    (op gets neither in add_preliminary_info), so THRU_DT_DAY is the only day."""
    from cms.utilities import add_through_date_info
    df = make_real_claim_df(spark, "opBase", rows)
    df = add_through_date_info(df)
    return df


def _op_staysDF_from(spark, op_rows):
    """OP-cohort staysDF: prep opBase + collapse via get_unique_stays(op)."""
    from cms.stays import get_unique_stays
    return get_unique_stays(_prep_opBase(spark, op_rows), claimType="op")


# ============================================================
# End-to-end tests for add_source_and_destination_info
# ============================================================

class TestAddSourceAndDestinationInfo:

    # ---- A. Output schema ----

    def test_output_columns_added(self, spark):
        from cms.stays import add_source_and_destination_info
        ip_rows = [_ip_src_dest_row(1, dsysrtky=1, orgnpinm=100,
                                    admsn_dt=20200110, thru_dt=20200115)]
        staysDF = _ip_staysDF_from(spark, ip_rows)
        cmsDFS = _build_cmsDFS(spark, ip_rows=ip_rows)
        out = add_source_and_destination_info(staysDF, cmsDFS)
        for col in ("admissionSource", "thruDestination",
                    "otherStaysOnAdmission", "otherStaysOnThru"):
            assert col in out.columns
        for col in ("DSYSRTKY", "CLAIMNO", "ADMSN_DT_DAY", "THRU_DT_DAY"):
            assert col in out.columns

    # ---- B / C. Home fallback + self-exclusion regression ----

    def test_self_only_returns_home_for_both(self, spark):
        # Primary regression test for the self-inclusion bug the refactor
        # fixed. Without the not_self lambda the row's own losDays contains
        # ADMSN_DT_DAY and THRU_DT_DAY, and both columns would resolve to
        # "ipOther" instead of "home".
        from cms.stays import add_source_and_destination_info
        ip_rows = [_ip_src_dest_row(1, dsysrtky=1, orgnpinm=100,
                                    admsn_dt=20200110, thru_dt=20200115)]
        staysDF = _ip_staysDF_from(spark, ip_rows)
        cmsDFS = _build_cmsDFS(spark, ip_rows=ip_rows)
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert len(out) == 1
        assert out[0]["admissionSource"] == "home"
        assert out[0]["thruDestination"] == "home"

    def test_beneficiary_absent_from_cmsDFS_returns_home(self, spark):
        # Exercises the .isNull() branch of _resolve_setting: the
        # beneficiary in staysDF has no rows in any cmsDFS entry, so the
        # left-outer join leaves otherStays null.
        from cms.stays import add_source_and_destination_info
        stays_rows = [_ip_src_dest_row(1, dsysrtky=1, orgnpinm=100,
                                       admsn_dt=20200110, thru_dt=20200115)]
        staysDF = _ip_staysDF_from(spark, stays_rows)
        other_rows = [_ip_src_dest_row(99, dsysrtky=999, orgnpinm=100,
                                       admsn_dt=20200110, thru_dt=20200115)]
        cmsDFS = _build_cmsDFS(spark, ip_rows=other_rows)
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert len(out) == 1
        assert out[0]["admissionSource"] == "home"
        assert out[0]["thruDestination"] == "home"

    def test_split_claim_stay_self_excludes(self, spark):
        # Two interim IP claims for the same stay (same stay key) get
        # collapsed into a single stay entry by _per_stay_index, and the
        # not_self comparison still drops it. Result: "home" because there
        # are no other-claim entries.
        from cms.stays import add_source_and_destination_info
        ip_rows = [
            _ip_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                             admsn_dt=20200110, thru_dt=20200115),
            _ip_src_dest_row(11, dsysrtky=1, orgnpinm=100,
                             admsn_dt=20200110, thru_dt=20200120),
        ]
        staysDF = _ip_staysDF_from(spark, ip_rows)
        cmsDFS = _build_cmsDFS(spark, ip_rows=ip_rows)
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert len(out) == 1
        assert out[0]["admissionSource"] == "home"
        assert out[0]["thruDestination"] == "home"

    # ---- D. Source identification (admissionSource) ----

    def test_admission_source_snf(self, spark):
        from cms.stays import add_source_and_destination_info
        cur = _ip_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                               admsn_dt=20200115, thru_dt=20200120)
        snf = _snf_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                                admsn_dt=20200110, thru_dt=20200115)
        staysDF = _ip_staysDF_from(spark, [cur])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[cur], snf_rows=[snf])
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert out[0]["admissionSource"] == "snf"

    def test_admission_source_hosp(self, spark):
        # "hosp" = hospice in this codebase (add_admission_date_info copies
        # HSPCSTRT to ADMSN_DT when claimType="hosp").
        from cms.stays import add_source_and_destination_info
        cur = _ip_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                               admsn_dt=20200115, thru_dt=20200120)
        hosp = _hosp_src_dest_row(30, dsysrtky=1, orgnpinm=300,
                                  hspcstrt=20200110, thru_dt=20200115)
        staysDF = _ip_staysDF_from(spark, [cur])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[cur], hosp_rows=[hosp])
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert out[0]["admissionSource"] == "hosp"

    def test_admission_source_hha(self, spark):
        from cms.stays import add_source_and_destination_info
        cur = _ip_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                               admsn_dt=20200115, thru_dt=20200120)
        hha = _hha_src_dest_row(40, dsysrtky=1, orgnpinm=400,
                                hhstrtdt=20200110, thru_dt=20200115)
        staysDF = _ip_staysDF_from(spark, [cur])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[cur], hha_rows=[hha])
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert out[0]["admissionSource"] == "hha"

    def test_admission_source_ipRehab(self, spark):
        # Prior IP stay with rehabilitation=1 (CCN-derived: PROVIDER starts
        # with two letters then "3050", landing in [3025, 3099]).
        from cms.stays import add_source_and_destination_info
        cur = _ip_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                               admsn_dt=20200115, thru_dt=20200120)
        rehab = _ip_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                                 admsn_dt=20200110, thru_dt=20200115,
                                 provider=_REHAB_CCN)
        staysDF = _ip_staysDF_from(spark, [cur])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[cur, rehab])
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert out[0]["admissionSource"] == "ipRehab"

    def test_admission_source_ipLtc(self, spark):
        # Prior IP stay with ltcHospital=1 (simulated NPI-join flag).
        from cms.stays import add_source_and_destination_info
        cur = _ip_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                               admsn_dt=20200115, thru_dt=20200120)
        ltc = _ip_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                               admsn_dt=20200110, thru_dt=20200115, ltc=1)
        staysDF = _ip_staysDF_from(spark, [cur])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[cur, ltc])
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert out[0]["admissionSource"] == "ipLtc"

    def test_admission_source_ipOther(self, spark):
        # Prior IP stay with neither flag, different ORGNPINM so the stay key
        # doesn't collide with the current stay's.
        from cms.stays import add_source_and_destination_info
        cur = _ip_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                               admsn_dt=20200115, thru_dt=20200120)
        other = _ip_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                                 admsn_dt=20200110, thru_dt=20200115)
        staysDF = _ip_staysDF_from(spark, [cur])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[cur, other])
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert out[0]["admissionSource"] == "ipOther"

    # ---- E. Destination identification (thruDestination) ----

    def test_thru_destination_snf(self, spark):
        from cms.stays import add_source_and_destination_info
        cur = _ip_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                               admsn_dt=20200110, thru_dt=20200120)
        snf = _snf_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                                admsn_dt=20200120, thru_dt=20200125)
        staysDF = _ip_staysDF_from(spark, [cur])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[cur], snf_rows=[snf])
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert out[0]["thruDestination"] == "snf"

    def test_thru_destination_hha(self, spark):
        from cms.stays import add_source_and_destination_info
        cur = _ip_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                               admsn_dt=20200110, thru_dt=20200120)
        hha = _hha_src_dest_row(40, dsysrtky=1, orgnpinm=400,
                                hhstrtdt=20200120, thru_dt=20200125)
        staysDF = _ip_staysDF_from(spark, [cur])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[cur], hha_rows=[hha])
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert out[0]["thruDestination"] == "hha"

    def test_thru_destination_hosp(self, spark):
        from cms.stays import add_source_and_destination_info
        cur = _ip_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                               admsn_dt=20200110, thru_dt=20200120)
        hosp = _hosp_src_dest_row(30, dsysrtky=1, orgnpinm=300,
                                  hspcstrt=20200120, thru_dt=20200125)
        staysDF = _ip_staysDF_from(spark, [cur])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[cur], hosp_rows=[hosp])
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert out[0]["thruDestination"] == "hosp"

    def test_thru_destination_ipRehab(self, spark):
        from cms.stays import add_source_and_destination_info
        cur = _ip_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                               admsn_dt=20200110, thru_dt=20200120)
        rehab = _ip_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                                 admsn_dt=20200120, thru_dt=20200125,
                                 provider=_REHAB_CCN)
        staysDF = _ip_staysDF_from(spark, [cur])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[cur, rehab])
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert out[0]["thruDestination"] == "ipRehab"

    def test_thru_destination_ipLtc(self, spark):
        from cms.stays import add_source_and_destination_info
        cur = _ip_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                               admsn_dt=20200110, thru_dt=20200120)
        ltc = _ip_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                               admsn_dt=20200120, thru_dt=20200125, ltc=1)
        staysDF = _ip_staysDF_from(spark, [cur])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[cur, ltc])
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert out[0]["thruDestination"] == "ipLtc"

    def test_thru_destination_ipOther(self, spark):
        from cms.stays import add_source_and_destination_info
        cur = _ip_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                               admsn_dt=20200110, thru_dt=20200120)
        other = _ip_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                                 admsn_dt=20200120, thru_dt=20200125)
        staysDF = _ip_staysDF_from(spark, [cur])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[cur, other])
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert out[0]["thruDestination"] == "ipOther"

    # ---- F. Priority cascade (adjacent pairs in _SOURCE_DEST_PRIORITY) ----

    def test_priority_hosp_beats_ipRehab(self, spark):
        from cms.stays import add_source_and_destination_info
        cur = _ip_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                               admsn_dt=20200115, thru_dt=20200120)
        hosp = _hosp_src_dest_row(30, dsysrtky=1, orgnpinm=300,
                                  hspcstrt=20200110, thru_dt=20200115)
        rehab = _ip_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                                 admsn_dt=20200110, thru_dt=20200115,
                                 provider=_REHAB_CCN)
        staysDF = _ip_staysDF_from(spark, [cur])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[cur, rehab], hosp_rows=[hosp])
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert out[0]["admissionSource"] == "hosp"

    def test_priority_ipRehab_beats_snf(self, spark):
        from cms.stays import add_source_and_destination_info
        cur = _ip_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                               admsn_dt=20200115, thru_dt=20200120)
        rehab = _ip_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                                 admsn_dt=20200110, thru_dt=20200115,
                                 provider=_REHAB_CCN)
        snf = _snf_src_dest_row(30, dsysrtky=1, orgnpinm=300,
                                admsn_dt=20200110, thru_dt=20200115)
        staysDF = _ip_staysDF_from(spark, [cur])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[cur, rehab], snf_rows=[snf])
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert out[0]["admissionSource"] == "ipRehab"

    def test_priority_snf_beats_ipLtc(self, spark):
        from cms.stays import add_source_and_destination_info
        cur = _ip_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                               admsn_dt=20200115, thru_dt=20200120)
        snf = _snf_src_dest_row(30, dsysrtky=1, orgnpinm=300,
                                admsn_dt=20200110, thru_dt=20200115)
        ltc = _ip_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                               admsn_dt=20200110, thru_dt=20200115, ltc=1)
        staysDF = _ip_staysDF_from(spark, [cur])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[cur, ltc], snf_rows=[snf])
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert out[0]["admissionSource"] == "snf"

    def test_priority_ipLtc_beats_ipOther(self, spark):
        from cms.stays import add_source_and_destination_info
        cur = _ip_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                               admsn_dt=20200115, thru_dt=20200120)
        ltc = _ip_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                               admsn_dt=20200110, thru_dt=20200115, ltc=1)
        other = _ip_src_dest_row(30, dsysrtky=1, orgnpinm=300,
                                 admsn_dt=20200110, thru_dt=20200115)
        staysDF = _ip_staysDF_from(spark, [cur])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[cur, ltc, other])
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert out[0]["admissionSource"] == "ipLtc"

    def test_priority_ipOther_beats_hha(self, spark):
        from cms.stays import add_source_and_destination_info
        cur = _ip_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                               admsn_dt=20200115, thru_dt=20200120)
        other = _ip_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                                 admsn_dt=20200110, thru_dt=20200115)
        hha = _hha_src_dest_row(40, dsysrtky=1, orgnpinm=400,
                                hhstrtdt=20200110, thru_dt=20200115)
        staysDF = _ip_staysDF_from(spark, [cur])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[cur, other], hha_rows=[hha])
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert out[0]["admissionSource"] == "ipOther"

    # ---- G. IP sub-bucketing edge case (both flags) ----

    def test_both_rehab_and_ltc_flags_resolves_to_ipRehab(self, spark):
        # An ipBase row with both rehab=1 and ltc=1 contributes to BOTH
        # ipRehab and ipLtc buckets. Under priority (ipRehab > ipLtc),
        # ipRehab wins. Pinning this behaviour: if the bucket filters are
        # made mutually exclusive later, update this test.
        from cms.stays import add_source_and_destination_info
        cur = _ip_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                               admsn_dt=20200115, thru_dt=20200120)
        both = _ip_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                                admsn_dt=20200110, thru_dt=20200115,
                                provider=_REHAB_CCN, ltc=1)
        staysDF = _ip_staysDF_from(spark, [cur])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[cur, both])
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert out[0]["admissionSource"] == "ipRehab"

    # ---- H. Cross-beneficiary independence ----

    def test_beneficiaries_independent(self, spark):
        # A's prior SNF must not affect B (no claims) and vice versa.
        from cms.stays import add_source_and_destination_info
        a_cur = _ip_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                                 admsn_dt=20200115, thru_dt=20200120)
        a_snf = _snf_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                                  admsn_dt=20200110, thru_dt=20200115)
        b_cur = _ip_src_dest_row(11, dsysrtky=2, orgnpinm=100,
                                 admsn_dt=20200201, thru_dt=20200205)
        staysDF = _ip_staysDF_from(spark, [a_cur, b_cur])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[a_cur, b_cur], snf_rows=[a_snf])
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        by_dsys = {r["DSYSRTKY"]: r for r in out}
        assert by_dsys[1]["admissionSource"] == "snf"
        assert by_dsys[2]["admissionSource"] == "home"

    # ---- I. claimType kwarg ----

    def test_claimType_snf_self_excludes_snf_stay(self, spark):
        # With claimType="snf", _stay_keys uses THRU_DT_DAY, so the current
        # SNF stay's struct matches the snf bucket entry and self-exclusion
        # fires correctly. Result: "home".
        from cms.stays import add_source_and_destination_info
        snf_rows = [_snf_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                                      admsn_dt=20200110, thru_dt=20200115)]
        staysDF = _snf_staysDF_from(spark, snf_rows)
        cmsDFS = _build_cmsDFS(spark, snf_rows=snf_rows)
        out = add_source_and_destination_info(staysDF, cmsDFS,
                                              claimType="snf").collect()
        assert len(out) == 1
        assert out[0]["admissionSource"] == "home"

    def test_claimType_mismatch_breaks_self_exclusion(self, spark):
        # Same SNF cohort, but the caller forgets the kwarg and the function
        # uses claimType="ip" defaults. The stay-key struct mismatch (current
        # keyed on ADMSN_DT_DAY, snf entry keyed on THRU_DT_DAY) means
        # not_self does NOT fire on the row's own entry: the SNF stay "finds
        # itself" via the snf bucket and admissionSource = "snf". Documents
        # that claimType is load-bearing.
        from cms.stays import add_source_and_destination_info
        snf_rows = [_snf_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                                      admsn_dt=20200110, thru_dt=20200115)]
        staysDF = _snf_staysDF_from(spark, snf_rows)
        cmsDFS = _build_cmsDFS(spark, snf_rows=snf_rows)
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert out[0]["admissionSource"] == "snf"

    # ---- J. Stay-level granularity (no double-counting split claims) ----

    def test_split_other_stay_not_double_counted(self, spark):
        # A beneficiary has 2 interim IP claims for the SAME prior stay plus
        # 1 IP claim for the CURRENT stay (different ADMSN_DT_DAY + different
        # ORGNPINM). _per_stay_index should collapse the two interim claims
        # into a single stay entry, so otherStaysOnAdmission has size 1, not 2.
        from cms.stays import add_source_and_destination_info
        cur = _ip_src_dest_row(10, dsysrtky=1, orgnpinm=100,
                               admsn_dt=20200115, thru_dt=20200120)
        prior_a = _ip_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                                   admsn_dt=20200110, thru_dt=20200113)
        prior_b = _ip_src_dest_row(21, dsysrtky=1, orgnpinm=200,
                                   admsn_dt=20200110, thru_dt=20200115)
        staysDF = _ip_staysDF_from(spark, [cur])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[cur, prior_a, prior_b])
        out = add_source_and_destination_info(staysDF, cmsDFS).collect()
        assert out[0]["admissionSource"] == "ipOther"
        assert len(out[0]["otherStaysOnAdmission"]) == 1

    # ---- K. op claimType: single-day visit, +/-1 day AND windows ----
    #
    # An op ED visit is a single day (its through date, D). With claimType="op":
    #   source  (admissionSource): an other stay counts iff its losDays spans BOTH D-1 and D
    #   dest    (thruDestination): an other stay counts iff its losDays spans BOTH D and D+1
    # These dates are consecutive calendar days, which map to consecutive THRU_DT_DAY
    # indices, so 20200114 / 20200115 / 20200116 are D-1 / D / D+1. op is never in the
    # otherStays index, so self-exclusion is moot; the op stay is only ever the probe.

    def test_op_admission_source_snf(self, spark):
        # SNF spanning 20200110-20200115 covers both D-1 and D -> source=snf.
        # It does NOT cover D+1, so it is not a destination -> home.
        from cms.stays import add_source_and_destination_info
        op = _op_src_dest_row(10, dsysrtky=1, orgnpinm=100, thru_dt=20200115)
        snf = _snf_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                                admsn_dt=20200110, thru_dt=20200115)
        staysDF = _op_staysDF_from(spark, [op])
        cmsDFS = _build_cmsDFS(spark, snf_rows=[snf])
        out = add_source_and_destination_info(staysDF, cmsDFS,
                                              claimType="op").collect()
        assert len(out) == 1
        assert out[0]["admissionSource"] == "snf"
        assert out[0]["thruDestination"] == "home"

    def test_op_source_excludes_stay_ending_day_before(self, spark):
        # SNF ending on D-1 (20200114) covers D-1 but not D, so it fails the
        # "both D-1 AND D" source rule -> home. This is the key difference from
        # a plain overlap: a stay the patient left the day before is not a source.
        from cms.stays import add_source_and_destination_info
        op = _op_src_dest_row(10, dsysrtky=1, orgnpinm=100, thru_dt=20200115)
        snf = _snf_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                                admsn_dt=20200110, thru_dt=20200114)
        staysDF = _op_staysDF_from(spark, [op])
        cmsDFS = _build_cmsDFS(spark, snf_rows=[snf])
        out = add_source_and_destination_info(staysDF, cmsDFS,
                                              claimType="op").collect()
        assert out[0]["admissionSource"] == "home"
        assert out[0]["thruDestination"] == "home"

    def test_op_thru_destination_snf(self, spark):
        # SNF spanning 20200115-20200120 covers both D and D+1 -> dest=snf.
        # It does NOT cover D-1, so it is not a source -> home.
        from cms.stays import add_source_and_destination_info
        op = _op_src_dest_row(10, dsysrtky=1, orgnpinm=100, thru_dt=20200115)
        snf = _snf_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                                admsn_dt=20200115, thru_dt=20200120)
        staysDF = _op_staysDF_from(spark, [op])
        cmsDFS = _build_cmsDFS(spark, snf_rows=[snf])
        out = add_source_and_destination_info(staysDF, cmsDFS,
                                              claimType="op").collect()
        assert out[0]["thruDestination"] == "snf"
        assert out[0]["admissionSource"] == "home"

    def test_op_destination_excludes_stay_admitting_day_after(self, spark):
        # An ip stay admitting on D+1 (20200116) covers D+1 but not D, so it
        # fails the "both D AND D+1" destination rule -> home. Mirror of the
        # source boundary exclusion.
        from cms.stays import add_source_and_destination_info
        op = _op_src_dest_row(10, dsysrtky=1, orgnpinm=100, thru_dt=20200115)
        ip = _ip_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                              admsn_dt=20200116, thru_dt=20200120)
        staysDF = _op_staysDF_from(spark, [op])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[ip])
        out = add_source_and_destination_info(staysDF, cmsDFS,
                                              claimType="op").collect()
        assert out[0]["thruDestination"] == "home"
        assert out[0]["admissionSource"] == "home"

    def test_op_source_and_destination_differ(self, spark):
        # The two windows look in opposite directions, so they can resolve to
        # different settings: a SNF the patient came from (spans into D) and an
        # ip stay the patient went to (spans out of D).
        from cms.stays import add_source_and_destination_info
        op = _op_src_dest_row(10, dsysrtky=1, orgnpinm=100, thru_dt=20200115)
        snf = _snf_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                                admsn_dt=20200110, thru_dt=20200115)   # D-1 and D
        ip = _ip_src_dest_row(30, dsysrtky=1, orgnpinm=300,
                              admsn_dt=20200115, thru_dt=20200118)      # D and D+1
        staysDF = _op_staysDF_from(spark, [op])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[ip], snf_rows=[snf])
        out = add_source_and_destination_info(staysDF, cmsDFS,
                                              claimType="op").collect()
        assert out[0]["admissionSource"] == "snf"
        assert out[0]["thruDestination"] == "ipOther"

    def test_op_stay_spanning_both_boundaries_is_source_and_destination(self, spark):
        # A hospice stay spanning 20200110-20200120 covers D-1, D, and D+1, so
        # it satisfies both windows -> source=hosp AND dest=hosp.
        from cms.stays import add_source_and_destination_info
        op = _op_src_dest_row(10, dsysrtky=1, orgnpinm=100, thru_dt=20200115)
        hosp = _hosp_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                                  hspcstrt=20200110, thru_dt=20200120)
        staysDF = _op_staysDF_from(spark, [op])
        cmsDFS = _build_cmsDFS(spark, hosp_rows=[hosp])
        out = add_source_and_destination_info(staysDF, cmsDFS,
                                              claimType="op").collect()
        assert out[0]["admissionSource"] == "hosp"
        assert out[0]["thruDestination"] == "hosp"

    def test_op_no_overlap_returns_home(self, spark):
        # A beneficiary with a stay nowhere near the ED day -> home on both.
        from cms.stays import add_source_and_destination_info
        op = _op_src_dest_row(10, dsysrtky=1, orgnpinm=100, thru_dt=20200115)
        snf = _snf_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                                admsn_dt=20200201, thru_dt=20200205)
        staysDF = _op_staysDF_from(spark, [op])
        cmsDFS = _build_cmsDFS(spark, snf_rows=[snf])
        out = add_source_and_destination_info(staysDF, cmsDFS,
                                              claimType="op").collect()
        assert out[0]["admissionSource"] == "home"
        assert out[0]["thruDestination"] == "home"

    def test_op_destination_priority_hosp_over_ipOther(self, spark):
        # Two settings both satisfy the destination window {D, D+1}: an ipOther
        # stay and a hospice stay. _SOURCE_DEST_PRIORITY puts hosp above ipOther,
        # so dest=hosp. Neither covers D-1, so source=home.
        from cms.stays import add_source_and_destination_info
        op = _op_src_dest_row(10, dsysrtky=1, orgnpinm=100, thru_dt=20200115)
        ip = _ip_src_dest_row(20, dsysrtky=1, orgnpinm=200,
                              admsn_dt=20200115, thru_dt=20200116)      # D and D+1
        hosp = _hosp_src_dest_row(30, dsysrtky=1, orgnpinm=300,
                                  hspcstrt=20200115, thru_dt=20200117)  # D and D+1
        staysDF = _op_staysDF_from(spark, [op])
        cmsDFS = _build_cmsDFS(spark, ip_rows=[ip], hosp_rows=[hosp])
        out = add_source_and_destination_info(staysDF, cmsDFS,
                                              claimType="op").collect()
        assert out[0]["thruDestination"] == "hosp"
        assert out[0]["admissionSource"] == "home"
