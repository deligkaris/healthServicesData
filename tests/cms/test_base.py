import pytest
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType


# ============================================================
# Helper to build minimal DataFrames for add_septicShockPoa
# ============================================================

def _septic_shock_poa_schema():
    return StructType([
        StructField("dgnsPoaCodeAll", ArrayType(StringType()), True),
    ])


def make_septic_shock_poa_df(spark, codes_lists):
    """Create a DataFrame with a single dgnsPoaCodeAll column.

    codes_lists: list of lists of strings (one entry per row).
    """
    rows = [{"dgnsPoaCodeAll": codes} for codes in codes_lists]
    return spark.createDataFrame(rows, schema=_septic_shock_poa_schema())


# ============================================================
# Helper to build DataFrames with CLM_POA_IND_SW1..25 columns
# ============================================================

def _poa_code_schema():
    return StructType([
        StructField(f"CLM_POA_IND_SW{i}", StringType(), True) for i in range(1, 26)
    ])


def make_poa_df(spark, rows_codes):
    """Create a DataFrame with 25 CLM_POA_IND_SW columns.

    rows_codes: list of lists; each inner list has up to 25 POA values
    (shorter lists are padded with None to length 25).
    """
    rows = []
    for codes in rows_codes:
        padded = list(codes) + [None] * (25 - len(codes))
        rows.append({f"CLM_POA_IND_SW{i+1}": padded[i] for i in range(25)})
    return spark.createDataFrame(rows, schema=_poa_code_schema())


# ============================================================
# Tests for add_septicShockPoa
# ============================================================

class TestAddSepticShockPoa:

    def test_r6521_present_returns_1(self, spark):
        from cms.base import add_septicShockPoa
        df = make_septic_shock_poa_df(spark, [["R6521"]])
        result = add_septicShockPoa(df).collect()[0]
        assert result["septicShockPoa"] == 1

    def test_r6521_with_other_codes_returns_1(self, spark):
        from cms.base import add_septicShockPoa
        df = make_septic_shock_poa_df(spark, [["I10", "R6521", "E119"]])
        result = add_septicShockPoa(df).collect()[0]
        assert result["septicShockPoa"] == 1

    def test_no_r6521_returns_0(self, spark):
        from cms.base import add_septicShockPoa
        df = make_septic_shock_poa_df(spark, [["I10", "E119", "J189"]])
        result = add_septicShockPoa(df).collect()[0]
        assert result["septicShockPoa"] == 0

    def test_empty_array_returns_0(self, spark):
        from cms.base import add_septicShockPoa
        df = make_septic_shock_poa_df(spark, [[]])
        result = add_septicShockPoa(df).collect()[0]
        assert result["septicShockPoa"] == 0

    def test_similar_but_different_code_returns_0(self, spark):
        # R65.20 (severe sepsis without septic shock) is NOT R6521 (with septic shock)
        from cms.base import add_septicShockPoa
        df = make_septic_shock_poa_df(spark, [["R6520", "R65"]])
        result = add_septicShockPoa(df).collect()[0]
        assert result["septicShockPoa"] == 0

    def test_multiple_rows_mixed(self, spark):
        from cms.base import add_septicShockPoa
        df = make_septic_shock_poa_df(spark, [
            ["R6521"],
            ["I10"],
            ["A419", "R6521"],
            [],
        ])
        results = [r["septicShockPoa"] for r in add_septicShockPoa(df).collect()]
        assert results == [1, 0, 1, 0]


# ============================================================
# Tests for add_poaCodeAll
# ============================================================

class TestAddPoaCodeAll:

    def test_column_added(self, spark):
        from cms.base import add_poaCodeAll
        df = make_poa_df(spark, [["Y"] * 25])
        result_df = add_poaCodeAll(df)
        assert "poaCodeAll" in result_df.columns

    def test_array_length_is_25(self, spark):
        from cms.base import add_poaCodeAll
        df = make_poa_df(spark, [["Y"] * 25])
        result = add_poaCodeAll(df).collect()[0]
        assert len(result["poaCodeAll"]) == 25

    def test_preserves_order(self, spark):
        from cms.base import add_poaCodeAll
        codes = [str(i % 10) for i in range(1, 26)]  # distinct, ordered values
        df = make_poa_df(spark, [codes])
        result = add_poaCodeAll(df).collect()[0]
        assert result["poaCodeAll"] == codes

    def test_mixed_flags(self, spark):
        from cms.base import add_poaCodeAll
        codes = ["Y", "N", "U", "W", "1"] + [None] * 20
        df = make_poa_df(spark, [codes])
        result = add_poaCodeAll(df).collect()[0]
        assert result["poaCodeAll"][:5] == ["Y", "N", "U", "W", "1"]
        assert all(v is None for v in result["poaCodeAll"][5:])

    def test_all_nulls(self, spark):
        from cms.base import add_poaCodeAll
        df = make_poa_df(spark, [[]])  # all 25 columns are None
        result = add_poaCodeAll(df).collect()[0]
        assert result["poaCodeAll"] == [None] * 25

    def test_does_not_drop_input_columns(self, spark):
        from cms.base import add_poaCodeAll
        df = make_poa_df(spark, [["Y"] * 25])
        result_df = add_poaCodeAll(df)
        for i in range(1, 26):
            assert f"CLM_POA_IND_SW{i}" in result_df.columns

    def test_multiple_rows(self, spark):
        from cms.base import add_poaCodeAll
        df = make_poa_df(spark, [
            ["Y"] * 25,
            ["N"] * 25,
            ["Y", "N", "U"] + [None] * 22,
        ])
        rows = add_poaCodeAll(df).collect()
        assert rows[0]["poaCodeAll"] == ["Y"] * 25
        assert rows[1]["poaCodeAll"] == ["N"] * 25
        assert rows[2]["poaCodeAll"][:3] == ["Y", "N", "U"]
        assert all(v is None for v in rows[2]["poaCodeAll"][3:])


# ============================================================
# Helper to build DataFrames with dgnsCodeAll + poaCodeAll
# ============================================================

def _dgns_poa_schema():
    return StructType([
        StructField("dgnsCodeAll", ArrayType(StringType()), True),
        StructField("poaCodeAll", ArrayType(StringType()), True),
    ])


def make_dgns_poa_df(spark, rows):
    """Create a DataFrame with dgnsCodeAll and poaCodeAll array columns.

    rows: list of (dgns_codes, poa_flags) tuples.
    """
    data = [{"dgnsCodeAll": dgns, "poaCodeAll": poa} for dgns, poa in rows]
    return spark.createDataFrame(data, schema=_dgns_poa_schema())


# ============================================================
# Tests for add_dgnsPoaCodeAll
# ============================================================

class TestAddDgnsPoaCodeAll:

    def test_all_y_keeps_all_codes(self, spark):
        from cms.base import add_dgnsPoaCodeAll
        df = make_dgns_poa_df(spark, [(["I10", "E119", "J189"], ["Y", "Y", "Y"])])
        result = add_dgnsPoaCodeAll(df).collect()[0]
        assert result["dgnsPoaCodeAll"] == ["I10", "E119", "J189"]

    def test_all_n_returns_empty(self, spark):
        from cms.base import add_dgnsPoaCodeAll
        df = make_dgns_poa_df(spark, [(["I10", "E119", "J189"], ["N", "N", "N"])])
        result = add_dgnsPoaCodeAll(df).collect()[0]
        assert result["dgnsPoaCodeAll"] == []

    def test_mixed_keeps_only_y(self, spark):
        from cms.base import add_dgnsPoaCodeAll
        df = make_dgns_poa_df(spark, [
            (["I10", "E119", "J189", "R6521"], ["Y", "N", "Y", "N"])
        ])
        result = add_dgnsPoaCodeAll(df).collect()[0]
        assert result["dgnsPoaCodeAll"] == ["I10", "J189"]

    def test_non_y_flags_excluded(self, spark):
        # Only 'Y' should pass the filter; U, W, 1, N, None should be excluded.
        from cms.base import add_dgnsPoaCodeAll
        df = make_dgns_poa_df(spark, [
            (["A", "B", "C", "D", "E", "F"], ["Y", "N", "U", "W", "1", None])
        ])
        result = add_dgnsPoaCodeAll(df).collect()[0]
        assert result["dgnsPoaCodeAll"] == ["A"]

    def test_empty_arrays(self, spark):
        from cms.base import add_dgnsPoaCodeAll
        df = make_dgns_poa_df(spark, [([], [])])
        result = add_dgnsPoaCodeAll(df).collect()[0]
        assert result["dgnsPoaCodeAll"] == []

    def test_preserves_order(self, spark):
        from cms.base import add_dgnsPoaCodeAll
        df = make_dgns_poa_df(spark, [
            (["Z01", "A41", "I63", "R6521", "G20"],
             ["Y",   "N",   "Y",   "Y",      "N"])
        ])
        result = add_dgnsPoaCodeAll(df).collect()[0]
        assert result["dgnsPoaCodeAll"] == ["Z01", "I63", "R6521"]

    def test_drops_intermediate_struct_column(self, spark):
        from cms.base import add_dgnsPoaCodeAll
        df = make_dgns_poa_df(spark, [(["I10"], ["Y"])])
        result_df = add_dgnsPoaCodeAll(df)
        assert "dgnsPoaCodeStruct" not in result_df.columns

    def test_multiple_rows(self, spark):
        from cms.base import add_dgnsPoaCodeAll
        df = make_dgns_poa_df(spark, [
            (["I10", "E119"], ["Y", "Y"]),
            (["I10", "E119"], ["N", "N"]),
            (["A", "B", "C"], ["Y", "N", "Y"]),
        ])
        results = [r["dgnsPoaCodeAll"] for r in add_dgnsPoaCodeAll(df).collect()]
        assert results == [["I10", "E119"], [], ["A", "C"]]


# ============================================================
# Helper to build DataFrames with STUS_CD column
# ============================================================

def _stus_cd_schema():
    return StructType([StructField("STUS_CD", IntegerType(), True)])


def make_stus_cd_df(spark, codes):
    rows = [{"STUS_CD": c} for c in codes]
    return spark.createDataFrame(rows, schema=_stus_cd_schema())


# ============================================================
# Tests for add_diedInVisit
# ============================================================

class TestAddDiedInVisit:

    def test_stus_cd_20_returns_1(self, spark):
        from cms.base import add_diedInVisit
        df = make_stus_cd_df(spark, [20])
        result = add_diedInVisit(df).collect()[0]
        assert result["diedInVisit"] == 1

    def test_stus_cd_1_returns_0(self, spark):
        # 01 = discharged to home/self care
        from cms.base import add_diedInVisit
        df = make_stus_cd_df(spark, [1])
        result = add_diedInVisit(df).collect()[0]
        assert result["diedInVisit"] == 0

    def test_stus_cd_null_returns_null(self, spark):
        from cms.base import add_diedInVisit
        df = make_stus_cd_df(spark, [None])
        result = add_diedInVisit(df).collect()[0]
        assert result["diedInVisit"] is None

    def test_multiple_rows_mixed(self, spark):
        from cms.base import add_diedInVisit
        df = make_stus_cd_df(spark, [20, 1, 3, 20, 30, None])
        results = [r["diedInVisit"] for r in add_diedInVisit(df).collect()]
        assert results == [1, 0, 0, 1, 0, None]


# ============================================================
# Helpers to build DataFrames with acute-organ-failure flag columns
# ============================================================

AOF_COMPONENTS = [
    "shock",
    "acuteRespiratoryFailure",
    "acuteNeurologicalFailure",
    "coagulopathy",
    "acuteHepaticInjuryFailure",
    "acuteRenalInjuryFailure",
    "acidosis",
]


def _aof_schema(suffix=""):
    return StructType([
        StructField(c + suffix, IntegerType(), True) for c in AOF_COMPONENTS
    ])


def make_aof_df(spark, rows, suffix=""):
    """rows: list of dicts mapping component name (without suffix) -> 0/1.

    Missing components default to 0.
    """
    data = []
    for r in rows:
        data.append({c + suffix: r.get(c, 0) for c in AOF_COMPONENTS})
    return spark.createDataFrame(data, schema=_aof_schema(suffix))


# ============================================================
# Tests for add_acuteOrganFailure
# ============================================================

class TestAddAcuteOrganFailure:

    def test_all_zero_returns_0(self, spark):
        from cms.base import add_acuteOrganFailure
        df = make_aof_df(spark, [{}])  # all components default to 0
        result = add_acuteOrganFailure(df).collect()[0]
        assert result["acuteOrganFailure"] == 0

    def test_all_one_returns_1(self, spark):
        from cms.base import add_acuteOrganFailure
        df = make_aof_df(spark, [{c: 1 for c in AOF_COMPONENTS}])
        result = add_acuteOrganFailure(df).collect()[0]
        assert result["acuteOrganFailure"] == 1

    @pytest.mark.parametrize("component", AOF_COMPONENTS)
    def test_single_component_set_returns_1(self, spark, component):
        from cms.base import add_acuteOrganFailure
        df = make_aof_df(spark, [{component: 1}])
        result = add_acuteOrganFailure(df).collect()[0]
        assert result["acuteOrganFailure"] == 1

    def test_multiple_rows_mixed(self, spark):
        from cms.base import add_acuteOrganFailure
        df = make_aof_df(spark, [
            {},                                 # all zero -> 0
            {"shock": 1},                       # one set -> 1
            {"acidosis": 1, "coagulopathy": 1}, # two set -> 1
        ])
        results = [r["acuteOrganFailure"] for r in add_acuteOrganFailure(df).collect()]
        assert results == [0, 1, 1]


# ============================================================
# Tests for add_acuteOrganFailurePoa
# ============================================================

class TestAddAcuteOrganFailurePoa:

    def test_all_zero_returns_0(self, spark):
        from cms.base import add_acuteOrganFailurePoa
        df = make_aof_df(spark, [{}], suffix="Poa")
        result = add_acuteOrganFailurePoa(df).collect()[0]
        assert result["acuteOrganFailurePoa"] == 0

    def test_all_one_returns_1(self, spark):
        from cms.base import add_acuteOrganFailurePoa
        df = make_aof_df(spark, [{c: 1 for c in AOF_COMPONENTS}], suffix="Poa")
        result = add_acuteOrganFailurePoa(df).collect()[0]
        assert result["acuteOrganFailurePoa"] == 1

    @pytest.mark.parametrize("component", AOF_COMPONENTS)
    def test_single_component_set_returns_1(self, spark, component):
        from cms.base import add_acuteOrganFailurePoa
        df = make_aof_df(spark, [{component: 1}], suffix="Poa")
        result = add_acuteOrganFailurePoa(df).collect()[0]
        assert result["acuteOrganFailurePoa"] == 1

    def test_multiple_rows_mixed(self, spark):
        from cms.base import add_acuteOrganFailurePoa
        df = make_aof_df(spark, [
            {},
            {"shock": 1},
            {"acidosis": 1, "coagulopathy": 1},
        ], suffix="Poa")
        results = [r["acuteOrganFailurePoa"] for r in add_acuteOrganFailurePoa(df).collect()]
        assert results == [0, 1, 1]


# ============================================================
# Helper to build DataFrames with a shockDgns column
# ============================================================

def _shock_dgns_schema():
    return StructType([StructField("shockDgns", IntegerType(), True)])


def make_shock_dgns_df(spark, values):
    rows = [{"shockDgns": v} for v in values]
    return spark.createDataFrame(rows, schema=_shock_dgns_schema())


# ============================================================
# Tests for add_shock
# ============================================================

class TestAddShock:

    def test_shockDgns_1_returns_1(self, spark):
        from cms.base import add_shock
        df = make_shock_dgns_df(spark, [1])
        result = add_shock(df).collect()[0]
        assert result["shock"] == 1

    def test_shockDgns_0_returns_0(self, spark):
        from cms.base import add_shock
        df = make_shock_dgns_df(spark, [0])
        result = add_shock(df).collect()[0]
        assert result["shock"] == 0

    def test_shockDgns_null_returns_0(self, spark):
        # F.col("shockDgns")==1 yields NULL on NULL input, which falls into otherwise.
        from cms.base import add_shock
        df = make_shock_dgns_df(spark, [None])
        result = add_shock(df).collect()[0]
        assert result["shock"] == 0

    def test_multiple_rows(self, spark):
        from cms.base import add_shock
        df = make_shock_dgns_df(spark, [1, 0, 1, 0, None])
        results = [r["shock"] for r in add_shock(df).collect()]
        assert results == [1, 0, 1, 0, 0]


# ============================================================
# Helper to build DataFrames with a dgnsCodeAll column
# ============================================================

def _dgns_code_all_schema():
    return StructType([StructField("dgnsCodeAll", ArrayType(StringType()), True)])


def make_dgns_code_all_df(spark, codes_lists):
    rows = [{"dgnsCodeAll": codes} for codes in codes_lists]
    return spark.createDataFrame(rows, schema=_dgns_code_all_schema())


# ============================================================
# End-to-end tests for the shock pipeline (add_shockDgns + add_shock)
# ============================================================

SHOCK_CODES = ["R57", "I951", "I952", "I953", "I958", "I959", "R031", "R6521"]


class TestShockPipeline:

    @pytest.mark.parametrize("code", SHOCK_CODES)
    def test_each_shock_code_alone_returns_1(self, spark, code):
        from cms.base import add_shockDgns, add_shock
        df = make_dgns_code_all_df(spark, [[code]])
        result = add_shock(add_shockDgns(df)).collect()[0]
        assert result["shock"] == 1

    def test_shock_code_among_others_returns_1(self, spark):
        from cms.base import add_shockDgns, add_shock
        df = make_dgns_code_all_df(spark, [["I10", "E119", "R6521", "J189"]])
        result = add_shock(add_shockDgns(df)).collect()[0]
        assert result["shock"] == 1

    def test_no_shock_code_returns_0(self, spark):
        from cms.base import add_shockDgns, add_shock
        df = make_dgns_code_all_df(spark, [["I10", "E119", "J189"]])
        result = add_shock(add_shockDgns(df)).collect()[0]
        assert result["shock"] == 0

    def test_empty_dgns_returns_0(self, spark):
        from cms.base import add_shockDgns, add_shock
        df = make_dgns_code_all_df(spark, [[]])
        result = add_shock(add_shockDgns(df)).collect()[0]
        assert result["shock"] == 0

    def test_similar_but_different_codes_return_0(self, spark):
        # Codes near the shock list but genuinely not matching: none start with R57
        # and none are among the exact codes I951/I952/I953/I958/I959/R031/R6521.
        # I950 / I954 are I95 codes outside the list; R032 is not R031.
        from cms.base import add_shockDgns, add_shock
        df = make_dgns_code_all_df(spark, [["I950", "I954", "R032", "R56"]])
        result = add_shock(add_shockDgns(df)).collect()[0]
        assert result["shock"] == 0

    def test_r57_family_codes_return_1(self, spark):
        # Prefix matching: every R57.x code (cardiogenic R570, hypovolemic R571,
        # other R578, unspecified R579) is shock, not just the bare "R57".
        from cms.base import add_shockDgns, add_shock
        for code in ["R570", "R571", "R578", "R579"]:
            df = make_dgns_code_all_df(spark, [[code]])
            result = add_shock(add_shockDgns(df)).collect()[0]
            assert result["shock"] == 1, code

    def test_multiple_rows_mixed(self, spark):
        from cms.base import add_shockDgns, add_shock
        df = make_dgns_code_all_df(spark, [
            ["R57"],                       # shock
            ["I10", "E119"],               # no shock
            ["J189", "R6521"],             # shock
            [],                            # no shock
            ["I951", "I952"],              # shock
        ])
        results = [r["shock"] for r in add_shock(add_shockDgns(df)).collect()]
        assert results == [1, 0, 1, 0, 1]


# ============================================================
# Tests for add_shockPoa
# ============================================================

class TestAddShockPoa:

    @pytest.mark.parametrize("code", SHOCK_CODES)
    def test_each_shock_code_alone_returns_1(self, spark, code):
        from cms.base import add_shockPoa
        df = make_septic_shock_poa_df(spark, [[code]])
        result = add_shockPoa(df).collect()[0]
        assert result["shockPoa"] == 1

    def test_shock_code_among_others_returns_1(self, spark):
        from cms.base import add_shockPoa
        df = make_septic_shock_poa_df(spark, [["I10", "E119", "I952", "J189"]])
        result = add_shockPoa(df).collect()[0]
        assert result["shockPoa"] == 1

    def test_no_shock_code_returns_0(self, spark):
        from cms.base import add_shockPoa
        df = make_septic_shock_poa_df(spark, [["I10", "E119", "J189"]])
        result = add_shockPoa(df).collect()[0]
        assert result["shockPoa"] == 0

    def test_empty_array_returns_0(self, spark):
        from cms.base import add_shockPoa
        df = make_septic_shock_poa_df(spark, [[]])
        result = add_shockPoa(df).collect()[0]
        assert result["shockPoa"] == 0

    def test_similar_but_different_codes_return_0(self, spark):
        # Codes near the shock list but genuinely not matching: none start with R57
        # and none are among the exact codes I951/I952/I953/I958/I959/R031/R6521.
        from cms.base import add_shockPoa
        df = make_septic_shock_poa_df(spark, [["I950", "I954", "R032", "R56"]])
        result = add_shockPoa(df).collect()[0]
        assert result["shockPoa"] == 0

    def test_r57_family_codes_return_1(self, spark):
        # Prefix matching: every R57.x code is shock present on admission.
        from cms.base import add_shockPoa
        for code in ["R570", "R571", "R578", "R579"]:
            df = make_septic_shock_poa_df(spark, [[code]])
            result = add_shockPoa(df).collect()[0]
            assert result["shockPoa"] == 1, code

    def test_multiple_rows_mixed(self, spark):
        from cms.base import add_shockPoa
        df = make_septic_shock_poa_df(spark, [
            ["R57"],
            ["I10", "E119"],
            ["J189", "R6521"],
            [],
            ["I951", "I952"],
        ])
        results = [r["shockPoa"] for r in add_shockPoa(df).collect()]
        assert results == [1, 0, 1, 0, 1]


# ============================================================
# Helper to build DataFrames with dgnsCodeAll + prcdrCodeAll
# ============================================================

def _dgns_prcdr_schema():
    return StructType([
        StructField("dgnsCodeAll", ArrayType(StringType()), True),
        StructField("prcdrCodeAll", ArrayType(StringType()), True),
    ])


def make_dgns_prcdr_df(spark, rows):
    """rows: list of (dgns_codes, prcdr_codes) tuples."""
    data = [{"dgnsCodeAll": d, "prcdrCodeAll": p} for d, p in rows]
    return spark.createDataFrame(data, schema=_dgns_prcdr_schema())


# ============================================================
# End-to-end tests for add_acuteRespiratoryFailure
# ============================================================

ARF_DGNS_CODES = ["J80", "J960", "J969", "R063", "R092", "R0600", "R0603", "R0609", "R0683", "R0689"]
ARF_PRCDR_CODES = ["5A1935Z", "5A1945Z", "5A1955Z"]


class TestAddAcuteRespiratoryFailure:

    @pytest.mark.parametrize("code", ARF_DGNS_CODES)
    def test_each_dgns_code_alone_returns_1(self, spark, code):
        from cms.base import add_acuteRespiratoryFailure
        df = make_dgns_prcdr_df(spark, [([code], [])])
        result = add_acuteRespiratoryFailure(df).collect()[0]
        assert result["acuteRespiratoryFailure"] == 1

    @pytest.mark.parametrize("code", ARF_PRCDR_CODES)
    def test_each_prcdr_code_alone_returns_1(self, spark, code):
        from cms.base import add_acuteRespiratoryFailure
        df = make_dgns_prcdr_df(spark, [([], [code])])
        result = add_acuteRespiratoryFailure(df).collect()[0]
        assert result["acuteRespiratoryFailure"] == 1

    def test_dgns_code_among_others_returns_1(self, spark):
        from cms.base import add_acuteRespiratoryFailure
        df = make_dgns_prcdr_df(spark, [(["I10", "E119", "J969"], ["00H00MZ"])])
        result = add_acuteRespiratoryFailure(df).collect()[0]
        assert result["acuteRespiratoryFailure"] == 1

    def test_prcdr_code_among_others_returns_1(self, spark):
        from cms.base import add_acuteRespiratoryFailure
        df = make_dgns_prcdr_df(spark, [(["I10", "E119"], ["00H00MZ", "5A1945Z"])])
        result = add_acuteRespiratoryFailure(df).collect()[0]
        assert result["acuteRespiratoryFailure"] == 1

    def test_no_matching_codes_returns_0(self, spark):
        from cms.base import add_acuteRespiratoryFailure
        df = make_dgns_prcdr_df(spark, [(["I10", "E119", "J189"], ["00H00MZ"])])
        result = add_acuteRespiratoryFailure(df).collect()[0]
        assert result["acuteRespiratoryFailure"] == 0

    def test_empty_arrays_return_0(self, spark):
        from cms.base import add_acuteRespiratoryFailure
        df = make_dgns_prcdr_df(spark, [([], [])])
        result = add_acuteRespiratoryFailure(df).collect()[0]
        assert result["acuteRespiratoryFailure"] == 0

    def test_similar_but_different_codes_return_0(self, spark):
        # J81/J961/J968/R060 are not in the ARF dgns list; 5A1925Z is not in the prcdr list.
        from cms.base import add_acuteRespiratoryFailure
        df = make_dgns_prcdr_df(spark, [(["J81", "J961", "J968", "R060"], ["5A1925Z"])])
        result = add_acuteRespiratoryFailure(df).collect()[0]
        assert result["acuteRespiratoryFailure"] == 0

    def test_dgns_and_prcdr_both_match_returns_1(self, spark):
        from cms.base import add_acuteRespiratoryFailure
        df = make_dgns_prcdr_df(spark, [(["J80"], ["5A1955Z"])])
        result = add_acuteRespiratoryFailure(df).collect()[0]
        assert result["acuteRespiratoryFailure"] == 1

    def test_multiple_rows_mixed(self, spark):
        from cms.base import add_acuteRespiratoryFailure
        df = make_dgns_prcdr_df(spark, [
            (["J80"], []),                       # dgns match
            ([], ["5A1935Z"]),                   # prcdr match
            (["I10", "E119"], ["00H00MZ"]),      # neither
            ([], []),                            # empty
            (["R0689"], ["5A1945Z"]),            # both match
        ])
        results = [r["acuteRespiratoryFailure"] for r in add_acuteRespiratoryFailure(df).collect()]
        assert results == [1, 1, 0, 0, 1]


# ============================================================
# End-to-end tests for add_acuteNeurologicalFailure
# ============================================================

ANF_DGNS_CODES = ["F05", "F06", "F53", "G931", "G934", "R401", "R402", "I6783"]
ANF_PRCDR_CODES = ["4A0034Z", "4A00X4Z", "4A0134Z", "4A01X4Z", "4A1034Z", "4A10X4Z"]


class TestAddAcuteNeurologicalFailure:

    @pytest.mark.parametrize("code", ANF_DGNS_CODES)
    def test_each_dgns_code_alone_returns_1(self, spark, code):
        from cms.base import add_acuteNeurologicalFailure
        df = make_dgns_prcdr_df(spark, [([code], [])])
        result = add_acuteNeurologicalFailure(df).collect()[0]
        assert result["acuteNeurologicalFailure"] == 1

    @pytest.mark.parametrize("code", ANF_PRCDR_CODES)
    def test_each_prcdr_code_alone_returns_1(self, spark, code):
        from cms.base import add_acuteNeurologicalFailure
        df = make_dgns_prcdr_df(spark, [([], [code])])
        result = add_acuteNeurologicalFailure(df).collect()[0]
        assert result["acuteNeurologicalFailure"] == 1

    def test_dgns_code_among_others_returns_1(self, spark):
        from cms.base import add_acuteNeurologicalFailure
        df = make_dgns_prcdr_df(spark, [(["I10", "E119", "G931"], ["00H00MZ"])])
        result = add_acuteNeurologicalFailure(df).collect()[0]
        assert result["acuteNeurologicalFailure"] == 1

    def test_prcdr_code_among_others_returns_1(self, spark):
        from cms.base import add_acuteNeurologicalFailure
        df = make_dgns_prcdr_df(spark, [(["I10", "E119"], ["00H00MZ", "4A0134Z"])])
        result = add_acuteNeurologicalFailure(df).collect()[0]
        assert result["acuteNeurologicalFailure"] == 1

    def test_no_matching_codes_returns_0(self, spark):
        from cms.base import add_acuteNeurologicalFailure
        df = make_dgns_prcdr_df(spark, [(["I10", "E119", "J189"], ["00H00MZ"])])
        result = add_acuteNeurologicalFailure(df).collect()[0]
        assert result["acuteNeurologicalFailure"] == 0

    def test_empty_arrays_return_0(self, spark):
        from cms.base import add_acuteNeurologicalFailure
        df = make_dgns_prcdr_df(spark, [([], [])])
        result = add_acuteNeurologicalFailure(df).collect()[0]
        assert result["acuteNeurologicalFailure"] == 0

    def test_similar_but_different_codes_return_0(self, spark):
        # Codes near the ANF lists but not in them.
        # F04/F07/F54/G930/G935/R400/R403/I6782 are not in the dgns list.
        # 4A0044Z is not in the prcdr list.
        from cms.base import add_acuteNeurologicalFailure
        df = make_dgns_prcdr_df(spark, [
            (["F04", "F07", "F54", "G930", "G935", "R400", "R403", "I6782"], ["4A0044Z"])
        ])
        result = add_acuteNeurologicalFailure(df).collect()[0]
        assert result["acuteNeurologicalFailure"] == 0

    def test_dgns_and_prcdr_both_match_returns_1(self, spark):
        from cms.base import add_acuteNeurologicalFailure
        df = make_dgns_prcdr_df(spark, [(["F05"], ["4A1034Z"])])
        result = add_acuteNeurologicalFailure(df).collect()[0]
        assert result["acuteNeurologicalFailure"] == 1

    def test_multiple_rows_mixed(self, spark):
        from cms.base import add_acuteNeurologicalFailure
        df = make_dgns_prcdr_df(spark, [
            (["F05"], []),                       # dgns match
            ([], ["4A0034Z"]),                   # prcdr match
            (["I10", "E119"], ["00H00MZ"]),      # neither
            ([], []),                            # empty
            (["I6783"], ["4A10X4Z"]),            # both match
        ])
        results = [r["acuteNeurologicalFailure"] for r in add_acuteNeurologicalFailure(df).collect()]
        assert results == [1, 1, 0, 0, 1]


# ============================================================
# End-to-end tests for add_coagulopathy
# ============================================================

COAG_DGNS_CODES = ["D65", "D688", "D689", "D696", "D473", "D681", "D6959", "D6951"]


class TestAddCoagulopathy:

    @pytest.mark.parametrize("code", COAG_DGNS_CODES)
    def test_each_code_alone_returns_1(self, spark, code):
        from cms.base import add_coagulopathy
        df = make_dgns_code_all_df(spark, [[code]])
        result = add_coagulopathy(df).collect()[0]
        assert result["coagulopathy"] == 1

    def test_code_among_others_returns_1(self, spark):
        from cms.base import add_coagulopathy
        df = make_dgns_code_all_df(spark, [["I10", "E119", "D696", "J189"]])
        result = add_coagulopathy(df).collect()[0]
        assert result["coagulopathy"] == 1

    def test_no_matching_code_returns_0(self, spark):
        from cms.base import add_coagulopathy
        df = make_dgns_code_all_df(spark, [["I10", "E119", "J189"]])
        result = add_coagulopathy(df).collect()[0]
        assert result["coagulopathy"] == 0

    def test_empty_array_returns_0(self, spark):
        from cms.base import add_coagulopathy
        df = make_dgns_code_all_df(spark, [[]])
        result = add_coagulopathy(df).collect()[0]
        assert result["coagulopathy"] == 0

    def test_similar_but_different_codes_return_0(self, spark):
        # Codes near the coag list but not in it.
        # D66 is hemophilia A (different from D65 / DIC).
        # D687/D690 are not in the list; D6952/D6958 are not in the list.
        from cms.base import add_coagulopathy
        df = make_dgns_code_all_df(spark, [["D66", "D687", "D690", "D6952", "D6958", "D472"]])
        result = add_coagulopathy(df).collect()[0]
        assert result["coagulopathy"] == 0

    def test_multiple_rows_mixed(self, spark):
        from cms.base import add_coagulopathy
        df = make_dgns_code_all_df(spark, [
            ["D65"],
            ["I10", "E119"],
            ["J189", "D6959"],
            [],
            ["D688", "D689"],
        ])
        results = [r["coagulopathy"] for r in add_coagulopathy(df).collect()]
        assert results == [1, 0, 1, 0, 1]


# ============================================================
# End-to-end tests for add_acuteHepaticInjuryFailure
# ============================================================

AHIF_DGNS_CODES = ["K720", "K762", "K763", "K716", "K759", "K7291"]


class TestAddAcuteHepaticInjuryFailure:

    @pytest.mark.parametrize("code", AHIF_DGNS_CODES)
    def test_each_code_alone_returns_1(self, spark, code):
        from cms.base import add_acuteHepaticInjuryFailure
        df = make_dgns_code_all_df(spark, [[code]])
        result = add_acuteHepaticInjuryFailure(df).collect()[0]
        assert result["acuteHepaticInjuryFailure"] == 1

    def test_code_among_others_returns_1(self, spark):
        from cms.base import add_acuteHepaticInjuryFailure
        df = make_dgns_code_all_df(spark, [["I10", "E119", "K762", "J189"]])
        result = add_acuteHepaticInjuryFailure(df).collect()[0]
        assert result["acuteHepaticInjuryFailure"] == 1

    def test_no_matching_code_returns_0(self, spark):
        from cms.base import add_acuteHepaticInjuryFailure
        df = make_dgns_code_all_df(spark, [["I10", "E119", "J189"]])
        result = add_acuteHepaticInjuryFailure(df).collect()[0]
        assert result["acuteHepaticInjuryFailure"] == 0

    def test_empty_array_returns_0(self, spark):
        from cms.base import add_acuteHepaticInjuryFailure
        df = make_dgns_code_all_df(spark, [[]])
        result = add_acuteHepaticInjuryFailure(df).collect()[0]
        assert result["acuteHepaticInjuryFailure"] == 0

    def test_similar_but_different_codes_return_0(self, spark):
        # Codes near the AHIF list but not in it.
        # K721/K761/K764/K717/K758/K7290 are not in the list.
        from cms.base import add_acuteHepaticInjuryFailure
        df = make_dgns_code_all_df(spark, [["K721", "K761", "K764", "K717", "K758", "K7290"]])
        result = add_acuteHepaticInjuryFailure(df).collect()[0]
        assert result["acuteHepaticInjuryFailure"] == 0

    def test_multiple_rows_mixed(self, spark):
        from cms.base import add_acuteHepaticInjuryFailure
        df = make_dgns_code_all_df(spark, [
            ["K720"],
            ["I10", "E119"],
            ["J189", "K7291"],
            [],
            ["K762", "K763"],
        ])
        results = [r["acuteHepaticInjuryFailure"] for r in add_acuteHepaticInjuryFailure(df).collect()]
        assert results == [1, 0, 1, 0, 1]


# ============================================================
# End-to-end tests for add_acuteRenalInjuryFailure
# ============================================================

ARIF_DGNS_CODES = ["N17", "N003"]
# Real ICD-10-PCS codes for hemodialysis/peritoneal dialysis all start with 5A1D.
ARIF_PRCDR_CODES = ["5A1D00Z", "5A1D60Z", "5A1D70Z", "5A1D80Z", "5A1D90Z"]


class TestAddAcuteRenalInjuryFailure:

    @pytest.mark.parametrize("code", ARIF_DGNS_CODES)
    def test_each_dgns_code_alone_returns_1(self, spark, code):
        from cms.base import add_acuteRenalInjuryFailure
        df = make_dgns_prcdr_df(spark, [([code], [])])
        result = add_acuteRenalInjuryFailure(df).collect()[0]
        assert result["acuteRenalInjuryFailure"] == 1

    @pytest.mark.parametrize("code", ARIF_PRCDR_CODES)
    def test_each_prcdr_code_alone_returns_1(self, spark, code):
        from cms.base import add_acuteRenalInjuryFailure
        df = make_dgns_prcdr_df(spark, [([], [code])])
        result = add_acuteRenalInjuryFailure(df).collect()[0]
        assert result["acuteRenalInjuryFailure"] == 1

    def test_dgns_code_among_others_returns_1(self, spark):
        from cms.base import add_acuteRenalInjuryFailure
        df = make_dgns_prcdr_df(spark, [(["I10", "E119", "N17"], ["00H00MZ"])])
        result = add_acuteRenalInjuryFailure(df).collect()[0]
        assert result["acuteRenalInjuryFailure"] == 1

    def test_prcdr_code_among_others_returns_1(self, spark):
        from cms.base import add_acuteRenalInjuryFailure
        df = make_dgns_prcdr_df(spark, [(["I10", "E119"], ["00H00MZ", "5A1D70Z"])])
        result = add_acuteRenalInjuryFailure(df).collect()[0]
        assert result["acuteRenalInjuryFailure"] == 1

    def test_no_matching_codes_returns_0(self, spark):
        from cms.base import add_acuteRenalInjuryFailure
        df = make_dgns_prcdr_df(spark, [(["I10", "E119", "J189"], ["00H00MZ"])])
        result = add_acuteRenalInjuryFailure(df).collect()[0]
        assert result["acuteRenalInjuryFailure"] == 0

    def test_empty_arrays_return_0(self, spark):
        from cms.base import add_acuteRenalInjuryFailure
        df = make_dgns_prcdr_df(spark, [([], [])])
        result = add_acuteRenalInjuryFailure(df).collect()[0]
        assert result["acuteRenalInjuryFailure"] == 0

    def test_similar_but_different_codes_return_0(self, spark):
        # N18/N19/N002 are not in the dgns list. 5A2D00Z / 5B1D00Z don't start with 5A1D.
        from cms.base import add_acuteRenalInjuryFailure
        df = make_dgns_prcdr_df(spark, [(["N18", "N19", "N002"], ["5A2D00Z", "5B1D00Z"])])
        result = add_acuteRenalInjuryFailure(df).collect()[0]
        assert result["acuteRenalInjuryFailure"] == 0

    def test_dgns_and_prcdr_both_match_returns_1(self, spark):
        from cms.base import add_acuteRenalInjuryFailure
        df = make_dgns_prcdr_df(spark, [(["N17"], ["5A1D90Z"])])
        result = add_acuteRenalInjuryFailure(df).collect()[0]
        assert result["acuteRenalInjuryFailure"] == 1

    def test_multiple_rows_mixed(self, spark):
        from cms.base import add_acuteRenalInjuryFailure
        df = make_dgns_prcdr_df(spark, [
            (["N17"], []),                       # dgns match
            ([], ["5A1D60Z"]),                   # prcdr match
            (["I10", "E119"], ["00H00MZ"]),      # neither
            ([], []),                            # empty
            (["N003"], ["5A1D80Z"]),             # both match
        ])
        results = [r["acuteRenalInjuryFailure"] for r in add_acuteRenalInjuryFailure(df).collect()]
        assert results == [1, 1, 0, 0, 1]


# ============================================================
# End-to-end tests for add_acidosis (add_acidosisDgns + add_acidosis)
# ============================================================

class TestAddAcidosis:

    def test_e872_alone_returns_1(self, spark):
        from cms.base import add_acidosis
        df = make_dgns_code_all_df(spark, [["E872"]])
        result = add_acidosis(df).collect()[0]
        assert result["acidosis"] == 1
        assert result["acidosisDgns"] == 1

    def test_e872_among_others_returns_1(self, spark):
        from cms.base import add_acidosis
        df = make_dgns_code_all_df(spark, [["I10", "E119", "E872", "J189"]])
        result = add_acidosis(df).collect()[0]
        assert result["acidosis"] == 1

    def test_no_e872_returns_0(self, spark):
        from cms.base import add_acidosis
        df = make_dgns_code_all_df(spark, [["I10", "E119", "J189"]])
        result = add_acidosis(df).collect()[0]
        assert result["acidosis"] == 0

    def test_empty_array_returns_0(self, spark):
        from cms.base import add_acidosis
        df = make_dgns_code_all_df(spark, [[]])
        result = add_acidosis(df).collect()[0]
        assert result["acidosis"] == 0

    def test_similar_but_different_codes_return_0(self, spark):
        # E87 / E871 / E873 do NOT start with E872, so none match.
        from cms.base import add_acidosis
        df = make_dgns_code_all_df(spark, [["E87", "E871", "E873"]])
        result = add_acidosis(df).collect()[0]
        assert result["acidosis"] == 0

    def test_e872_prefixed_code_returns_1(self, spark):
        # Prefix matching: any code starting with E872 matches (per get_acidosisCondition).
        from cms.base import add_acidosis
        df = make_dgns_code_all_df(spark, [["E8720"]])
        result = add_acidosis(df).collect()[0]
        assert result["acidosis"] == 1

    def test_multiple_rows_mixed(self, spark):
        from cms.base import add_acidosis
        df = make_dgns_code_all_df(spark, [
            ["E872"],                          # acidosis
            ["I10", "E119"],                   # no acidosis
            ["J189", "E872"],                  # acidosis
            [],                                # no acidosis
            ["E871", "E873"],                  # similar but no exact match
            ["E872", "E872"],                  # duplicate E872 still 1
        ])
        results = [r["acidosis"] for r in add_acidosis(df).collect()]
        assert results == [1, 0, 1, 0, 0, 1]

    def test_acidosisDgns_column_also_added(self, spark):
        from cms.base import add_acidosis
        df = make_dgns_code_all_df(spark, [["E872"], ["I10"]])
        result_df = add_acidosis(df)
        assert "acidosisDgns" in result_df.columns
        assert "acidosis" in result_df.columns
        rows = result_df.collect()
        assert rows[0]["acidosisDgns"] == 1 and rows[0]["acidosis"] == 1
        assert rows[1]["acidosisDgns"] == 0 and rows[1]["acidosis"] == 0


# ============================================================
# End-to-end tests for add_rrt (add_rrtPrcdr + add_rrt)
# ============================================================

RRT_PRCDR_CODES = ["5A1D00Z", "5A1D60Z", "5A1D70Z", "5A1D80Z", "5A1D90Z"]


class TestAddRrt:

    @pytest.mark.parametrize("code", RRT_PRCDR_CODES)
    def test_each_5a1d_code_alone_returns_1(self, spark, code):
        from cms.base import add_rrt
        df = make_dgns_prcdr_df(spark, [([], [code])])
        result = add_rrt(df).collect()[0]
        assert result["rrt"] == 1
        assert result["rrtPrcdr"] == 1

    def test_5a1d_code_among_others_returns_1(self, spark):
        from cms.base import add_rrt
        df = make_dgns_prcdr_df(spark, [(["I10", "E119"], ["00H00MZ", "5A1D90Z", "0DJ08ZZ"])])
        result = add_rrt(df).collect()[0]
        assert result["rrt"] == 1

    def test_no_5a1d_code_returns_0(self, spark):
        from cms.base import add_rrt
        df = make_dgns_prcdr_df(spark, [(["I10", "E119", "N17"], ["00H00MZ", "0DJ08ZZ"])])
        result = add_rrt(df).collect()[0]
        assert result["rrt"] == 0

    def test_empty_prcdr_array_returns_0(self, spark):
        from cms.base import add_rrt
        df = make_dgns_prcdr_df(spark, [([], [])])
        result = add_rrt(df).collect()[0]
        assert result["rrt"] == 0

    def test_similar_but_different_codes_return_0(self, spark):
        # 5A1C/5A1E/5A2D differ in the 4th character; 15A1D90Z does not start with 5A1D.
        from cms.base import add_rrt
        df = make_dgns_prcdr_df(spark, [([], ["5A1C00Z", "5A1E00Z", "5A2D00Z", "15A1D90Z"])])
        result = add_rrt(df).collect()[0]
        assert result["rrt"] == 0

    def test_dgns_5a1d_does_not_trigger(self, spark):
        # The 5A1D match must come from prcdrCodeAll, not dgnsCodeAll.
        from cms.base import add_rrt
        df = make_dgns_prcdr_df(spark, [(["5A1D90Z"], ["00H00MZ"])])
        result = add_rrt(df).collect()[0]
        assert result["rrt"] == 0

    def test_multiple_rows_mixed(self, spark):
        from cms.base import add_rrt
        df = make_dgns_prcdr_df(spark, [
            ([], ["5A1D90Z"]),                       # RRT
            (["N17"], ["00H00MZ"]),                  # no RRT
            ([], ["0DJ08ZZ", "5A1D70Z"]),            # RRT
            ([], []),                                # no RRT
            ([], ["5A1C00Z", "5A2D00Z"]),            # similar but no
            (["I10"], ["5A1D00Z", "5A1D80Z"]),       # RRT (multiple matches)
        ])
        results = [r["rrt"] for r in add_rrt(df).collect()]
        assert results == [1, 0, 1, 0, 0, 1]

    def test_rrtPrcdr_column_also_added(self, spark):
        from cms.base import add_rrt
        df = make_dgns_prcdr_df(spark, [([], ["5A1D90Z"]), ([], ["00H00MZ"])])
        result_df = add_rrt(df)
        assert "rrtPrcdr" in result_df.columns
        assert "rrt" in result_df.columns
        rows = result_df.collect()
        assert rows[0]["rrtPrcdr"] == 1 and rows[0]["rrt"] == 1
        assert rows[1]["rrtPrcdr"] == 0 and rows[1]["rrt"] == 0


# ============================================================
# End-to-end tests for add_ecmo (add_ecmoPrcdr + add_ecmo)
# ============================================================

ECMO_PRCDR_CODES = ["5A1522F", "5A1522G", "5A1522H"]


class TestAddEcmo:

    @pytest.mark.parametrize("code", ECMO_PRCDR_CODES)
    def test_each_ecmo_code_alone_returns_1(self, spark, code):
        from cms.base import add_ecmo
        df = make_dgns_prcdr_df(spark, [([], [code])])
        result = add_ecmo(df).collect()[0]
        assert result["ecmo"] == 1
        assert result["ecmoPrcdr"] == 1

    def test_ecmo_code_among_others_returns_1(self, spark):
        from cms.base import add_ecmo
        df = make_dgns_prcdr_df(spark, [(["I10", "E119"], ["00H00MZ", "5A1522G", "0DJ08ZZ"])])
        result = add_ecmo(df).collect()[0]
        assert result["ecmo"] == 1

    def test_no_ecmo_code_returns_0(self, spark):
        from cms.base import add_ecmo
        df = make_dgns_prcdr_df(spark, [(["I10", "E119", "N17"], ["00H00MZ", "0DJ08ZZ"])])
        result = add_ecmo(df).collect()[0]
        assert result["ecmo"] == 0

    def test_empty_prcdr_array_returns_0(self, spark):
        from cms.base import add_ecmo
        df = make_dgns_prcdr_df(spark, [([], [])])
        result = add_ecmo(df).collect()[0]
        assert result["ecmo"] == 0

    def test_similar_but_different_codes_return_0(self, spark):
        # 5A1522A/5A1522Z differ in the last character; 5A15220 and 5A1521F differ earlier.
        from cms.base import add_ecmo
        df = make_dgns_prcdr_df(spark, [([], ["5A1522A", "5A1522Z", "5A15220", "5A1521F"])])
        result = add_ecmo(df).collect()[0]
        assert result["ecmo"] == 0

    def test_dgns_ecmo_does_not_trigger(self, spark):
        # An ECMO code in dgnsCodeAll must not trigger ecmo.
        from cms.base import add_ecmo
        df = make_dgns_prcdr_df(spark, [(["5A1522F"], ["00H00MZ"])])
        result = add_ecmo(df).collect()[0]
        assert result["ecmo"] == 0

    def test_multiple_rows_mixed(self, spark):
        from cms.base import add_ecmo
        df = make_dgns_prcdr_df(spark, [
            ([], ["5A1522F"]),                          # ECMO (VA)
            (["N17"], ["00H00MZ"]),                     # no ECMO
            ([], ["0DJ08ZZ", "5A1522G"]),               # ECMO
            ([], []),                                   # no ECMO
            ([], ["5A1521F", "5A1522A"]),               # similar but no
            (["I10"], ["5A1522F", "5A1522H"]),          # ECMO (multiple matches)
        ])
        results = [r["ecmo"] for r in add_ecmo(df).collect()]
        assert results == [1, 0, 1, 0, 0, 1]

    def test_ecmoPrcdr_column_also_added(self, spark):
        from cms.base import add_ecmo
        df = make_dgns_prcdr_df(spark, [([], ["5A1522F"]), ([], ["00H00MZ"])])
        result_df = add_ecmo(df)
        assert "ecmoPrcdr" in result_df.columns
        assert "ecmo" in result_df.columns
        rows = result_df.collect()
        assert rows[0]["ecmoPrcdr"] == 1 and rows[0]["ecmo"] == 1
        assert rows[1]["ecmoPrcdr"] == 0 and rows[1]["ecmo"] == 0


# ============================================================
# End-to-end tests for add_endoscopy (add_endoscopyPrcdr + add_endoscopy)
# ============================================================

# Representative codes spanning the endoscopy list (drainage, excision,
# extraction, inspection, insertion, occlusion, release, removal,
# reposition, restriction, revision, introduction, irrigation).
ENDOSCOPY_SAMPLE_CODES = [
    "0D917ZX",  # 0D9 drainage
    "0D9A7ZX",
    "0DB18ZX",  # 0DB excision
    "0DBE7ZX",
    "0DD18ZX",  # 0DD extraction
    "0DJ08ZZ",  # 0DJ inspection
    "0D20X0Z",  # 0D2 change
    "0D514ZZ",  # 0D5 destruction
    "0D767DZ",  # 0D7 dilation
    "0D9130Z",  # 0D9 (drainage with device)
    "0DH50DZ",  # 0DH insertion
    "0DL10CZ",  # 0DL occlusion
    "0DN97ZZ",  # 0DN release
    "0DP070Z",  # 0DP removal
    "0DS5XZZ",  # 0DS reposition
    "0DV67DZ",  # 0DV restriction
    "0DW03YZ",  # 0DW revision
    "0DWD3YZ",
    "3E0G328",  # 3E0G introduction
    "3E1G38Z",  # 3E1G irrigation
]


class TestAddEndoscopy:

    @pytest.mark.parametrize("code", ENDOSCOPY_SAMPLE_CODES)
    def test_each_endoscopy_code_alone_returns_1(self, spark, code):
        from cms.base import add_endoscopy
        df = make_dgns_prcdr_df(spark, [([], [code])])
        result = add_endoscopy(df).collect()[0]
        assert result["endoscopy"] == 1
        assert result["endoscopyPrcdr"] == 1

    def test_endoscopy_code_among_others_returns_1(self, spark):
        from cms.base import add_endoscopy
        df = make_dgns_prcdr_df(spark, [(["I10", "E119"], ["00H00MZ", "0DJ08ZZ", "5A1D90Z"])])
        result = add_endoscopy(df).collect()[0]
        assert result["endoscopy"] == 1

    def test_no_endoscopy_code_returns_0(self, spark):
        from cms.base import add_endoscopy
        df = make_dgns_prcdr_df(spark, [(["I10", "E119", "N17"], ["00H00MZ", "5A1D90Z", "5A1522F"])])
        result = add_endoscopy(df).collect()[0]
        assert result["endoscopy"] == 0

    def test_empty_prcdr_array_returns_0(self, spark):
        from cms.base import add_endoscopy
        df = make_dgns_prcdr_df(spark, [([], [])])
        result = add_endoscopy(df).collect()[0]
        assert result["endoscopy"] == 0

    def test_similar_but_different_codes_return_0(self, spark):
        # Codes that are NOT in the endoscopy list but look similar.
        # 0D917ZZ vs 0D917ZX (last char), 0DJ07ZZ vs 0DJ08ZZ (7th char),
        # 3E0G327 vs 3E0G328 (last char), 0DH50DY vs 0DH50DZ (last char).
        from cms.base import add_endoscopy
        df = make_dgns_prcdr_df(spark, [([], ["0D917ZZ", "0DJ07ZZ", "3E0G327", "0DH50DY"])])
        result = add_endoscopy(df).collect()[0]
        assert result["endoscopy"] == 0

    def test_dgns_endoscopy_does_not_trigger(self, spark):
        # An endoscopy code placed in dgnsCodeAll must not trigger endoscopy.
        from cms.base import add_endoscopy
        df = make_dgns_prcdr_df(spark, [(["0DJ08ZZ"], ["00H00MZ"])])
        result = add_endoscopy(df).collect()[0]
        assert result["endoscopy"] == 0

    def test_multiple_rows_mixed(self, spark):
        from cms.base import add_endoscopy
        df = make_dgns_prcdr_df(spark, [
            ([], ["0DJ08ZZ"]),                          # endoscopy (inspection)
            (["N17"], ["00H00MZ"]),                     # no endoscopy
            ([], ["0D917ZX", "5A1D90Z"]),               # endoscopy (drainage)
            ([], []),                                   # no endoscopy
            ([], ["0D917ZZ", "3E0G327"]),               # similar but no
            (["I10"], ["3E1G38Z", "0DB18ZX"]),          # endoscopy (multiple matches)
        ])
        results = [r["endoscopy"] for r in add_endoscopy(df).collect()]
        assert results == [1, 0, 1, 0, 0, 1]

    def test_endoscopyPrcdr_column_also_added(self, spark):
        from cms.base import add_endoscopy
        df = make_dgns_prcdr_df(spark, [([], ["0DJ08ZZ"]), ([], ["00H00MZ"])])
        result_df = add_endoscopy(df)
        assert "endoscopyPrcdr" in result_df.columns
        assert "endoscopy" in result_df.columns
        rows = result_df.collect()
        assert rows[0]["endoscopyPrcdr"] == 1 and rows[0]["endoscopy"] == 1
        assert rows[1]["endoscopyPrcdr"] == 0 and rows[1]["endoscopy"] == 0


# ============================================================
# Tests for add_transferToIn
# ============================================================

class TestAddTransferToIn:

    def test_stus_cd_2_returns_1(self, spark):
        # 02 = discharged/transferred to a short-term general hospital
        from cms.base import add_transferToIn
        df = make_stus_cd_df(spark, [2])
        result = add_transferToIn(df).collect()[0]
        assert result["transferToIn"] == 1

    def test_stus_cd_5_returns_1(self, spark):
        # 05 = discharged/transferred to other IPT care
        from cms.base import add_transferToIn
        df = make_stus_cd_df(spark, [5])
        result = add_transferToIn(df).collect()[0]
        assert result["transferToIn"] == 1

    def test_stus_cd_1_returns_0(self, spark):
        # 01 = discharged to home/self care
        from cms.base import add_transferToIn
        df = make_stus_cd_df(spark, [1])
        result = add_transferToIn(df).collect()[0]
        assert result["transferToIn"] == 0

    def test_stus_cd_20_returns_0(self, spark):
        # 20 = expired
        from cms.base import add_transferToIn
        df = make_stus_cd_df(spark, [20])
        result = add_transferToIn(df).collect()[0]
        assert result["transferToIn"] == 0

    @pytest.mark.parametrize("code", [3, 4, 6, 7, 25, 30, 50, 65])
    def test_other_stus_codes_return_0(self, spark, code):
        from cms.base import add_transferToIn
        df = make_stus_cd_df(spark, [code])
        result = add_transferToIn(df).collect()[0]
        assert result["transferToIn"] == 0

    def test_stus_cd_null_returns_0(self, spark):
        # F.col("STUS_CD").isin([2,5]) yields NULL on NULL input,
        # which falls into the .otherwise(0) branch.
        from cms.base import add_transferToIn
        df = make_stus_cd_df(spark, [None])
        result = add_transferToIn(df).collect()[0]
        assert result["transferToIn"] == 0

    def test_multiple_rows_mixed(self, spark):
        from cms.base import add_transferToIn
        df = make_stus_cd_df(spark, [2, 5, 1, 3, 20, 5, None, 2, 50])
        results = [r["transferToIn"] for r in add_transferToIn(df).collect()]
        assert results == [1, 1, 0, 0, 0, 1, 0, 1, 0]

    def test_column_added(self, spark):
        from cms.base import add_transferToIn
        df = make_stus_cd_df(spark, [2])
        result_df = add_transferToIn(df)
        assert "transferToIn" in result_df.columns
        assert "STUS_CD" in result_df.columns


# ============================================================
# Helper to build DataFrames with SRC_ADMS column
# ============================================================

def _src_adms_schema():
    return StructType([StructField("SRC_ADMS", IntegerType(), True)])


def make_src_adms_df(spark, codes):
    rows = [{"SRC_ADMS": c} for c in codes]
    return spark.createDataFrame(rows, schema=_src_adms_schema())


# ============================================================
# Tests for add_transferFromDifferentFacility
# ============================================================

class TestAddTransferFromDifferentFacility:

    def test_src_adms_4_returns_1(self, spark):
        # 04 = transfer from a different hospital
        from cms.base import add_transferFromDifferentFacility
        df = make_src_adms_df(spark, [4])
        result = add_transferFromDifferentFacility(df).collect()[0]
        assert result["transferFromDifferentFacility"] == 1

    def test_src_adms_1_returns_0(self, spark):
        # 01 = physician referral
        from cms.base import add_transferFromDifferentFacility
        df = make_src_adms_df(spark, [1])
        result = add_transferFromDifferentFacility(df).collect()[0]
        assert result["transferFromDifferentFacility"] == 0

    @pytest.mark.parametrize("code", [1, 2, 3, 5, 6, 7, 8, 9])
    def test_other_src_codes_return_0(self, spark, code):
        from cms.base import add_transferFromDifferentFacility
        df = make_src_adms_df(spark, [code])
        result = add_transferFromDifferentFacility(df).collect()[0]
        assert result["transferFromDifferentFacility"] == 0

    def test_src_adms_null_returns_0(self, spark):
        # F.col("SRC_ADMS")==4 yields NULL on NULL input,
        # which falls into the .otherwise(0) branch.
        from cms.base import add_transferFromDifferentFacility
        df = make_src_adms_df(spark, [None])
        result = add_transferFromDifferentFacility(df).collect()[0]
        assert result["transferFromDifferentFacility"] == 0

    def test_multiple_rows_mixed(self, spark):
        from cms.base import add_transferFromDifferentFacility
        df = make_src_adms_df(spark, [4, 1, 2, 4, 5, None, 4, 9])
        results = [r["transferFromDifferentFacility"] for r in add_transferFromDifferentFacility(df).collect()]
        assert results == [1, 0, 0, 1, 0, 0, 1, 0]

    def test_column_added(self, spark):
        from cms.base import add_transferFromDifferentFacility
        df = make_src_adms_df(spark, [4])
        result_df = add_transferFromDifferentFacility(df)
        assert "transferFromDifferentFacility" in result_df.columns
        assert "SRC_ADMS" in result_df.columns


# ============================================================
# Helper to build a DataFrame using a real cms.schemas claim schema,
# padding unspecified fields with NULL.
# ============================================================

def make_real_claim_df(spark, claim_type, rows):
    """Build a DataFrame for the given claim_type using the production schema
    from cms.schemas. Each row only needs to specify the fields the test cares
    about; everything else in the schema is set to None.

    Args:
        claim_type: one of "opBase", "ipBase", "snfBase", "hhaBase", "hospBase"
        rows: list of dicts of partial field values.
    """
    from cms.schemas import schemas
    schema = schemas[claim_type]
    field_names = [f.name for f in schema.fields]
    padded = [{name: r.get(name) for name in field_names} for r in rows]
    return spark.createDataFrame(padded, schema=schema)


# ============================================================
# End-to-end tests for add_diedInVisit against real claim schemas
# ============================================================

class TestAddDiedInVisitEndToEnd:

    def test_ip_base_real_schema(self, spark):
        from cms.base import add_diedInVisit
        rows = [
            {"DSYSRTKY": 1, "CLAIMNO": 101, "STUS_CD": 20},   # expired -> 1
            {"DSYSRTKY": 2, "CLAIMNO": 102, "STUS_CD": 1},    # home -> 0
            {"DSYSRTKY": 3, "CLAIMNO": 103, "STUS_CD": 20},   # expired -> 1
            {"DSYSRTKY": 4, "CLAIMNO": 104, "STUS_CD": 3},    # SNF -> 0
            {"DSYSRTKY": 5, "CLAIMNO": 105, "STUS_CD": None}, # NULL -> NULL
            {"DSYSRTKY": 6, "CLAIMNO": 106, "STUS_CD": 2},    # transfer -> 0
        ]
        df = make_real_claim_df(spark, "ipBase", rows)
        result_df = add_diedInVisit(df)

        by_claim = {r["CLAIMNO"]: r["diedInVisit"] for r in result_df.collect()}
        assert by_claim[101] == 1
        assert by_claim[102] == 0
        assert by_claim[103] == 1
        assert by_claim[104] == 0
        assert by_claim[105] is None
        assert by_claim[106] == 0

    def test_ip_base_preserves_original_columns(self, spark):
        from cms.base import add_diedInVisit
        from cms.schemas import schemas
        rows = [{"DSYSRTKY": 1, "CLAIMNO": 101, "STUS_CD": 20}]
        df = make_real_claim_df(spark, "ipBase", rows)
        result_df = add_diedInVisit(df)
        # All original ipBase fields are still present.
        original_fields = [f.name for f in schemas["ipBase"].fields]
        for f in original_fields:
            assert f in result_df.columns, f"original column {f} dropped"
        assert "diedInVisit" in result_df.columns

    @pytest.mark.parametrize("claim_type", ["opBase", "ipBase", "snfBase", "hhaBase", "hospBase"])
    def test_all_base_claim_types_with_stus_cd(self, spark, claim_type):
        # add_diedInVisit is generic and should work on any base claim schema
        # that has STUS_CD.
        from cms.base import add_diedInVisit
        rows = [
            {"DSYSRTKY": 1, "CLAIMNO": 1, "STUS_CD": 20},
            {"DSYSRTKY": 2, "CLAIMNO": 2, "STUS_CD": 1},
            {"DSYSRTKY": 3, "CLAIMNO": 3, "STUS_CD": None},
        ]
        df = make_real_claim_df(spark, claim_type, rows)
        result_df = add_diedInVisit(df)
        rows_out = sorted(result_df.collect(), key=lambda r: r["CLAIMNO"])
        assert rows_out[0]["diedInVisit"] == 1
        assert rows_out[1]["diedInVisit"] == 0
        assert rows_out[2]["diedInVisit"] is None

    def test_mortality_rate_from_real_ip_claims(self, spark):
        # End-to-end sanity check: in a 10-claim cohort with 3 deaths
        # (STUS_CD=20), the sum of diedInVisit should be 3.
        from cms.base import add_diedInVisit
        rows = [
            {"DSYSRTKY": i, "CLAIMNO": i, "STUS_CD": stus}
            for i, stus in enumerate([1, 20, 3, 20, 1, 4, 6, 20, 1, 50], start=1)
        ]
        df = make_real_claim_df(spark, "ipBase", rows)
        result_df = add_diedInVisit(df)
        total_deaths = result_df.agg(F.sum("diedInVisit")).collect()[0][0]
        assert total_deaths == 3
        total_claims = result_df.count()
        assert total_claims == 10


# ============================================================
# Helper: compute the absolute day-number that
# add_admission_date_info / add_death_date_info would produce for a
# YYYYMMDD integer. Used to construct DEATH_DT_DAY values consistent
# with the corresponding ADMSN_DT in end-to-end tests.
# ============================================================

def _real_day_number(date_int):
    import datetime
    from utilities import daysInYearsPriorDict
    s = str(date_int)
    year, month, day = int(s[:4]), int(s[4:6]), int(s[6:8])
    day_of_year = datetime.date(year, month, day).timetuple().tm_yday
    return daysInYearsPriorDict[year] + day_of_year


# ============================================================
# End-to-end tests for add_30DaysAfterAdmissionDateDead
# ============================================================

class TestAdd30DaysAfterAdmissionDateDead:

    def test_pipeline_with_real_ip_schema(self, spark):
        # Full pipeline: real ipBase schema -> add_admission_date_info ->
        # simulated MBSF join supplying DEATH_DT_DAY ->
        # add_daysDeadAfterAdmissionDate -> add_30DaysAfterAdmissionDateDead.
        from cms.base import (
            add_admission_date_info,
            add_daysDeadAfterAdmissionDate,
            add_30DaysAfterAdmissionDateDead,
        )
        rows = [
            {"DSYSRTKY": 1, "CLAIMNO": 101, "ADMSN_DT": 20200115},  # death same day -> 1
            {"DSYSRTKY": 2, "CLAIMNO": 102, "ADMSN_DT": 20200115},  # death + 1 day  -> 1
            {"DSYSRTKY": 3, "CLAIMNO": 103, "ADMSN_DT": 20200115},  # death + 30 days (boundary) -> 1
            {"DSYSRTKY": 4, "CLAIMNO": 104, "ADMSN_DT": 20200115},  # death + 31 days -> 0
            {"DSYSRTKY": 5, "CLAIMNO": 105, "ADMSN_DT": 20200115},  # death + 90 days -> 0
            {"DSYSRTKY": 6, "CLAIMNO": 106, "ADMSN_DT": 20200115},  # alive (DEATH_DT_DAY null) -> 0
        ]
        df = make_real_claim_df(spark, "ipBase", rows)
        df = add_admission_date_info(df, "ip")

        deaths = [
            {"DSYSRTKY": 1, "DEATH_DT_DAY": _real_day_number(20200115)},
            {"DSYSRTKY": 2, "DEATH_DT_DAY": _real_day_number(20200116)},
            {"DSYSRTKY": 3, "DEATH_DT_DAY": _real_day_number(20200214)},
            {"DSYSRTKY": 4, "DEATH_DT_DAY": _real_day_number(20200215)},
            {"DSYSRTKY": 5, "DEATH_DT_DAY": _real_day_number(20200414)},
            {"DSYSRTKY": 6, "DEATH_DT_DAY": None},
        ]
        deaths_schema = StructType([
            StructField("DSYSRTKY", IntegerType(), True),
            StructField("DEATH_DT_DAY", IntegerType(), True),
        ])
        deaths_df = spark.createDataFrame(deaths, schema=deaths_schema)
        df = df.join(deaths_df, on="DSYSRTKY", how="left")

        df = add_daysDeadAfterAdmissionDate(df)
        df = add_30DaysAfterAdmissionDateDead(df)

        by_claim = {r["CLAIMNO"]: r["30DaysAfterAdmissionDateDead"] for r in df.collect()}
        assert by_claim[101] == 1
        assert by_claim[102] == 1
        assert by_claim[103] == 1
        assert by_claim[104] == 0
        assert by_claim[105] == 0
        assert by_claim[106] == 0

    def test_boundary_day_30_is_inclusive(self, spark):
        # The check is <= 30, so exactly 30 days after admission is still flagged.
        from cms.base import add_daysDeadAfterAdmissionDate, add_30DaysAfterAdmissionDateDead
        rows = [
            {"DSYSRTKY": 1, "CLAIMNO": 1, "ADMSN_DT_DAY": 1000, "DEATH_DT_DAY": 1030},  # 30 days
            {"DSYSRTKY": 2, "CLAIMNO": 2, "ADMSN_DT_DAY": 1000, "DEATH_DT_DAY": 1031},  # 31 days
        ]
        schema = StructType([
            StructField("DSYSRTKY", IntegerType(), True),
            StructField("CLAIMNO", IntegerType(), True),
            StructField("ADMSN_DT_DAY", IntegerType(), True),
            StructField("DEATH_DT_DAY", IntegerType(), True),
        ])
        df = spark.createDataFrame(rows, schema=schema)
        df = add_daysDeadAfterAdmissionDate(df)
        df = add_30DaysAfterAdmissionDateDead(df)
        out = {r["CLAIMNO"]: r["30DaysAfterAdmissionDateDead"] for r in df.collect()}
        assert out[1] == 1
        assert out[2] == 0

    def test_alive_patient_returns_0(self, spark):
        # DEATH_DT_DAY null -> daysDeadAfterAdmissionDate null -> .otherwise(0).
        from cms.base import add_daysDeadAfterAdmissionDate, add_30DaysAfterAdmissionDateDead
        schema = StructType([
            StructField("DSYSRTKY", IntegerType(), True),
            StructField("CLAIMNO", IntegerType(), True),
            StructField("ADMSN_DT_DAY", IntegerType(), True),
            StructField("DEATH_DT_DAY", IntegerType(), True),
        ])
        df = spark.createDataFrame(
            [{"DSYSRTKY": 1, "CLAIMNO": 1, "ADMSN_DT_DAY": 1000, "DEATH_DT_DAY": None}],
            schema=schema,
        )
        df = add_daysDeadAfterAdmissionDate(df)
        df = add_30DaysAfterAdmissionDateDead(df)
        row = df.collect()[0]
        assert row["daysDeadAfterAdmissionDate"] is None
        assert row["30DaysAfterAdmissionDateDead"] == 0

    def test_death_on_admission_day_returns_1(self, spark):
        # 0 days after admission still counts as 30-day mortality.
        from cms.base import add_daysDeadAfterAdmissionDate, add_30DaysAfterAdmissionDateDead
        schema = StructType([
            StructField("DSYSRTKY", IntegerType(), True),
            StructField("CLAIMNO", IntegerType(), True),
            StructField("ADMSN_DT_DAY", IntegerType(), True),
            StructField("DEATH_DT_DAY", IntegerType(), True),
        ])
        df = spark.createDataFrame(
            [{"DSYSRTKY": 1, "CLAIMNO": 1, "ADMSN_DT_DAY": 1000, "DEATH_DT_DAY": 1000}],
            schema=schema,
        )
        df = add_daysDeadAfterAdmissionDate(df)
        df = add_30DaysAfterAdmissionDateDead(df)
        row = df.collect()[0]
        assert row["daysDeadAfterAdmissionDate"] == 0
        assert row["30DaysAfterAdmissionDateDead"] == 1

    def test_30day_mortality_rate_real_ip_cohort(self, spark):
        # End-to-end aggregate check: 10-claim ipBase cohort, 4 deaths within 30
        # days of admission (claims 1, 3, 6, 9 die at +0, +5, +30, +12 respectively;
        # claims 2 and 7 die at +45 and +200; the rest are alive).
        from cms.base import (
            add_admission_date_info,
            add_daysDeadAfterAdmissionDate,
            add_30DaysAfterAdmissionDateDead,
        )
        admsn = 20200101
        rows = [{"DSYSRTKY": i, "CLAIMNO": i, "ADMSN_DT": admsn} for i in range(1, 11)]
        df = make_real_claim_df(spark, "ipBase", rows)
        df = add_admission_date_info(df, "ip")

        base_day = _real_day_number(admsn)
        death_offsets = {1: 0, 2: 45, 3: 5, 6: 30, 7: 200, 9: 12}  # others alive
        deaths = []
        for i in range(1, 11):
            deaths.append({
                "DSYSRTKY": i,
                "DEATH_DT_DAY": (base_day + death_offsets[i]) if i in death_offsets else None,
            })
        deaths_schema = StructType([
            StructField("DSYSRTKY", IntegerType(), True),
            StructField("DEATH_DT_DAY", IntegerType(), True),
        ])
        df = df.join(spark.createDataFrame(deaths, schema=deaths_schema), on="DSYSRTKY", how="left")

        df = add_daysDeadAfterAdmissionDate(df)
        df = add_30DaysAfterAdmissionDateDead(df)

        total_30day_deaths = df.agg(F.sum("30DaysAfterAdmissionDateDead")).collect()[0][0]
        assert total_30day_deaths == 4  # claims 1, 3, 6, 9
        assert df.count() == 10


# ============================================================
# Shared pipeline helpers for the remaining death-related functions.
# Each helper builds a real-schema ipBase DF, runs the date-info function
# to produce the *_DT_DAY column, simulates the MBSF death join, and
# returns the DF after the corresponding add_daysDead* call.
# ============================================================

_DEATHS_SCHEMA = StructType([
    StructField("DSYSRTKY", IntegerType(), True),
    StructField("DEATH_DT_DAY", IntegerType(), True),
])


def _setup_admission_pipeline(spark, claim_death_offsets, admsn=20200115):
    """Build a real ipBase DF (one row per claim), run add_admission_date_info,
    simulate the MBSF death join, and run add_daysDeadAfterAdmissionDate.

    claim_death_offsets: dict {CLAIMNO: offset-in-days-from-admission or None for alive}.
    """
    from cms.base import add_admission_date_info, add_daysDeadAfterAdmissionDate
    base_day = _real_day_number(admsn)
    rows = [{"DSYSRTKY": cn, "CLAIMNO": cn, "ADMSN_DT": admsn}
            for cn in claim_death_offsets]
    df = make_real_claim_df(spark, "ipBase", rows)
    df = add_admission_date_info(df, "ip")
    deaths = [{"DSYSRTKY": cn,
               "DEATH_DT_DAY": (base_day + off) if off is not None else None}
              for cn, off in claim_death_offsets.items()]
    deaths_df = spark.createDataFrame(deaths, schema=_DEATHS_SCHEMA)
    df = df.join(deaths_df, on="DSYSRTKY", how="left")
    df = add_daysDeadAfterAdmissionDate(df)
    return df


def _setup_through_pipeline(spark, claim_death_offsets, thru=20200115):
    """Same shape as _setup_admission_pipeline but for THRU_DT-based mortality.
    Uses cms.utilities.add_through_date_info to build THRU_DT_DAY."""
    from cms.utilities import add_through_date_info
    from cms.base import add_daysDeadAfterThroughDate
    base_day = _real_day_number(thru)
    rows = [{"DSYSRTKY": cn, "CLAIMNO": cn, "THRU_DT": thru}
            for cn in claim_death_offsets]
    df = make_real_claim_df(spark, "ipBase", rows)
    df = add_through_date_info(df)
    deaths = [{"DSYSRTKY": cn,
               "DEATH_DT_DAY": (base_day + off) if off is not None else None}
              for cn, off in claim_death_offsets.items()]
    deaths_df = spark.createDataFrame(deaths, schema=_DEATHS_SCHEMA)
    df = df.join(deaths_df, on="DSYSRTKY", how="left")
    df = add_daysDeadAfterThroughDate(df)
    return df


# ============================================================
# Tests for add_daysDeadAfterAdmissionDate
# ============================================================

class TestAddDaysDeadAfterAdmissionDate:

    def test_computes_offset_end_to_end(self, spark):
        df = _setup_admission_pipeline(spark, {1: 0, 2: 5, 3: 30, 4: 100, 5: None})
        out = {r["CLAIMNO"]: r["daysDeadAfterAdmissionDate"] for r in df.collect()}
        assert out[1] == 0
        assert out[2] == 5
        assert out[3] == 30
        assert out[4] == 100
        assert out[5] is None

    def test_negative_when_death_precedes_admission(self, spark):
        # In production filter_visits_in_living_period would drop such rows;
        # the function itself simply subtracts.
        df = _setup_admission_pipeline(spark, {1: -3})
        out = df.collect()[0]
        assert out["daysDeadAfterAdmissionDate"] == -3


# ============================================================
# Tests for add_daysDeadAfterThroughDate
# ============================================================

class TestAddDaysDeadAfterThroughDate:

    def test_computes_offset_end_to_end(self, spark):
        df = _setup_through_pipeline(spark, {1: 0, 2: 5, 3: 90, 4: 200, 5: None})
        out = {r["CLAIMNO"]: r["daysDeadAfterThroughDate"] for r in df.collect()}
        assert out[1] == 0
        assert out[2] == 5
        assert out[3] == 90
        assert out[4] == 200
        assert out[5] is None

    def test_negative_when_death_precedes_through(self, spark):
        df = _setup_through_pipeline(spark, {1: -1})
        out = df.collect()[0]
        assert out["daysDeadAfterThroughDate"] == -1


# ============================================================
# Tests for add_90DaysAfterAdmissionDateDead
# ============================================================

class TestAdd90DaysAfterAdmissionDateDead:

    def test_pipeline_with_real_ip_schema(self, spark):
        from cms.base import add_90DaysAfterAdmissionDateDead
        df = _setup_admission_pipeline(spark, {
            1: 0,     # 1
            2: 30,    # 1
            3: 89,    # 1
            4: 90,    # 1 (boundary)
            5: 91,    # 0
            6: 200,   # 0
            7: None,  # 0
        })
        df = add_90DaysAfterAdmissionDateDead(df)
        out = {r["CLAIMNO"]: r["90DaysAfterAdmissionDateDead"] for r in df.collect()}
        assert out == {1: 1, 2: 1, 3: 1, 4: 1, 5: 0, 6: 0, 7: 0}

    def test_boundary_day_90_is_inclusive(self, spark):
        from cms.base import add_90DaysAfterAdmissionDateDead
        df = _setup_admission_pipeline(spark, {1: 90, 2: 91})
        df = add_90DaysAfterAdmissionDateDead(df)
        out = {r["CLAIMNO"]: r["90DaysAfterAdmissionDateDead"] for r in df.collect()}
        assert out == {1: 1, 2: 0}

    def test_alive_returns_0(self, spark):
        from cms.base import add_90DaysAfterAdmissionDateDead
        df = _setup_admission_pipeline(spark, {1: None})
        df = add_90DaysAfterAdmissionDateDead(df)
        assert df.collect()[0]["90DaysAfterAdmissionDateDead"] == 0


# ============================================================
# Tests for add_365DaysAfterAdmissionDateDead
# ============================================================

class TestAdd365DaysAfterAdmissionDateDead:

    def test_pipeline_with_real_ip_schema(self, spark):
        from cms.base import add_365DaysAfterAdmissionDateDead
        df = _setup_admission_pipeline(spark, {
            1: 0,     # 1
            2: 90,    # 1
            3: 200,   # 1
            4: 365,   # 1 (boundary)
            5: 366,   # 0
            6: 500,   # 0
            7: None,  # 0
        })
        df = add_365DaysAfterAdmissionDateDead(df)
        out = {r["CLAIMNO"]: r["365DaysAfterAdmissionDateDead"] for r in df.collect()}
        assert out == {1: 1, 2: 1, 3: 1, 4: 1, 5: 0, 6: 0, 7: 0}

    def test_boundary_day_365_is_inclusive(self, spark):
        from cms.base import add_365DaysAfterAdmissionDateDead
        df = _setup_admission_pipeline(spark, {1: 365, 2: 366})
        df = add_365DaysAfterAdmissionDateDead(df)
        out = {r["CLAIMNO"]: r["365DaysAfterAdmissionDateDead"] for r in df.collect()}
        assert out == {1: 1, 2: 0}

    def test_alive_returns_0(self, spark):
        from cms.base import add_365DaysAfterAdmissionDateDead
        df = _setup_admission_pipeline(spark, {1: None})
        df = add_365DaysAfterAdmissionDateDead(df)
        assert df.collect()[0]["365DaysAfterAdmissionDateDead"] == 0


# ============================================================
# Tests for add_90DaysAfterThroughDateDead
# ============================================================

class TestAdd90DaysAfterThroughDateDead:

    def test_pipeline_with_real_ip_schema(self, spark):
        from cms.base import add_90DaysAfterThroughDateDead
        df = _setup_through_pipeline(spark, {
            1: 0,     # 1
            2: 30,    # 1
            3: 89,    # 1
            4: 90,    # 1 (boundary)
            5: 91,    # 0
            6: 200,   # 0
            7: None,  # 0
        })
        df = add_90DaysAfterThroughDateDead(df)
        out = {r["CLAIMNO"]: r["90DaysAfterThroughDateDead"] for r in df.collect()}
        assert out == {1: 1, 2: 1, 3: 1, 4: 1, 5: 0, 6: 0, 7: 0}

    def test_boundary_day_90_is_inclusive(self, spark):
        from cms.base import add_90DaysAfterThroughDateDead
        df = _setup_through_pipeline(spark, {1: 90, 2: 91})
        df = add_90DaysAfterThroughDateDead(df)
        out = {r["CLAIMNO"]: r["90DaysAfterThroughDateDead"] for r in df.collect()}
        assert out == {1: 1, 2: 0}

    def test_alive_returns_0(self, spark):
        from cms.base import add_90DaysAfterThroughDateDead
        df = _setup_through_pipeline(spark, {1: None})
        df = add_90DaysAfterThroughDateDead(df)
        assert df.collect()[0]["90DaysAfterThroughDateDead"] == 0


# ============================================================
# Tests for add_365DaysAfterThroughDateDead
# ============================================================

class TestAdd365DaysAfterThroughDateDead:

    def test_pipeline_with_real_ip_schema(self, spark):
        from cms.base import add_365DaysAfterThroughDateDead
        df = _setup_through_pipeline(spark, {
            1: 0,     # 1
            2: 90,    # 1
            3: 200,   # 1
            4: 365,   # 1 (boundary)
            5: 366,   # 0
            6: 500,   # 0
            7: None,  # 0
        })
        df = add_365DaysAfterThroughDateDead(df)
        out = {r["CLAIMNO"]: r["365DaysAfterThroughDateDead"] for r in df.collect()}
        assert out == {1: 1, 2: 1, 3: 1, 4: 1, 5: 0, 6: 0, 7: 0}

    def test_boundary_day_365_is_inclusive(self, spark):
        from cms.base import add_365DaysAfterThroughDateDead
        df = _setup_through_pipeline(spark, {1: 365, 2: 366})
        df = add_365DaysAfterThroughDateDead(df)
        out = {r["CLAIMNO"]: r["365DaysAfterThroughDateDead"] for r in df.collect()}
        assert out == {1: 1, 2: 0}

    def test_alive_returns_0(self, spark):
        from cms.base import add_365DaysAfterThroughDateDead
        df = _setup_through_pipeline(spark, {1: None})
        df = add_365DaysAfterThroughDateDead(df)
        assert df.collect()[0]["365DaysAfterThroughDateDead"] == 0


# ============================================================
# Cross-function consistency: 30 -> 90 -> 365 (admission-based)
# and 90 -> 365 (through-based). If a claim is flagged at horizon X,
# it must also be flagged at every larger horizon.
# ============================================================

class TestMortalityFlagConsistency:

    def test_admission_flags_are_monotone(self, spark):
        from cms.base import (
            add_30DaysAfterAdmissionDateDead,
            add_90DaysAfterAdmissionDateDead,
            add_365DaysAfterAdmissionDateDead,
        )
        df = _setup_admission_pipeline(spark, {1: 15, 2: 60, 3: 200, 4: 500, 5: None})
        df = add_30DaysAfterAdmissionDateDead(df)
        df = add_90DaysAfterAdmissionDateDead(df)
        df = add_365DaysAfterAdmissionDateDead(df)
        for r in df.collect():
            assert r["30DaysAfterAdmissionDateDead"] <= r["90DaysAfterAdmissionDateDead"]
            assert r["90DaysAfterAdmissionDateDead"] <= r["365DaysAfterAdmissionDateDead"]

    def test_through_flags_are_monotone(self, spark):
        from cms.base import (
            add_90DaysAfterThroughDateDead,
            add_365DaysAfterThroughDateDead,
        )
        df = _setup_through_pipeline(spark, {1: 15, 2: 60, 3: 200, 4: 500, 5: None})
        df = add_90DaysAfterThroughDateDead(df)
        df = add_365DaysAfterThroughDateDead(df)
        for r in df.collect():
            assert r["90DaysAfterThroughDateDead"] <= r["365DaysAfterThroughDateDead"]


# ============================================================
# End-to-end tests for add_prior_hospitalization_info
#
# add_prior_hospitalization_info(baseDF, ipBaseDF) consumes derived columns:
#   baseDF   -> DSYSRTKY, CLAIMNO, ADMSN_DT_DAY, ADMSN_DT_MONTH, ffsFirstMonth
#   ipBaseDF -> DSYSRTKY, THRU_DT_DAY
# These tests build both DataFrames from the real cms.schemas ipBase schema and run
# the actual date-enrichment pipeline that produces those columns
# (base.add_admission_date_info for the index stays, utilities.add_through_date_info
# for the prior-history stays), simulating only the MBSF-supplied ffsFirstMonth via a
# join -- the same end-to-end approach used by the add_30DaysAfterAdmissionDateDead
# tests above.
# ============================================================

def _real_month_number(date_int):
    """Absolute month number add_admission_date_info would produce for a YYYYMMDD int
    (monthsInYearsPrior[year] + month-of-year). Mirrors _real_day_number for months so
    ffsFirstMonth values can be set relative to a claim's real ADMSN_DT_MONTH."""
    from utilities import monthsInYearsPriorDict
    s = str(date_int)
    year, month = int(s[:4]), int(s[4:6])
    return monthsInYearsPriorDict[year] + month


def _ffs_first_month(admsn_dt, coverage_months):
    """ffsFirstMonth value giving exactly `coverage_months` of FFS coverage before admsn_dt,
    i.e. ADMSN_DT_MONTH - ffsFirstMonth == coverage_months."""
    return _real_month_number(admsn_dt) - coverage_months


_PRIOR_HOSP_FFS_SCHEMA = StructType([
    StructField("DSYSRTKY", IntegerType(), True),
    StructField("ffsFirstMonth", IntegerType(), True),
])


def _setup_prior_hospitalization_dfs(spark, index_claims, prior_throughs):
    """Build the (baseDF, ipBaseDF) pair add_prior_hospitalization_info expects, end-to-end.

    index_claims: list of dicts {DSYSRTKY, CLAIMNO, ADMSN_DT, ffsFirstMonth} -- the index
        (receiving) stays. Built from the real ipBase schema, run through
        add_admission_date_info (producing ADMSN_DT_DAY/ADMSN_DT_MONTH), then joined to a
        simulated MBSF frame supplying ffsFirstMonth. ffsFirstMonth is a per-beneficiary
        value (real MBSF gives one first-FFS-month per DSYSRTKY), so claims sharing a
        DSYSRTKY must agree on it; the simulated frame is deduped to one row per beneficiary.
    prior_throughs: list of dicts {DSYSRTKY, THRU_DT} -- one row per prior inpatient stay.
        Built from the real ipBase schema, run through add_through_date_info (producing
        THRU_DT_DAY).
    """
    from cms.base import add_admission_date_info
    from cms.utilities import add_through_date_info

    base_rows = [{"DSYSRTKY": c["DSYSRTKY"], "CLAIMNO": c["CLAIMNO"], "ADMSN_DT": c["ADMSN_DT"]}
                 for c in index_claims]
    baseDF = make_real_claim_df(spark, "ipBase", base_rows)
    baseDF = add_admission_date_info(baseDF, "ip")

    # One ffsFirstMonth per beneficiary; conflicting values for the same DSYSRTKY are a
    # test-authoring error (real MBSF supplies a single first-FFS-month per beneficiary).
    ffs_by_bene = {}
    for c in index_claims:
        prev = ffs_by_bene.setdefault(c["DSYSRTKY"], c["ffsFirstMonth"])
        assert prev == c["ffsFirstMonth"], \
            f"conflicting ffsFirstMonth for DSYSRTKY {c['DSYSRTKY']}"
    ffs_rows = [{"DSYSRTKY": k, "ffsFirstMonth": v} for k, v in ffs_by_bene.items()]
    ffsDF = spark.createDataFrame(ffs_rows, schema=_PRIOR_HOSP_FFS_SCHEMA)
    baseDF = baseDF.join(ffsDF, on="DSYSRTKY", how="left_outer")

    ip_rows = [{"DSYSRTKY": p["DSYSRTKY"], "THRU_DT": p["THRU_DT"]} for p in prior_throughs]
    ipBaseDF = make_real_claim_df(spark, "ipBase", ip_rows)
    ipBaseDF = add_through_date_info(ipBaseDF)

    return baseDF, ipBaseDF


class TestAddPriorHospitalizationInfoEndToEnd:

    def test_counts_window_boundaries_real_pipeline(self, spark):
        # One beneficiary, index admission 2021-06-01, plenty of FFS coverage.
        # Prior ip THRU dates chosen at exact day-difference boundaries:
        #   2021-06-01 (diff 0)   -> excluded (must be >= 1 day before admission)
        #   2021-05-02 (diff 30)  -> 6mo + 12mo
        #   2021-02-21 (diff 100) -> 6mo + 12mo
        #   2020-12-01 (diff 182) -> 6mo + 12mo  (6-month upper boundary, inclusive)
        #   2020-11-30 (diff 183) -> 12mo only
        #   2020-06-01 (diff 365) -> 12mo only   (12-month upper boundary, inclusive)
        #   2020-05-31 (diff 366) -> excluded
        # Expected: hospitalizationsIn12Months == 5, hospitalizationsIn6Months == 3.
        from cms.base import add_prior_hospitalization_info
        admsn = 20210601
        index_claims = [{"DSYSRTKY": 1, "CLAIMNO": 101, "ADMSN_DT": admsn,
                         "ffsFirstMonth": _ffs_first_month(admsn, 24)}]
        prior_throughs = [
            {"DSYSRTKY": 1, "THRU_DT": 20210601},  # diff 0   -> excluded
            {"DSYSRTKY": 1, "THRU_DT": 20210502},  # diff 30  -> 6 + 12
            {"DSYSRTKY": 1, "THRU_DT": 20210221},  # diff 100 -> 6 + 12
            {"DSYSRTKY": 1, "THRU_DT": 20201201},  # diff 182 -> 6 + 12
            {"DSYSRTKY": 1, "THRU_DT": 20201130},  # diff 183 -> 12 only
            {"DSYSRTKY": 1, "THRU_DT": 20200601},  # diff 365 -> 12 only
            {"DSYSRTKY": 1, "THRU_DT": 20200531},  # diff 366 -> excluded
        ]
        baseDF, ipBaseDF = _setup_prior_hospitalization_dfs(spark, index_claims, prior_throughs)
        r = add_prior_hospitalization_info(baseDF, ipBaseDF).collect()[0]
        assert r["hospitalizationsIn12Months"] == 5
        assert r["hospitalizationsIn6Months"] == 3
        assert r["hospitalizedIn12Months"] == 1
        assert r["hospitalizedIn6Months"] == 1

    def test_no_prior_hospitalizations_returns_zero(self, spark):
        # Beneficiary with full coverage but no qualifying prior ip stay: counts are 0
        # (left_outer miss -> fillna(0)), flags are 0 -- not null.
        from cms.base import add_prior_hospitalization_info
        admsn = 20210601
        index_claims = [{"DSYSRTKY": 1, "CLAIMNO": 101, "ADMSN_DT": admsn,
                         "ffsFirstMonth": _ffs_first_month(admsn, 24)}]
        # Only an ip stay AFTER the admission (diff < 1) -> never qualifies.
        prior_throughs = [{"DSYSRTKY": 1, "THRU_DT": 20210701}]
        baseDF, ipBaseDF = _setup_prior_hospitalization_dfs(spark, index_claims, prior_throughs)
        r = add_prior_hospitalization_info(baseDF, ipBaseDF).collect()[0]
        assert r["hospitalizationsIn12Months"] == 0
        assert r["hospitalizationsIn6Months"] == 0
        assert r["hospitalizedIn12Months"] == 0
        assert r["hospitalizedIn6Months"] == 0

    def test_coverage_below_6_months_nulls_both_windows(self, spark):
        # ADMSN_DT_MONTH - ffsFirstMonth == 3 (< 6 and < 12): both lookbacks are null
        # even though a qualifying prior stay exists.
        from cms.base import add_prior_hospitalization_info
        admsn = 20210601
        index_claims = [{"DSYSRTKY": 1, "CLAIMNO": 101, "ADMSN_DT": admsn,
                         "ffsFirstMonth": _ffs_first_month(admsn, 3)}]
        prior_throughs = [{"DSYSRTKY": 1, "THRU_DT": 20210221}]  # diff 100, would be 1/1
        baseDF, ipBaseDF = _setup_prior_hospitalization_dfs(spark, index_claims, prior_throughs)
        r = add_prior_hospitalization_info(baseDF, ipBaseDF).collect()[0]
        assert r["hospitalizationsIn12Months"] is None
        assert r["hospitalizationsIn6Months"] is None
        assert r["hospitalizedIn12Months"] is None
        assert r["hospitalizedIn6Months"] is None

    def test_coverage_between_6_and_12_months_nulls_only_12_month(self, spark):
        # ADMSN_DT_MONTH - ffsFirstMonth == 8 (>= 6 but < 12): the 6-month window is kept
        # while the 12-month window is nulled. This exercises the per-window FFS logic
        # independently (the two windows now share one aggregation but keep separate nulling).
        from cms.base import add_prior_hospitalization_info
        admsn = 20210601
        index_claims = [{"DSYSRTKY": 1, "CLAIMNO": 101, "ADMSN_DT": admsn,
                         "ffsFirstMonth": _ffs_first_month(admsn, 8)}]
        prior_throughs = [{"DSYSRTKY": 1, "THRU_DT": 20210221}]  # diff 100 -> within 6mo
        baseDF, ipBaseDF = _setup_prior_hospitalization_dfs(spark, index_claims, prior_throughs)
        r = add_prior_hospitalization_info(baseDF, ipBaseDF).collect()[0]
        assert r["hospitalizationsIn12Months"] is None
        assert r["hospitalizedIn12Months"] is None
        assert r["hospitalizationsIn6Months"] == 1
        assert r["hospitalizedIn6Months"] == 1

    def test_ffs_coverage_exact_boundaries(self, spark):
        # Coverage exactly 12 months (not < 12) -> 12mo kept; coverage exactly 6 months
        # (not < 6, but < 12) -> 6mo kept, 12mo nulled. Both have one prior stay at diff 100.
        from cms.base import add_prior_hospitalization_info
        admsn = 20210601
        index_claims = [
            {"DSYSRTKY": 1, "CLAIMNO": 101, "ADMSN_DT": admsn,
             "ffsFirstMonth": _ffs_first_month(admsn, 12)},  # exactly 12 months
            {"DSYSRTKY": 2, "CLAIMNO": 102, "ADMSN_DT": admsn,
             "ffsFirstMonth": _ffs_first_month(admsn, 6)},   # exactly 6 months
        ]
        prior_throughs = [
            {"DSYSRTKY": 1, "THRU_DT": 20210221},  # diff 100
            {"DSYSRTKY": 2, "THRU_DT": 20210221},  # diff 100
        ]
        baseDF, ipBaseDF = _setup_prior_hospitalization_dfs(spark, index_claims, prior_throughs)
        by_claim = {r["CLAIMNO"]: r for r in
                    add_prior_hospitalization_info(baseDF, ipBaseDF).collect()}
        # exactly-12-months beneficiary: both windows kept
        assert by_claim[101]["hospitalizationsIn12Months"] == 1
        assert by_claim[101]["hospitalizationsIn6Months"] == 1
        # exactly-6-months beneficiary: 12mo nulled, 6mo kept
        assert by_claim[102]["hospitalizationsIn12Months"] is None
        assert by_claim[102]["hospitalizationsIn6Months"] == 1

    def test_multiple_beneficiaries_counted_independently(self, spark):
        # Three beneficiaries in one run; each beneficiary's prior stays must not leak into
        # another's count (partition by DSYSRTKY/ADMSN_DT_DAY/CLAIMNO).
        from cms.base import add_prior_hospitalization_info
        admsn = 20210601
        ffs = _ffs_first_month(admsn, 24)
        index_claims = [
            {"DSYSRTKY": 1, "CLAIMNO": 101, "ADMSN_DT": admsn, "ffsFirstMonth": ffs},
            {"DSYSRTKY": 2, "CLAIMNO": 102, "ADMSN_DT": admsn, "ffsFirstMonth": ffs},
            {"DSYSRTKY": 3, "CLAIMNO": 103, "ADMSN_DT": admsn, "ffsFirstMonth": ffs},
        ]
        prior_throughs = [
            {"DSYSRTKY": 1, "THRU_DT": 20210502},  # diff 30  -> 6 + 12
            {"DSYSRTKY": 1, "THRU_DT": 20201201},  # diff 182 -> 6 + 12
            {"DSYSRTKY": 1, "THRU_DT": 20201130},  # diff 183 -> 12 only
            {"DSYSRTKY": 2, "THRU_DT": 20200601},  # diff 365 -> 12 only
            # beneficiary 3 has no prior stays
        ]
        baseDF, ipBaseDF = _setup_prior_hospitalization_dfs(spark, index_claims, prior_throughs)
        by_claim = {r["CLAIMNO"]: r for r in
                    add_prior_hospitalization_info(baseDF, ipBaseDF).collect()}
        assert by_claim[101]["hospitalizationsIn12Months"] == 3
        assert by_claim[101]["hospitalizationsIn6Months"] == 2
        assert by_claim[102]["hospitalizationsIn12Months"] == 1
        assert by_claim[102]["hospitalizationsIn6Months"] == 0
        assert by_claim[102]["hospitalizedIn6Months"] == 0
        assert by_claim[103]["hospitalizationsIn12Months"] == 0
        assert by_claim[103]["hospitalizationsIn6Months"] == 0

    def test_same_beneficiary_index_claims_partitioned_by_admission(self, spark):
        # One beneficiary with two index stays at different admission dates. Each index claim
        # must count only stays in its OWN look-back window, from the shared prior-stay pool.
        from cms.base import add_prior_hospitalization_info
        # ffsFirstMonth is per beneficiary: anchor it 24 months before the EARLIER admission
        # so both index stays have >= 12 months of coverage (the later stay gets even more).
        shared_ffs = _ffs_first_month(20200601, 24)
        index_claims = [
            {"DSYSRTKY": 1, "CLAIMNO": 101, "ADMSN_DT": 20210601, "ffsFirstMonth": shared_ffs},
            {"DSYSRTKY": 1, "CLAIMNO": 111, "ADMSN_DT": 20200601, "ffsFirstMonth": shared_ffs},
        ]
        prior_throughs = [
            # diff 30 before 2021-06-01; ~382 days before 2020-06-01 admission would be
            # negative (after it) -> only counts for claim 101.
            {"DSYSRTKY": 1, "THRU_DT": 20210502},
            # diff 17 before 2020-06-01; ~382 days before 2021-06-01 (outside 365)
            # -> only counts for claim 111.
            {"DSYSRTKY": 1, "THRU_DT": 20200515},
        ]
        baseDF, ipBaseDF = _setup_prior_hospitalization_dfs(spark, index_claims, prior_throughs)
        by_claim = {r["CLAIMNO"]: r for r in
                    add_prior_hospitalization_info(baseDF, ipBaseDF).collect()}
        assert by_claim[101]["hospitalizationsIn12Months"] == 1
        assert by_claim[101]["hospitalizationsIn6Months"] == 1
        assert by_claim[111]["hospitalizationsIn12Months"] == 1
        assert by_claim[111]["hospitalizationsIn6Months"] == 1

    def test_output_columns_added_and_originals_preserved(self, spark):
        # The four documented output columns are added and every original ipBase field
        # (plus the pipeline-derived columns) survives the two joins.
        from cms.base import add_prior_hospitalization_info
        from cms.schemas import schemas
        admsn = 20210601
        index_claims = [{"DSYSRTKY": 1, "CLAIMNO": 101, "ADMSN_DT": admsn,
                         "ffsFirstMonth": _ffs_first_month(admsn, 24)}]
        prior_throughs = [{"DSYSRTKY": 1, "THRU_DT": 20210221}]
        baseDF, ipBaseDF = _setup_prior_hospitalization_dfs(spark, index_claims, prior_throughs)
        result_df = add_prior_hospitalization_info(baseDF, ipBaseDF)
        for c in ("hospitalizationsIn12Months", "hospitalizedIn12Months",
                  "hospitalizationsIn6Months", "hospitalizedIn6Months"):
            assert c in result_df.columns
        for f in (f.name for f in schemas["ipBase"].fields):
            assert f in result_df.columns, f"original column {f} dropped"
        # No row duplication introduced by the join-back.
        assert result_df.count() == 1
