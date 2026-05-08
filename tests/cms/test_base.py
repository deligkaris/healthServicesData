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
        # Codes that are NOT in the shock list but look similar.
        # R570 / R571 / R5710 — only "R57" exactly matches; the others should NOT.
        # I950 / I954 — only I951/I952/I953/I958/I959 match.
        from cms.base import add_shockDgns, add_shock
        df = make_dgns_code_all_df(spark, [["R570", "R571", "I950", "I954", "R032"]])
        result = add_shock(add_shockDgns(df)).collect()[0]
        assert result["shock"] == 0

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
        # Codes near the shock list but not in it.
        from cms.base import add_shockPoa
        df = make_septic_shock_poa_df(spark, [["R570", "R571", "I950", "I954", "R032"]])
        result = add_shockPoa(df).collect()[0]
        assert result["shockPoa"] == 0

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
