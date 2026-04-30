import pytest
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# ============================================================
# Helper to build MBSF-like DataFrames with sensible defaults
# ============================================================

def make_mbsf_row(**overrides):
    """Return a dict with default MBSF columns. Override any field via kwargs."""
    row = {
        "DSYSRTKY": 1,
        "RFRNC_YR": 2017,
        "STATE_CD": "36",
        "CNTY_CD": "061",
        "SEX": 1,
        "RACE": 1,
        "AGE": 70,
        "OREC": 0,
        "CREC": 0,
        "ESRD_IND": "0",
        "HMO_MO": 0,
        "A_MO_CNT": 12,
        "B_MO_CNT": 12,
        "V_DOD_SW": None,
        "DEATH_DT": None,
    }
    # BUYIN1-12: default to "3" (Part A+B)
    for i in range(1, 13):
        row[f"BUYIN{i}"] = "3"
    # HMOIND1-12: default to "0"
    for i in range(1, 13):
        row[f"HMOIND{i}"] = "0"
    # DUAL_01 - DUAL_12: default to 0 (no Medicaid)
    for i in range(1, 13):
        row[f"DUAL_{str(i).zfill(2)}"] = 0
    # MDCR_STUS_CD_01 - MDCR_STUS_CD_12: default to 10 (aged without ESRD)
    for i in range(1, 13):
        row[f"MDCR_STUS_CD_{str(i).zfill(2)}"] = 10
    row.update(overrides)
    return row


def _mbsf_schema():
    """Build a schema matching the columns produced by make_mbsf_row."""
    fields = [
        StructField("DSYSRTKY", IntegerType(), True),
        StructField("RFRNC_YR", IntegerType(), True),
        StructField("STATE_CD", StringType(), True),
        StructField("CNTY_CD", StringType(), True),
        StructField("SEX", IntegerType(), True),
        StructField("RACE", IntegerType(), True),
        StructField("AGE", IntegerType(), True),
        StructField("OREC", IntegerType(), True),
        StructField("CREC", IntegerType(), True),
        StructField("ESRD_IND", StringType(), True),
        StructField("HMO_MO", IntegerType(), True),
        StructField("A_MO_CNT", IntegerType(), True),
        StructField("B_MO_CNT", IntegerType(), True),
        StructField("V_DOD_SW", StringType(), True),
        StructField("DEATH_DT", IntegerType(), True),
    ]
    for i in range(1, 13):
        fields.append(StructField(f"BUYIN{i}", StringType(), True))
    for i in range(1, 13):
        fields.append(StructField(f"HMOIND{i}", StringType(), True))
    for i in range(1, 13):
        fields.append(StructField(f"DUAL_{str(i).zfill(2)}", IntegerType(), True))
    for i in range(1, 13):
        fields.append(StructField(f"MDCR_STUS_CD_{str(i).zfill(2)}", IntegerType(), True))
    return StructType(fields)

MBSF_SCHEMA = _mbsf_schema()


def make_mbsf_df(spark, rows):
    """Create a MBSF DataFrame from a list of row dicts (use make_mbsf_row)."""
    return spark.createDataFrame(rows, schema=MBSF_SCHEMA)


# ============================================================
# Tests
# ============================================================

class TestAddDeathDateInfo:

    def test_validated_death_date(self, spark):
        from cms.mbsf import add_death_date_info
        from utilities import daysInYearsPriorDict
        row = make_mbsf_row(V_DOD_SW="V", DEATH_DT=20170315)
        df = make_mbsf_df(spark, [row])
        result = add_death_date_info(df).collect()[0]
        assert result["DEATH_DT_YEAR"] == 2017
        assert result["DEATH_DT_MONTH"] == 3
        # Mar 15 = day 74 of non-leap year
        assert result["DEATH_DT_DAY"] == daysInYearsPriorDict[2017] + 74

    def test_no_death(self, spark):
        from cms.mbsf import add_death_date_info
        row = make_mbsf_row()
        df = make_mbsf_df(spark, [row])
        result = add_death_date_info(df).collect()[0]
        assert result["DEATH_DT_YEAR"] is None
        assert result["DEATH_DT_MONTH"] is None
        assert result["DEATH_DT_DAY"] is None

    def test_non_validated_death_ignored(self, spark):
        from cms.mbsf import add_death_date_info
        row = make_mbsf_row(V_DOD_SW="X", DEATH_DT=20170101)
        df = make_mbsf_df(spark, [row])
        result = add_death_date_info(df).collect()[0]
        assert result["DEATH_DT_YEAR"] is None
        assert result["DEATH_DT_DAY"] is None

    def test_helper_columns_dropped(self, spark):
        from cms.mbsf import add_death_date_info
        row = make_mbsf_row(V_DOD_SW="V", DEATH_DT=20170315)
        df = make_mbsf_df(spark, [row])
        result = add_death_date_info(df)
        assert "DEATH_DT_DAYSINYEARSPRIOR" not in result.columns
        assert "DEATH_DT_DAYOFYEAR" not in result.columns


class TestAddAllPartAB:

    def _prep(self, spark, row):
        """add_allPartAB requires DEATH_DT_MONTH and DEATH_DT_YEAR columns."""
        from cms.mbsf import add_death_date_info, add_allPartAB
        df = add_death_date_info(make_mbsf_df(spark, [row]))
        return add_allPartAB(df).collect()[0]

    def test_all_months_partAB(self, spark):
        """All 12 months with BUYIN='3' -> allPartAB=1."""
        result = self._prep(spark, make_mbsf_row())
        assert result["allPartAB"] == 1

    def test_one_month_missing(self, spark):
        """Month 6 has BUYIN='0' -> allPartAB=0."""
        result = self._prep(spark, make_mbsf_row(BUYIN6="0"))
        assert result["allPartAB"] == 0

    def test_all_months_code_C(self, spark):
        """All BUYIN='C' (state buy-in with Part A+B) -> allPartAB=1."""
        overrides = {f"BUYIN{i}": "C" for i in range(1, 13)}
        result = self._prep(spark, make_mbsf_row(**overrides))
        assert result["allPartAB"] == 1

    def test_dies_mid_year_with_coverage_until_death(self, spark):
        """Beneficiary dies in March, BUYIN 1-3='3', rest='0' -> allPartAB=1."""
        overrides = {f"BUYIN{i}": "0" for i in range(4, 13)}
        overrides.update(V_DOD_SW="V", DEATH_DT=20170315)
        result = self._prep(spark, make_mbsf_row(**overrides))
        assert result["allPartAB"] == 1

    def test_dies_mid_year_missing_month_before_death(self, spark):
        """Beneficiary dies in March, BUYIN2='0' -> allPartAB=0."""
        overrides = {f"BUYIN{i}": "0" for i in range(4, 13)}
        overrides.update(V_DOD_SW="V", DEATH_DT=20170315, BUYIN2="0")
        result = self._prep(spark, make_mbsf_row(**overrides))
        assert result["allPartAB"] == 0

    def test_no_partAB_any_month(self, spark):
        """All BUYIN='0' -> allPartAB=0."""
        overrides = {f"BUYIN{i}": "0" for i in range(1, 13)}
        result = self._prep(spark, make_mbsf_row(**overrides))
        assert result["allPartAB"] == 0


class TestAddHmo:

    def test_no_hmo(self, spark):
        from cms.mbsf import add_hmo
        row = make_mbsf_row(HMO_MO=0)
        result = add_hmo(make_mbsf_df(spark, [row])).collect()[0]
        assert result["hmo"] == 0

    def test_has_hmo(self, spark):
        from cms.mbsf import add_hmo
        row = make_mbsf_row(HMO_MO=3)
        result = add_hmo(make_mbsf_df(spark, [row])).collect()[0]
        assert result["hmo"] == 1


class TestAddFfs:

    def _prep(self, spark, row):
        from cms.mbsf import add_death_date_info, add_allPartAB, add_hmo, add_ffs
        df = add_death_date_info(make_mbsf_df(spark, [row]))
        df = add_allPartAB(df)
        df = add_hmo(df)
        return add_ffs(df).collect()[0]

    def test_ffs_yes(self, spark):
        """No HMO + all Part A+B -> ffs=1."""
        result = self._prep(spark, make_mbsf_row())
        assert result["ffs"] == 1

    def test_ffs_no_hmo(self, spark):
        """Has HMO -> ffs=0."""
        result = self._prep(spark, make_mbsf_row(HMO_MO=5))
        assert result["ffs"] == 0

    def test_ffs_no_partAB(self, spark):
        """No Part A+B -> ffs=0."""
        result = self._prep(spark, make_mbsf_row(BUYIN6="0"))
        assert result["ffs"] == 0


class TestAddFfsFirstMonth:

    def _prep(self, spark, rows):
        from cms.mbsf import add_death_date_info, add_allPartAB, add_hmo, add_ffs
        from cms.mbsf import add_rfrncYrMonthsInYearsPrior, add_ffsFirstMonth
        df = make_mbsf_df(spark, rows)
        df = add_death_date_info(df)
        df = add_allPartAB(df)
        df = add_hmo(df)
        df = add_ffs(df)
        df = add_rfrncYrMonthsInYearsPrior(df)
        return add_ffsFirstMonth(df)

    def test_ffs_first_month_single_year(self, spark):
        from utilities import monthsInYearsPriorDict
        row = make_mbsf_row()  # all BUYIN='3', first month of year = 1
        result = self._prep(spark, [row]).collect()[0]
        expected = monthsInYearsPriorDict[2017] + 1
        assert result["ffsFirstMonth"] == expected

    def test_ffs_first_month_across_years(self, spark):
        """Two years for same beneficiary: earliest first month wins."""
        from utilities import monthsInYearsPriorDict
        row1 = make_mbsf_row(RFRNC_YR=2017)
        row2 = make_mbsf_row(RFRNC_YR=2018)
        result = self._prep(spark, [row1, row2])
        rows = {r["RFRNC_YR"]: r for r in result.collect()}
        expected = monthsInYearsPriorDict[2017] + 1
        assert rows[2017]["ffsFirstMonth"] == expected
        assert rows[2018]["ffsFirstMonth"] == expected  # same min

    def test_non_ffs_has_null_first_month(self, spark):
        row = make_mbsf_row(HMO_MO=5)  # not FFS
        result = self._prep(spark, [row]).collect()[0]
        assert result["ffsFirstMonth"] is None


class TestAddFfsSinceJanuary:

    def _prep(self, spark, row):
        from cms.mbsf import add_death_date_info, add_allPartAB, add_hmo, add_ffs, add_ffsSinceJanuary
        df = add_death_date_info(make_mbsf_df(spark, [row]))
        df = add_allPartAB(df)
        df = add_hmo(df)
        df = add_ffs(df)
        return add_ffsSinceJanuary(df).collect()[0]

    def test_ffs_since_january(self, spark):
        """FFS=1, BUYIN1='3' -> ffsSinceJanuary=1."""
        result = self._prep(spark, make_mbsf_row())
        assert result["ffsSinceJanuary"] == 1

    def test_ffs_but_not_since_january(self, spark):
        """FFS=1 but BUYIN1='0' -> ffsSinceJanuary=0."""
        # Need BUYIN1='0' but still allPartAB=1. This is tricky because
        # allPartAB checks from first month with coverage. If BUYIN1='0' but 2-12='3',
        # the first month is 2, and slice(2,12) is all 1s -> allPartAB=1.
        result = self._prep(spark, make_mbsf_row(BUYIN1="0"))
        # allPartAB=1 (coverage from month 2 through 12), hmo=0 -> ffs=1
        # But BUYIN1 not in partABCodes -> ffsSinceJanuary=0
        assert result["ffsSinceJanuary"] == 0


class TestAddContinuousFfs:

    def _prep(self, spark, rows):
        from cms.mbsf import (add_death_date_info, add_allPartAB, add_hmo, add_ffs,
                               add_rfrncYrMonthsInYearsPrior, add_ffsFirstMonth, add_continuousFfs)
        df = make_mbsf_df(spark, rows)
        df = add_death_date_info(df)
        df = add_allPartAB(df)
        df = add_hmo(df)
        df = add_ffs(df)
        df = add_rfrncYrMonthsInYearsPrior(df)
        df = add_ffsFirstMonth(df)
        return add_continuousFfs(df)

    def test_continuous_ffs(self, spark):
        """Two consecutive years, both FFS since January -> continuousFfs=1."""
        rows = [make_mbsf_row(RFRNC_YR=2017), make_mbsf_row(RFRNC_YR=2018)]
        result = self._prep(spark, rows)
        for r in result.collect():
            assert r["continuousFfs"] == 1

    def test_non_continuous_ffs(self, spark):
        """Year 1 is FFS since Jan, year 2 has HMO -> continuousFfs=0 for all rows."""
        rows = [make_mbsf_row(RFRNC_YR=2017),
                make_mbsf_row(RFRNC_YR=2018, HMO_MO=5)]
        result = self._prep(spark, rows)
        for r in result.collect():
            assert r["continuousFfs"] == 0

    def test_loses_january_coverage(self, spark):
        """Year 1 since Jan, year 2 has BUYIN1='0' -> continuousFfs=0."""
        rows = [make_mbsf_row(RFRNC_YR=2017),
                make_mbsf_row(RFRNC_YR=2018, BUYIN1="0")]
        result = self._prep(spark, rows)
        for r in result.collect():
            assert r["continuousFfs"] == 0


class TestAddContinuousRfrncYr:

    def _prep(self, spark, rows):
        from cms.mbsf import add_continuousRfrncYr
        df = make_mbsf_df(spark, rows)
        return add_continuousRfrncYr(df)

    def test_consecutive_years(self, spark):
        rows = [make_mbsf_row(RFRNC_YR=2017), make_mbsf_row(RFRNC_YR=2018)]
        result = self._prep(spark, rows)
        for r in result.collect():
            assert r["continuousRfrncYr"] == 1

    def test_gap_year(self, spark):
        """Years 2017 and 2019 (gap of 2) -> continuousRfrncYr=0."""
        rows = [make_mbsf_row(RFRNC_YR=2017), make_mbsf_row(RFRNC_YR=2019)]
        result = self._prep(spark, rows)
        for r in result.collect():
            assert r["continuousRfrncYr"] == 0

    def test_single_year(self, spark):
        rows = [make_mbsf_row(RFRNC_YR=2017)]
        result = self._prep(spark, rows)
        assert result.collect()[0]["continuousRfrncYr"] == 1


class TestAddMedicaidEver:

    def test_no_medicaid(self, spark):
        from cms.mbsf import add_medicaidEver
        row = make_mbsf_row()  # all DUAL_XX = 0
        result = add_medicaidEver(make_mbsf_df(spark, [row])).collect()[0]
        assert result["medicaidEver"] == 0

    def test_has_medicaid_one_month(self, spark):
        from cms.mbsf import add_medicaidEver
        row = make_mbsf_row(DUAL_03=2)  # code 2 = qualified Medicare beneficiary plus
        result = add_medicaidEver(make_mbsf_df(spark, [row])).collect()[0]
        assert result["medicaidEver"] == 1

    def test_dual_code_not_in_list(self, spark):
        """DUAL code 9 is not in the qualifying list."""
        from cms.mbsf import add_medicaidEver
        row = make_mbsf_row(DUAL_06=9)
        result = add_medicaidEver(make_mbsf_df(spark, [row])).collect()[0]
        assert result["medicaidEver"] == 0

    def test_helper_columns_dropped(self, spark):
        from cms.mbsf import add_medicaidEver
        result = add_medicaidEver(make_mbsf_df(spark, [make_mbsf_row()]))
        assert "dualArray" not in result.columns
        assert "dualArrayFiltered" not in result.columns


class TestAddAnyEsrd:

    def test_no_esrd(self, spark):
        from cms.mbsf import add_anyEsrd
        row = make_mbsf_row()
        result = add_anyEsrd(make_mbsf_df(spark, [row])).collect()[0]
        assert result["anyEsrd"] == 0

    def test_esrd_ind_Y(self, spark):
        from cms.mbsf import add_anyEsrd
        row = make_mbsf_row(ESRD_IND="Y")
        result = add_anyEsrd(make_mbsf_df(spark, [row])).collect()[0]
        assert result["anyEsrd"] == 1

    def test_orec_2(self, spark):
        from cms.mbsf import add_anyEsrd
        row = make_mbsf_row(OREC=2)
        result = add_anyEsrd(make_mbsf_df(spark, [row])).collect()[0]
        assert result["anyEsrd"] == 1

    def test_crec_3(self, spark):
        from cms.mbsf import add_anyEsrd
        row = make_mbsf_row(CREC=3)
        result = add_anyEsrd(make_mbsf_df(spark, [row])).collect()[0]
        assert result["anyEsrd"] == 1

    def test_mdcr_stus_cd_11(self, spark):
        from cms.mbsf import add_anyEsrd
        row = make_mbsf_row(MDCR_STUS_CD_05=11)
        result = add_anyEsrd(make_mbsf_df(spark, [row])).collect()[0]
        assert result["anyEsrd"] == 1

    def test_mdcr_stus_cd_21(self, spark):
        from cms.mbsf import add_anyEsrd
        row = make_mbsf_row(MDCR_STUS_CD_01=21)
        result = add_anyEsrd(make_mbsf_df(spark, [row])).collect()[0]
        assert result["anyEsrd"] == 1

    def test_mdcr_stus_cd_31(self, spark):
        from cms.mbsf import add_anyEsrd
        row = make_mbsf_row(MDCR_STUS_CD_12=31)
        result = add_anyEsrd(make_mbsf_df(spark, [row])).collect()[0]
        assert result["anyEsrd"] == 1


class TestAddOhResident:

    def test_oh_resident(self, spark):
        from cms.mbsf import add_ohResident
        row = make_mbsf_row(STATE_CD="36")
        result = add_ohResident(make_mbsf_df(spark, [row])).collect()[0]
        assert result["ohResident"] == 1

    def test_not_oh_resident(self, spark):
        from cms.mbsf import add_ohResident
        row = make_mbsf_row(STATE_CD="39")
        result = add_ohResident(make_mbsf_df(spark, [row])).collect()[0]
        assert result["ohResident"] == 0


class TestAddCAppalachiaResident:

    def test_in_cAppalachia(self, spark):
        """Ohio STATE_CD='36' is in central Appalachia list."""
        from cms.mbsf import add_cAppalachiaResident
        row = make_mbsf_row(STATE_CD="36")
        result = add_cAppalachiaResident(make_mbsf_df(spark, [row])).collect()[0]
        assert result["cAppalachiaResident"] == 1

    def test_not_in_cAppalachia(self, spark):
        from cms.mbsf import add_cAppalachiaResident
        row = make_mbsf_row(STATE_CD="05")  # California
        result = add_cAppalachiaResident(make_mbsf_df(spark, [row])).collect()[0]
        assert result["cAppalachiaResident"] == 0


class TestAddSsaCounty:

    def test_concat(self, spark):
        from cms.mbsf import add_ssaCounty
        row = make_mbsf_row(STATE_CD="36", CNTY_CD="061")
        result = add_ssaCounty(make_mbsf_df(spark, [row])).collect()[0]
        assert result["ssaCounty"] == "36061"


class TestAddWillDie:

    def test_will_die(self, spark):
        from cms.mbsf import add_willDie
        rows = [make_mbsf_row(RFRNC_YR=2017, DEATH_DT=None),
                make_mbsf_row(RFRNC_YR=2018, DEATH_DT=20180601)]
        result = add_willDie(make_mbsf_df(spark, rows))
        for r in result.collect():
            assert r["willDie"] == 1

    def test_will_not_die(self, spark):
        from cms.mbsf import add_willDie
        rows = [make_mbsf_row(RFRNC_YR=2017), make_mbsf_row(RFRNC_YR=2018)]
        result = add_willDie(make_mbsf_df(spark, rows))
        for r in result.collect():
            assert r["willDie"] == 0


class TestFilterFFS:

    def test_filter_ffs(self, spark):
        from cms.mbsf import filter_FFS
        df = spark.createDataFrame([(1, 1), (2, 0)], ["DSYSRTKY", "ffs"])
        result = filter_FFS(df).collect()
        assert len(result) == 1
        assert result[0]["DSYSRTKY"] == 1


class TestFilterContinuousFfs:

    def test_filter(self, spark):
        from cms.mbsf import filter_continuousFfs
        df = spark.createDataFrame([(1, 1), (2, 0)], ["DSYSRTKY", "continuousFfs"])
        result = filter_continuousFfs(df).collect()
        assert len(result) == 1
        assert result[0]["DSYSRTKY"] == 1


class TestFilterContinuousRfrncYr:

    def test_filter(self, spark):
        from cms.mbsf import filter_continuousRfrncYr
        df = spark.createDataFrame([(1, 1), (2, 0)], ["DSYSRTKY", "continuousRfrncYr"])
        result = filter_continuousRfrncYr(df).collect()
        assert len(result) == 1
        assert result[0]["DSYSRTKY"] == 1


class TestFilterValidDod:

    def test_keeps_validated_and_no_death(self, spark):
        from cms.mbsf import filter_valid_dod
        df = spark.createDataFrame(
            [(1, 20170101, "V"), (2, None, None), (3, 20170601, "X")],
            ["DSYSRTKY", "DEATH_DT", "V_DOD_SW"]
        )
        result = filter_valid_dod(df)
        assert result.count() == 2
        ids = {r["DSYSRTKY"] for r in result.collect()}
        assert ids == {1, 2}


class TestAddResidentsInCounty:

    def test_count(self, spark):
        from cms.mbsf import add_residentsInCounty
        rows = [
            make_mbsf_row(DSYSRTKY=1, STATE_CD="36", CNTY_CD="061"),
            make_mbsf_row(DSYSRTKY=2, STATE_CD="36", CNTY_CD="061"),
            make_mbsf_row(DSYSRTKY=3, STATE_CD="36", CNTY_CD="099"),
        ]
        df = make_mbsf_df(spark, rows)
        df = df.withColumn("ssaCounty", F.concat(F.col("STATE_CD"), F.col("CNTY_CD")))
        result = add_residentsInCounty(df)
        rows_out = {r["DSYSRTKY"]: r for r in result.collect()}
        assert rows_out[1]["residentsInCounty"] == 2
        assert rows_out[2]["residentsInCounty"] == 2
        assert rows_out[3]["residentsInCounty"] == 1


class TestAddFipsCounty:

    def test_join(self, spark):
        from cms.mbsf import add_fipsCounty
        mbsf = spark.createDataFrame([(1, "36061")], ["DSYSRTKY", "ssaCounty"])
        cbsa = spark.createDataFrame([("36061", "39035")], ["ssaCounty", "fipsCounty"])
        result = add_fipsCounty(mbsf, cbsa).collect()[0]
        assert result["fipsCounty"] == "39035"

    def test_no_match(self, spark):
        from cms.mbsf import add_fipsCounty
        mbsf = spark.createDataFrame([(1, "99999")], ["DSYSRTKY", "ssaCounty"])
        cbsa = spark.createDataFrame([("36061", "39035")], ["ssaCounty", "fipsCounty"])
        result = add_fipsCounty(mbsf, cbsa).collect()[0]
        assert result["fipsCounty"] is None


class TestAddFipsState:

    def test_extracts_first_two(self, spark):
        from cms.mbsf import add_fipsState
        df = spark.createDataFrame([("39035",)], ["fipsCounty"])
        result = add_fipsState(df).collect()[0]
        assert result["fipsState"] == "39"


class TestAddRegion:

    def test_midwest(self, spark):
        from cms.mbsf import add_region
        df = spark.createDataFrame([("39",)], ["fipsState"])
        result = add_region(df).collect()[0]
        assert result["region"] == 2

    def test_south(self, spark):
        from cms.mbsf import add_region
        df = spark.createDataFrame([("48",)], ["fipsState"])
        result = add_region(df).collect()[0]
        assert result["region"] == 3

    def test_west(self, spark):
        from cms.mbsf import add_region
        df = spark.createDataFrame([("06",)], ["fipsState"])
        result = add_region(df).collect()[0]
        assert result["region"] == 4

    def test_northeast(self, spark):
        from cms.mbsf import add_region
        df = spark.createDataFrame([("36",)], ["fipsState"])
        result = add_region(df).collect()[0]
        assert result["region"] == 1

    def test_unknown_region(self, spark):
        from cms.mbsf import add_region
        df = spark.createDataFrame([("99",)], ["fipsState"])
        result = add_region(df).collect()[0]
        assert result["region"] is None


class TestAddContinuousFfsAndRfrncYr:

    def test_all_continuous(self, spark):
        from cms.mbsf import add_continuousFfsAndRfrncYr
        df = spark.createDataFrame([(1, 1, 1)], ["ffs", "continuousRfrncYr", "continuousFfs"])
        result = add_continuousFfsAndRfrncYr(df).collect()[0]
        assert result["continuousFfsAndRfrncYr"] == 1

    def test_not_continuous(self, spark):
        from cms.mbsf import add_continuousFfsAndRfrncYr
        df = spark.createDataFrame([(1, 0, 1)], ["ffs", "continuousRfrncYr", "continuousFfs"])
        result = add_continuousFfsAndRfrncYr(df).collect()[0]
        assert result["continuousFfsAndRfrncYr"] == 0


class TestPrepMbsf:

    def test_deduplicates(self, spark):
        from cms.mbsf import prep_mbsf
        rows = [make_mbsf_row(DSYSRTKY=1, RFRNC_YR=2017),
                make_mbsf_row(DSYSRTKY=1, RFRNC_YR=2017)]
        df = make_mbsf_df(spark, rows)
        result = prep_mbsf(df)
        assert result.count() == 1

    def test_keeps_different_years(self, spark):
        from cms.mbsf import prep_mbsf
        rows = [make_mbsf_row(DSYSRTKY=1, RFRNC_YR=2017),
                make_mbsf_row(DSYSRTKY=1, RFRNC_YR=2018)]
        df = make_mbsf_df(spark, rows)
        result = prep_mbsf(df)
        assert result.count() == 2


class TestGetDead:

    def test_returns_only_dead(self, spark):
        from cms.mbsf import add_death_date_info, get_dead
        rows = [make_mbsf_row(DSYSRTKY=1, V_DOD_SW="V", DEATH_DT=20170315),
                make_mbsf_row(DSYSRTKY=2)]
        df = add_death_date_info(make_mbsf_df(spark, rows))
        result = get_dead(df)
        assert result.count() == 1
        assert result.collect()[0]["DSYSRTKY"] == 1
        assert set(result.columns) == {"DSYSRTKY", "DEATH_DT_DAY"}


class TestDropUnusedColumns:

    def test_drops_expected_columns(self, spark):
        from cms.mbsf import drop_unused_columns
        row = make_mbsf_row()
        # Add the helper columns that drop_unused_columns expects
        extra = {
            "partABArray": [1]*12, "partABFirstMonthOfYear": 1,
            "partABLastMonthOfYear": 12, "partABArraySliced": [1]*12,
            "partABArraySlicedFiltered": [], "allPartAB": 1, "hmo": 0, "ffs": 1,
            "ffsFirstMonthOfYear": 1, "ffsSinceJanuary": 1,
            "ffsSinceJanuaryDifference": 0, "rfrncYrDifference": 0,
            "anyEsrdInMdcrStus": 0, "mdcrStusArray": [10]*12,
            "rfrncYrMonthsInYearsPrior": 60,
            "lastYearWithClaim": 2017, "probablyDead": 0,
        }
        row.update(extra)
        df = make_mbsf_df(spark, [row])
        result = drop_unused_columns(df)
        # These columns should be gone
        for col_prefix in ["BUYIN", "HMOIND", "DUAL_", "MDCR_STUS_CD_", "STATE_CNTY_FIPS_CD_"]:
            for c in result.columns:
                assert not c.startswith(col_prefix), f"Column {c} should have been dropped"
        assert "ESRD_IND" not in result.columns
        assert "partABArray" not in result.columns
        # These should remain
        assert "DSYSRTKY" in result.columns
        assert "RFRNC_YR" in result.columns
