import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# ============================================================
# Pure Python tests (no SparkSession needed)
# ============================================================

class TestYearsWithinCodeLimits:

    def test_within_limits(self):
        from cms.utilities import years_within_code_limits
        assert years_within_code_limits(2012, 2028) is True

    def test_yearI_below_min(self):
        from cms.utilities import years_within_code_limits
        assert years_within_code_limits(2009, 2020) is False

    def test_yearF_above_max(self):
        from cms.utilities import years_within_code_limits
        assert years_within_code_limits(2012, 2031) is False

    def test_exact_boundaries(self):
        from cms.utilities import years_within_code_limits, yearMin, yearMax
        assert years_within_code_limits(yearMin, yearMax) is True

    def test_both_outside(self):
        from cms.utilities import years_within_code_limits
        assert years_within_code_limits(2005, 2035) is False


class TestYearsWithinCmsDataLimits:

    def test_within_limits(self):
        from cms.utilities import years_within_cms_data_limits
        assert years_within_cms_data_limits(2012, 2022) is True

    def test_yearI_below_min(self):
        from cms.utilities import years_within_cms_data_limits
        assert years_within_cms_data_limits(2011, 2022) is False

    def test_yearF_above_max(self):
        from cms.utilities import years_within_cms_data_limits
        assert years_within_cms_data_limits(2012, 2023) is False

    def test_single_year(self):
        from cms.utilities import years_within_cms_data_limits
        assert years_within_cms_data_limits(2015, 2015) is True


class TestGetClaimTypeClaimPart:

    def test_opBase(self):
        from cms.utilities import get_claimType_claimPart
        assert get_claimType_claimPart("opBase") == ("op", "Base")

    def test_ipRevenue(self):
        from cms.utilities import get_claimType_claimPart
        assert get_claimType_claimPart("ipRevenue") == ("ip", "Revenue")

    def test_carLine(self):
        from cms.utilities import get_claimType_claimPart
        assert get_claimType_claimPart("carLine") == ("car", "Line")

    def test_snfBase(self):
        from cms.utilities import get_claimType_claimPart
        assert get_claimType_claimPart("snfBase") == ("snf", "Base")

    def test_hhaRevenue(self):
        from cms.utilities import get_claimType_claimPart
        assert get_claimType_claimPart("hhaRevenue") == ("hha", "Revenue")

    def test_hospBase(self):
        from cms.utilities import get_claimType_claimPart
        assert get_claimType_claimPart("hospBase") == ("hosp", "Base")

    def test_mbsf_returns_none_part(self):
        from cms.utilities import get_claimType_claimPart
        assert get_claimType_claimPart("mbsf") == ("mbsf", None)


class TestGetFilenames:

    def test_keys_present(self):
        from cms.utilities import get_filenames
        filenames = get_filenames("/data/cms", 2017, 2019)
        expected_keys = {"mbsf", "opBase", "opRevenue", "ipBase", "ipRevenue",
                         "snfBase", "snfRevenue", "hhaBase", "hhaRevenue",
                         "hospBase", "hospRevenue", "carBase", "carLine"}
        assert set(filenames.keys()) == expected_keys

    def test_list_lengths_match_year_range(self):
        from cms.utilities import get_filenames
        filenames = get_filenames("/data/cms", 2017, 2019)
        n_years = 3  # 2017, 2018, 2019
        for key in ["mbsf", "opBase", "ipBase", "snfBase", "hhaBase",
                     "hospBase", "carBase"]:
            assert len(filenames[key]) == n_years, f"{key} has {len(filenames[key])} files, expected {n_years}"

    def test_filenames_contain_year(self):
        from cms.utilities import get_filenames
        filenames = get_filenames("/data/cms", 2018, 2018)
        for key, flist in filenames.items():
            for f in flist:
                assert "2018" in f, f"{key}: filename {f} does not contain '2018'"

    def test_j_format_before_2016(self):
        from cms.utilities import get_filenames
        filenames = get_filenames("/data/cms", 2014, 2015)
        for f in filenames["ipBase"]:
            assert "claimsj" in f

    def test_k_format_from_2016(self):
        from cms.utilities import get_filenames
        filenames = get_filenames("/data/cms", 2016, 2017)
        for f in filenames["ipBase"]:
            assert "claimsk" in f

    def test_spanning_jk_transition(self):
        from cms.utilities import get_filenames
        filenames = get_filenames("/data/cms", 2015, 2017)
        ip_files = filenames["ipBase"]
        assert len(ip_files) == 3
        assert "claimsj" in ip_files[0]  # 2015
        assert "claimsk" in ip_files[1]  # 2016
        assert "claimsk" in ip_files[2]  # 2017

    def test_carrier_starts_at_2016_when_yearI_below(self):
        """Carrier files are unavailable before 2016."""
        from cms.utilities import get_filenames
        filenames = get_filenames("/data/cms", 2014, 2017)
        # carBase J format: only 2015 would qualify but carrier starts at 2016
        # so J portion uses yearIForCarrier=2016 which is >= yearJKTransition, producing empty J list
        # K portion: 2016, 2017
        assert len(filenames["carBase"]) == 2

    def test_path_structure(self):
        from cms.utilities import get_filenames
        filenames = get_filenames("/data/cms", 2018, 2018)
        assert filenames["mbsf"][0] == "/data/cms/MBSF/MBSF_2018/mbsf_2018.parquet"
        assert filenames["ipBase"][0] == "/data/cms/INP/INP_2018/inp_claimsk_2018.parquet"
        assert filenames["opRevenue"][0] == "/data/cms/OUT/OUT_2018/out_revenuek_2018.parquet"


# ============================================================
# PySpark tests
# ============================================================

class TestEnforceShortNames:

    def test_renames_long_to_short(self, spark):
        from cms.utilities import enforce_short_names
        df = spark.createDataFrame(
            [(1, 100, 20180101)],
            ["DESY_SORT_KEY", "CLAIM_NO", "CLM_THRU_DT"]
        )
        result = enforce_short_names(df, claimType="hha", claimPart="Base")
        assert "DSYSRTKY" in result.columns
        assert "CLAIMNO" in result.columns
        assert "THRU_DT" in result.columns
        row = result.collect()[0]
        assert row["DSYSRTKY"] == 1
        assert row["CLAIMNO"] == 100

    def test_car_base_rename(self, spark):
        from cms.utilities import enforce_short_names
        df = spark.createDataFrame(
            [(1, 200)],
            ["DESY_SORT_KEY", "CLAIM_NO"]
        )
        result = enforce_short_names(df, claimType="car", claimPart="Base")
        assert "DSYSRTKY" in result.columns
        assert "CLAIMNO" in result.columns


class TestPadZeros:

    def test_base_state_and_county_padded(self, spark):
        from cms.utilities import pad_zeros
        df = spark.createDataFrame(
            [(5, 10, 3)],
            ["STATE_CD", "CNTY_CD", "PRSTATE"]
        )
        result = pad_zeros(df, claimType="ip", claimPart="Base")
        row = result.collect()[0]
        assert row["STATE_CD"] == "05"
        assert row["CNTY_CD"] == "010"
        assert row["PRSTATE"] == "03"

    def test_base_carrier_no_prstate(self, spark):
        """Carrier base claims have STATE_CD and CNTY_CD but not PRSTATE padding."""
        from cms.utilities import pad_zeros
        df = spark.createDataFrame(
            [(5, 10)],
            ["STATE_CD", "CNTY_CD"]
        )
        result = pad_zeros(df, claimType="car", claimPart="Base")
        row = result.collect()[0]
        assert row["STATE_CD"] == "05"
        assert row["CNTY_CD"] == "010"

    def test_base_already_padded(self, spark):
        from cms.utilities import pad_zeros
        df = spark.createDataFrame(
            [("36", "061", "36")],
            ["STATE_CD", "CNTY_CD", "PRSTATE"]
        )
        result = pad_zeros(df, claimType="ip", claimPart="Base")
        row = result.collect()[0]
        assert row["STATE_CD"] == "36"
        assert row["CNTY_CD"] == "061"
        assert row["PRSTATE"] == "36"

    def test_mbsf_pads_state_county_and_fips(self, spark):
        from cms.utilities import pad_zeros
        cols = ["STATE_CD", "CNTY_CD"] + [f"STATE_CNTY_FIPS_CD_{x:02d}" for x in range(1, 13)]
        data = [[5, 10] + [39061] * 12]
        df = spark.createDataFrame(data, cols)
        result = pad_zeros(df, claimType="mbsf", claimPart=None)
        row = result.collect()[0]
        assert row["STATE_CD"] == "05"
        assert row["CNTY_CD"] == "010"
        assert row["STATE_CNTY_FIPS_CD_01"] == "39061"

    def test_null_state_stays_null(self, spark):
        """Null values: cast('int') on null -> null, cast('string') -> null, lpad(null) -> null."""
        from pyspark.sql.types import StructType, StructField, StringType
        from cms.utilities import pad_zeros
        schema = StructType([
            StructField("STATE_CD", StringType(), True),
            StructField("CNTY_CD", StringType(), True),
            StructField("PRSTATE", StringType(), True),
        ])
        df = spark.createDataFrame([(None, None, None)], schema=schema)
        result = pad_zeros(df, claimType="op", claimPart="Base")
        row = result.collect()[0]
        assert row["STATE_CD"] is None
        assert row["CNTY_CD"] is None
        assert row["PRSTATE"] is None

    def test_revenue_unchanged(self, spark):
        """Revenue claim parts should not be modified by pad_zeros."""
        from cms.utilities import pad_zeros
        df = spark.createDataFrame([(200, "99213")], ["REV_CNTR", "HCPCS_CD"])
        result = pad_zeros(df, claimType="ip", claimPart="Revenue")
        assert result.collect()[0]["REV_CNTR"] == 200


class TestFixYear:

    def test_two_digit_year_expanded(self, spark):
        from cms.utilities import fix_year
        df = spark.createDataFrame([(1, 15)], ["DSYSRTKY", "RFRNC_YR"])
        result = fix_year(df, claimType="mbsf", claimPart=None)
        row = result.collect()[0]
        assert row["RFRNC_YR"] == 2015

    def test_four_digit_year_unchanged(self, spark):
        from cms.utilities import fix_year
        df = spark.createDataFrame([(1, 2018)], ["DSYSRTKY", "RFRNC_YR"])
        result = fix_year(df, claimType="mbsf", claimPart=None)
        row = result.collect()[0]
        assert row["RFRNC_YR"] == 2018

    def test_non_mbsf_unchanged(self, spark):
        """fix_year only applies to mbsf; other claim types pass through."""
        from cms.utilities import fix_year
        df = spark.createDataFrame([(1, 20180101)], ["DSYSRTKY", "THRU_DT"])
        result = fix_year(df, claimType="ip", claimPart="Base")
        row = result.collect()[0]
        assert row["THRU_DT"] == 20180101

    def test_helper_column_dropped(self, spark):
        """fix_year should not leak the lengthRFRNC_YR helper column."""
        from cms.utilities import fix_year
        df = spark.createDataFrame([(1, 15)], ["DSYSRTKY", "RFRNC_YR"])
        result = fix_year(df, claimType="mbsf", claimPart=None)
        assert "lengthRFRNC_YR" not in result.columns


class TestEnforceSchema:

    def test_casts_columns_to_schema_types(self, spark):
        from cms.utilities import enforce_schema
        # opBase schema expects DSYSRTKY as IntegerType, PROVIDER as StringType, THRU_DT as IntegerType
        df = spark.createDataFrame(
            [("123", "ABC001", "20180101")],
            ["DSYSRTKY", "PROVIDER", "THRU_DT"]
        )
        result = enforce_schema(df, claimType="op", claimPart="Base")
        row = result.collect()[0]
        assert row["DSYSRTKY"] == 123
        assert row["PROVIDER"] == "ABC001"
        assert row["THRU_DT"] == 20180101

    def test_subset_of_schema_columns(self, spark):
        """DF with fewer columns than the full schema should still work."""
        from cms.utilities import enforce_schema
        df = spark.createDataFrame(
            [("999",)],
            ["DSYSRTKY"]
        )
        result = enforce_schema(df, claimType="op", claimPart="Base")
        assert result.columns == ["DSYSRTKY"]
        assert result.collect()[0]["DSYSRTKY"] == 999

    def test_non_castable_raises_error(self, spark):
        """A string that can't be cast to int raises an error (ANSI mode in Spark 4.x)."""
        from pyspark.sql.types import StructType, StructField, StringType
        from cms.utilities import enforce_schema
        schema = StructType([StructField("DSYSRTKY", StringType(), True)])
        df = spark.createDataFrame([("abc",)], schema=schema)
        result = enforce_schema(df, claimType="op", claimPart="Base")
        with pytest.raises(Exception):
            result.collect()

    def test_mbsf_schema(self, spark):
        from cms.utilities import enforce_schema
        df = spark.createDataFrame(
            [("1", "2018", "36", "061")],
            ["DSYSRTKY", "RFRNC_YR", "STATE_CD", "CNTY_CD"]
        )
        result = enforce_schema(df, claimType="mbsf", claimPart=None)
        row = result.collect()[0]
        assert row["DSYSRTKY"] == 1
        assert row["RFRNC_YR"] == 2018
        assert row["STATE_CD"] == "36"  # StringType, stays as string
        assert row["CNTY_CD"] == "061"


class TestAddThroughDateInfo:

    def test_basic_date(self, spark):
        """Jan 15, 2017 -> THRU_DT_YEAR=2017, THRU_DT_MONTH and THRU_DT_DAY computed."""
        from cms.utilities import add_through_date_info
        from utilities import daysInYearsPriorDict
        df = spark.createDataFrame([(20170115,)], ["THRU_DT"])
        result = add_through_date_info(df)
        row = result.collect()[0]
        assert row["THRU_DT_YEAR"] == 2017
        # Jan 15 is day 15 of the year
        expected_day = daysInYearsPriorDict[2017] + 15
        assert row["THRU_DT_DAY"] == expected_day

    def test_leap_year_feb29(self, spark):
        """Feb 29, 2016 (leap year) -> day 60 of 2016."""
        from cms.utilities import add_through_date_info
        from utilities import daysInYearsPriorDict
        df = spark.createDataFrame([(20160229,)], ["THRU_DT"])
        result = add_through_date_info(df)
        row = result.collect()[0]
        assert row["THRU_DT_YEAR"] == 2016
        expected_day = daysInYearsPriorDict[2016] + 60  # day 60 of leap year
        assert row["THRU_DT_DAY"] == expected_day

    def test_non_leap_year_mar1(self, spark):
        """Mar 1, 2017 (non-leap) -> day 60 of 2017."""
        from cms.utilities import add_through_date_info
        from utilities import daysInYearsPriorDict
        df = spark.createDataFrame([(20170301,)], ["THRU_DT"])
        result = add_through_date_info(df)
        row = result.collect()[0]
        assert row["THRU_DT_YEAR"] == 2017
        expected_day = daysInYearsPriorDict[2017] + 60  # day 60 of non-leap year
        assert row["THRU_DT_DAY"] == expected_day

    def test_dec31_leap_year(self, spark):
        """Dec 31, 2016 -> day 366 of 2016."""
        from cms.utilities import add_through_date_info
        from utilities import daysInYearsPriorDict
        df = spark.createDataFrame([(20161231,)], ["THRU_DT"])
        result = add_through_date_info(df)
        row = result.collect()[0]
        expected_day = daysInYearsPriorDict[2016] + 366
        assert row["THRU_DT_DAY"] == expected_day

    def test_dec31_non_leap_year(self, spark):
        """Dec 31, 2017 -> day 365 of 2017."""
        from cms.utilities import add_through_date_info
        from utilities import daysInYearsPriorDict
        df = spark.createDataFrame([(20171231,)], ["THRU_DT"])
        result = add_through_date_info(df)
        row = result.collect()[0]
        expected_day = daysInYearsPriorDict[2017] + 365
        assert row["THRU_DT_DAY"] == expected_day

    def test_jan1(self, spark):
        """Jan 1, 2018 -> day 1 of 2018."""
        from cms.utilities import add_through_date_info
        from utilities import daysInYearsPriorDict
        df = spark.createDataFrame([(20180101,)], ["THRU_DT"])
        result = add_through_date_info(df)
        row = result.collect()[0]
        expected_day = daysInYearsPriorDict[2018] + 1
        assert row["THRU_DT_DAY"] == expected_day

    def test_output_columns(self, spark):
        """Verify output columns: THRU_DT_YEAR, THRU_DT_MONTH, THRU_DT_DAY added; helper columns dropped."""
        from cms.utilities import add_through_date_info
        df = spark.createDataFrame([(20180601,)], ["THRU_DT"])
        result = add_through_date_info(df)
        assert "THRU_DT_YEAR" in result.columns
        assert "THRU_DT_MONTH" in result.columns
        assert "THRU_DT_DAY" in result.columns
        # intermediate columns should be dropped
        assert "THRU_DT_DAYOFYEAR" not in result.columns
        assert "THRU_DT_DAYSINYEARSPRIOR" not in result.columns
        assert "THRU_DT_MONTHOFYEAR" not in result.columns
        assert "THRU_DT_MONTHSINYEARSPRIOR" not in result.columns

    def test_month_calculation(self, spark):
        """June 2018 -> THRU_DT_MONTH = monthsInYearsPrior[2018] + 6."""
        from cms.utilities import add_through_date_info
        from utilities import monthsInYearsPriorDict
        df = spark.createDataFrame([(20180615,)], ["THRU_DT"])
        result = add_through_date_info(df)
        row = result.collect()[0]
        expected_month = monthsInYearsPriorDict[2018] + 6
        assert row["THRU_DT_MONTH"] == expected_month

    def test_multiple_rows(self, spark):
        """Process multiple rows at once."""
        from cms.utilities import add_through_date_info
        from utilities import daysInYearsPriorDict
        df = spark.createDataFrame(
            [(20170101,), (20180601,), (20160229,)],
            ["THRU_DT"]
        )
        result = add_through_date_info(df)
        assert result.count() == 3
        rows = {r["THRU_DT"]: r for r in result.collect()}
        assert rows[20170101]["THRU_DT_YEAR"] == 2017
        assert rows[20180601]["THRU_DT_YEAR"] == 2018
        assert rows[20160229]["THRU_DT_YEAR"] == 2016

    def test_consecutive_days_across_year_boundary(self, spark):
        """Dec 31 2017 and Jan 1 2018 should be exactly 1 day apart."""
        from cms.utilities import add_through_date_info
        df = spark.createDataFrame(
            [(20171231,), (20180101,)],
            ["THRU_DT"]
        )
        result = add_through_date_info(df)
        rows = {r["THRU_DT"]: r for r in result.collect()}
        assert rows[20180101]["THRU_DT_DAY"] - rows[20171231]["THRU_DT_DAY"] == 1
