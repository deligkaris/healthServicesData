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


class TestGetLastObservableDay:

    def test_is_the_day_before_january_1_of_the_next_year(self):
        from cms.utilities import get_lastObservableDay
        from utilities import daysInYearsPriorDict
        # Day numbers are daysInYearsPrior[year] + dayOfYear, so Jan 1 of yearF+1 is one day later.
        assert get_lastObservableDay(2021) + 1 == daysInYearsPriorDict[2022] + 1

    def test_accounts_for_leap_years(self):
        from cms.utilities import get_lastObservableDay
        assert get_lastObservableDay(2016) - get_lastObservableDay(2015) == 366  # 2016 is a leap year
        assert get_lastObservableDay(2021) - get_lastObservableDay(2020) == 365

    def test_increases_with_year(self):
        from cms.utilities import get_lastObservableDay
        days = [get_lastObservableDay(y) for y in range(2015, 2023)]
        assert days == sorted(days)


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
        """A non-castable string raises only under ANSI SQL mode (Spark 4.x default). The OSC cluster
        runs stock Spark 3.5 with ANSI OFF, where the cast returns null instead, so skip unless ANSI is on."""
        from pyspark.sql.types import StructType, StructField, StringType
        from cms.utilities import enforce_schema
        if spark.conf.get("spark.sql.ansi.enabled", "false") != "true":
            pytest.skip("ANSI SQL mode off (stock Spark 3.5 default): non-castable cast returns null, does not raise")
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


class TestPrepAhaDF:
    # AHA CSVs are read with no inferSchema, so every raw column arrives as a string.
    # prep_ahaDF must cast the numeric ones back; ahaBedsIcu had no cast and leaked out as a
    # string, forcing downstream R scripts to as.double() it. These tests lock in the int cast.

    # All raw columns prep_ahaDF references on the year<=2016 path, supplied as strings like the CSV read.
    AHA_COLS = ["MAPP3", "MAPP5", "MAPP8", "MAPP18", "BDH", "HOSPBD",
                "FTERES", "LAT", "LONG", "MSICBD", "CBSATYPE", "CNTRL", "MHSMEMB"]

    def _row(self, msicbd):
        # Representative string values; only MSICBD varies across tests.
        return [("1", "1", "1", "1", "150", "200", "10", "40.0", "-83.0",
                 msicbd, "Metro", "23", "1")]

    def test_ahaBedsIcu_is_int_not_string(self, spark):
        from utilities import prep_ahaDF
        df = spark.createDataFrame(self._row("20"), self.AHA_COLS)
        result = prep_ahaDF(df, "FY2016 ASDB")
        assert dict(result.dtypes)["ahaBedsIcu"] == "int"
        assert result.collect()[0]["ahaBedsIcu"] == 20

    def test_ahaBedsIcu_casts_multiple_rows(self, spark):
        from utilities import prep_ahaDF
        df = spark.createDataFrame(
            self._row("20") + self._row("305") + self._row("0"),
            self.AHA_COLS,
        )
        result = prep_ahaDF(df, "FY2016 ASDB")
        assert dict(result.dtypes)["ahaBedsIcu"] == "int"
        assert sorted(r["ahaBedsIcu"] for r in result.collect()) == [0, 20, 305]


class TestGetHcrisDF:
    # A few providers fill the Worksheet S-3 Part I bed count cell with a number that is not a bed
    # count (360044 filed 1594784 beds for FY2020), so get_hcrisDF checks that cell against the Bed
    # Days Available cell beside it and replaces a count more than 5 times too large. These tests
    # build a one year HOSP10FY2019 tree of the two headerless CSVs the function reads.

    YEAR = 2019
    # 01/01/2019-12/31/2019, so hcrisReportDays is 365 and every bed days cell below is beds x 365.
    RPT_FIELDS = ["{rec}", "9", "{prov}", "", "2", "01/01/2019", "12/31/2019", "08/31/2021",
                  "N", "N", "M", "02001", "4", "08/26/2021", "F", "08/26/2021", "", "04/29/2020"]

    def _build(self, spark, tmp_path, reports):
        # reports: {provider: [(bedsCell, bedDaysCell), ...]} as filed on line 14 column 2 and 3,
        # a None meaning the provider filed no such cell.
        folder = tmp_path / f"HOSP10FY{self.YEAR}"
        folder.mkdir()
        rptRows, nmrcRows = [], []
        for rec, (prov, cells) in enumerate(reports.items(), start=600000):
            rptRows.append(",".join(f.format(rec=rec, prov=prov) for f in self.RPT_FIELDS))
            beds, bedDays = cells
            if beds is not None:
                nmrcRows.append(f"{rec},S300001,01400,00200,{beds}")
            if bedDays is not None:
                nmrcRows.append(f"{rec},S300001,01400,00300,{bedDays}")
        (folder / f"HOSP10_{self.YEAR}_rpt.csv").write_text("\n".join(rptRows) + "\n")
        (folder / f"HOSP10_{self.YEAR}_nmrc.csv").write_text("\n".join(nmrcRows) + "\n")

        from utilities import get_hcrisDF
        df = get_hcrisDF(spark, str(tmp_path), yearInitial=self.YEAR, yearFinal=self.YEAR)
        self.rows = {r["PRVDR_NUM"]: r for r in df.collect()}
        return {prov: r["providerHcrisBedsTotal"] for prov, r in self.rows.items()}

    def test_count_consistent_with_bed_days_is_kept(self, spark, tmp_path):
        beds = self._build(spark, tmp_path, {"131316": (21, 7665)})
        assert beds["131316"] == 21

    def test_count_far_above_bed_days_is_replaced(self, spark, tmp_path):
        # 131316 as it actually filed FY2019: 52913 beds beside the 7665 bed days of a 21 bed hospital.
        beds = self._build(spark, tmp_path, {"131316": (52913, 7665)})
        assert beds["131316"] == 21

    def test_count_far_below_bed_days_is_kept(self, spark, tmp_path):
        # The other direction is the bed days cell being wrong, so the count must survive it.
        beds = self._build(spark, tmp_path, {"050058": (38, 98915)})
        assert beds["050058"] == 38

    def test_count_within_five_times_bed_days_is_kept(self, spark, tmp_path):
        # 100079 opened beds partway through the year: 524 at the end against bed days implying 325.
        beds = self._build(spark, tmp_path, {"100079": (524, 118660)})
        assert beds["100079"] == 524

    def test_count_without_bed_days_is_kept(self, spark, tmp_path):
        beds = self._build(spark, tmp_path, {"360044": (40, None)})
        assert beds["360044"] == 40

    def test_count_with_zero_bed_days_is_kept(self, spark, tmp_path):
        beds = self._build(spark, tmp_path, {"040011": (41, 0)})
        assert beds["040011"] == 41

    def test_count_is_kept_when_bed_days_imply_less_than_one_bed(self, spark, tmp_path):
        # 040011 as it filed FY2016: 41 beds against 6 bed days for the year. A hospital open six bed
        # days is the less believable of the two cells, so the count stands rather than becoming 0 or 1.
        beds = self._build(spark, tmp_path, {"040011": (41, 6)})
        assert beds["040011"] == 41

    def test_replacement_never_comes_out_as_no_beds(self, spark, tmp_path):
        # 020026 files 1 bed against 1 bed day every year; 0 is reserved for filing no such line at all.
        beds = self._build(spark, tmp_path, {"020026": (1, 1)})
        assert beds["020026"] == 1

    def test_replacement_is_rounded_to_a_whole_bed(self, spark, tmp_path):
        # 36135 bed days over 365 days is 99.0, and a count is only ever a whole number of beds.
        beds = self._build(spark, tmp_path, {"370215": (28401, 36135)})
        assert beds["370215"] == 99

    def test_bed_days_alone_does_not_invent_a_count(self, spark, tmp_path):
        beds = self._build(spark, tmp_path, {"131316": (21, 7665), "370215": (None, 36135)})
        assert beds == {"131316": 21}

    def test_bed_days_are_kept_as_a_column(self, spark, tmp_path):
        self._build(spark, tmp_path, {"131316": (52913, 7665)})
        assert self.rows["131316"]["providerHcrisBedDaysTotal"] == 7665

    def test_bed_days_are_long_not_int(self, spark, tmp_path):
        # A garbage bed days cell is 365 times a garbage bed count, so this column must not overflow:
        # 364036 filed 1098010950 for FY2016, half the int limit already.
        self._build(spark, tmp_path, {"364036": (30, 1098010950)})
        assert self.rows["364036"]["providerHcrisBedDaysTotal"] == 1098010950
