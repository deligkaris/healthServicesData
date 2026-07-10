import pytest
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


# ============================================================
# Helper to build minimal DataFrames with the inputs that
# add_dyadProportionTransfersIn reads (nodeInVol + dyadTransferVol).
# ============================================================

def _proportion_in_schema():
    return StructType([
        StructField("dyadTransferVol", IntegerType(), True),
        StructField("nodeInVol", IntegerType(), True),
    ])


def make_proportion_in_df(spark, rows):
    """rows: list of (dyadTransferVol, nodeInVol) tuples."""
    data = [{"dyadTransferVol": dv, "nodeInVol": nv} for dv, nv in rows]
    return spark.createDataFrame(data, schema=_proportion_in_schema())


# ============================================================
# Direct (unit-level) tests for add_dyadProportionTransfersIn
# ============================================================

class TestAddDyadProportionTransfersIn:

    def test_basic_ratio(self, spark):
        # 3 of 10 transfers in to B came from A -> 0.3
        from cms.transfers import add_dyadProportionTransfersIn
        df = make_proportion_in_df(spark, [(3, 10)])
        result = add_dyadProportionTransfersIn(df).collect()[0]
        assert result["dyadProportionTransfersIn"] == pytest.approx(0.3)

    def test_all_transfers_in_from_one_dyad(self, spark):
        # Every transfer ending in B came from A -> 1.0
        from cms.transfers import add_dyadProportionTransfersIn
        df = make_proportion_in_df(spark, [(5, 5)])
        result = add_dyadProportionTransfersIn(df).collect()[0]
        assert result["dyadProportionTransfersIn"] == pytest.approx(1.0)

    def test_single_transfer_in(self, spark):
        from cms.transfers import add_dyadProportionTransfersIn
        df = make_proportion_in_df(spark, [(1, 1)])
        result = add_dyadProportionTransfersIn(df).collect()[0]
        assert result["dyadProportionTransfersIn"] == pytest.approx(1.0)

    def test_node_in_vol_zero_returns_0(self, spark):
        # The .otherwise(0.) branch protects against divide-by-zero.
        from cms.transfers import add_dyadProportionTransfersIn
        df = make_proportion_in_df(spark, [(0, 0)])
        result = add_dyadProportionTransfersIn(df).collect()[0]
        assert result["dyadProportionTransfersIn"] == pytest.approx(0.0)

    def test_dyad_transfer_vol_zero_returns_0(self, spark):
        # 0 / nonzero -> 0.
        from cms.transfers import add_dyadProportionTransfersIn
        df = make_proportion_in_df(spark, [(0, 7)])
        result = add_dyadProportionTransfersIn(df).collect()[0]
        assert result["dyadProportionTransfersIn"] == pytest.approx(0.0)

    def test_result_in_unit_interval(self, spark):
        from cms.transfers import add_dyadProportionTransfersIn
        df = make_proportion_in_df(spark, [(1, 4), (2, 4), (4, 4)])
        results = [r["dyadProportionTransfersIn"] for r in add_dyadProportionTransfersIn(df).collect()]
        assert results == [pytest.approx(0.25), pytest.approx(0.5), pytest.approx(1.0)]
        for v in results:
            assert 0.0 <= v <= 1.0

    def test_multiple_rows_mixed(self, spark):
        from cms.transfers import add_dyadProportionTransfersIn
        df = make_proportion_in_df(spark, [
            (3, 10),   # 0.3
            (5, 5),    # 1.0
            (0, 0),    # otherwise -> 0.
            (0, 7),    # 0.
            (1, 4),    # 0.25
        ])
        results = [r["dyadProportionTransfersIn"] for r in add_dyadProportionTransfersIn(df).collect()]
        assert results == [
            pytest.approx(0.3),
            pytest.approx(1.0),
            pytest.approx(0.0),
            pytest.approx(0.0),
            pytest.approx(0.25),
        ]

    def test_column_added(self, spark):
        from cms.transfers import add_dyadProportionTransfersIn
        df = make_proportion_in_df(spark, [(1, 2)])
        result_df = add_dyadProportionTransfersIn(df)
        assert "dyadProportionTransfersIn" in result_df.columns
        assert "dyadTransferVol" in result_df.columns
        assert "nodeInVol" in result_df.columns


# ============================================================
# End-to-end pipeline test: raw transfer rows -> upstream
# functions -> add_dyadProportionTransfersIn
# ============================================================

def _raw_transfers_schema():
    # NPIs are typed as IntegerType because add_dyad builds an F.array of
    # (fromORGNPINM, toORGNPINM, fromTHRU_DT_YEAR) which requires a common type
    # with the integer year column.
    return StructType([
        StructField("fromORGNPINM", IntegerType(), True),
        StructField("toORGNPINM", IntegerType(), True),
        StructField("fromTHRU_DT_YEAR", IntegerType(), True),
        StructField("toTHRU_DT_YEAR", IntegerType(), True),
        StructField("fromCLAIMNO", StringType(), True),
        StructField("toCLAIMNO", StringType(), True),
    ])


def make_raw_transfers_df(spark, rows):
    """rows: list of (fromNPI, toNPI, fromYear, toYear, fromClaim, toClaim)."""
    data = [
        {"fromORGNPINM": f, "toORGNPINM": t, "fromTHRU_DT_YEAR": fy,
         "toTHRU_DT_YEAR": ty, "fromCLAIMNO": fc, "toCLAIMNO": tc}
        for f, t, fy, ty, fc, tc in rows
    ]
    return spark.createDataFrame(data, schema=_raw_transfers_schema())


class TestDyadProportionTransfersInPipeline:
    """Build a synthetic transfers DataFrame, run the upstream functions
    (add_node_volume_info, add_dyad, add_dyadTransferVol), and verify that
    add_dyadProportionTransfersIn yields (A->B transfers) / (all transfers ending in B)."""

    def test_pipeline_proportions(self, spark):
        from cms.transfers import (
            add_node_volume_info,
            add_dyad,
            add_dyadTransferVol,
            add_dyadProportionTransfersIn,
        )

        # NPIs: A=100, B=200, C=300, D=400.
        # Year 2020:
        #   A -> B (claim a1)        Node B in 2020 receives 3 transfers (from A, C, C)
        #   C -> B (claim c1)        Dyad A->B = 1  => 1/3
        #   C -> B (claim c2)        Dyad C->B = 2  => 2/3
        #   A -> D (claim a2)        Node D in 2020 receives 1 transfer; A->D = 1 => 1/1
        # Year 2021 (independent year so partitions don't bleed):
        #   A -> B (claim a3)        Node B in 2021 receives 1 transfer; A->B = 1 => 1/1
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (300, 200, 2020, 2020, "c1", "b2"),
            (300, 200, 2020, 2020, "c2", "b3"),
            (100, 400, 2020, 2020, "a2", "d1"),
            (100, 200, 2021, 2021, "a3", "b4"),
        ]
        df = make_raw_transfers_df(spark, rows)

        df = add_node_volume_info(df)
        df = add_dyad(df)
        df = add_dyadTransferVol(df)
        df = add_dyadProportionTransfersIn(df)

        # Map (fromORGNPINM, toORGNPINM, fromTHRU_DT_YEAR, fromCLAIMNO) -> proportion
        out = {(r["fromORGNPINM"], r["toORGNPINM"], r["fromTHRU_DT_YEAR"], r["fromCLAIMNO"]):
               r["dyadProportionTransfersIn"] for r in df.collect()}

        assert out[(100, 200, 2020, "a1")] == pytest.approx(1.0 / 3.0)
        assert out[(300, 200, 2020, "c1")] == pytest.approx(2.0 / 3.0)
        assert out[(300, 200, 2020, "c2")] == pytest.approx(2.0 / 3.0)
        assert out[(100, 400, 2020, "a2")] == pytest.approx(1.0)
        # 2021 partition stays separate from 2020 even though A->B exists in both
        assert out[(100, 200, 2021, "a3")] == pytest.approx(1.0)

    def test_pipeline_all_values_in_unit_interval(self, spark):
        from cms.transfers import (
            add_node_volume_info,
            add_dyad,
            add_dyadTransferVol,
            add_dyadProportionTransfersIn,
        )
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (300, 200, 2020, 2020, "c1", "b2"),
            (300, 200, 2020, 2020, "c2", "b3"),
            (100, 400, 2020, 2020, "a2", "d1"),
        ]
        df = make_raw_transfers_df(spark, rows)
        df = add_node_volume_info(df)
        df = add_dyad(df)
        df = add_dyadTransferVol(df)
        df = add_dyadProportionTransfersIn(df)
        values = [r["dyadProportionTransfersIn"] for r in df.collect()]
        for v in values:
            assert 0.0 <= v <= 1.0

    def test_proportion_consistent_when_to_year_differs_from_from_year(self, spark):
        # Regression: nodeInVol keys on fromTHRU_DT_YEAR so the proportion is constant
        # within a dyad even when the to-stay discharges in a different calendar year.
        # Three A->B transfers, all with fromTHRU_DT_YEAR=2020 but spanning toTHRU_DT_YEARs
        # 2020, 2021, 2022 -- all three rows should see the same dyadProportionTransfersIn.
        from cms.transfers import (
            add_node_volume_info,
            add_dyad,
            add_dyadTransferVol,
            add_dyadProportionTransfersIn,
        )
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 200, 2020, 2021, "a2", "b2"),
            (100, 200, 2020, 2022, "a3", "b3"),
        ]
        df = make_raw_transfers_df(spark, rows)
        df = add_node_volume_info(df)
        df = add_dyad(df)
        df = add_dyadTransferVol(df)
        df = add_dyadProportionTransfersIn(df)
        values = {r["fromCLAIMNO"]: r["dyadProportionTransfersIn"] for r in df.collect()}
        assert values["a1"] == pytest.approx(1.0)
        assert values["a2"] == pytest.approx(1.0)
        assert values["a3"] == pytest.approx(1.0)


# ============================================================
# Helper to build minimal DataFrames with the inputs that
# add_dyadProportionTransfersOut reads (nodeOutVol + dyadTransferVol).
# ============================================================

def _proportion_out_schema():
    return StructType([
        StructField("dyadTransferVol", IntegerType(), True),
        StructField("nodeOutVol", IntegerType(), True),
    ])


def make_proportion_out_df(spark, rows):
    """rows: list of (dyadTransferVol, nodeOutVol) tuples."""
    data = [{"dyadTransferVol": dv, "nodeOutVol": nv} for dv, nv in rows]
    return spark.createDataFrame(data, schema=_proportion_out_schema())


# ============================================================
# Direct (unit-level) tests for add_dyadProportionTransfersOut
# ============================================================

class TestAddDyadProportionTransfersOut:

    def test_basic_ratio(self, spark):
        # 3 of 10 transfers out of A went to B -> 0.3
        from cms.transfers import add_dyadProportionTransfersOut
        df = make_proportion_out_df(spark, [(3, 10)])
        result = add_dyadProportionTransfersOut(df).collect()[0]
        assert result["dyadProportionTransfersOut"] == pytest.approx(0.3)

    def test_all_transfers_out_to_one_dyad(self, spark):
        # Every transfer out of A went to B -> 1.0
        from cms.transfers import add_dyadProportionTransfersOut
        df = make_proportion_out_df(spark, [(5, 5)])
        result = add_dyadProportionTransfersOut(df).collect()[0]
        assert result["dyadProportionTransfersOut"] == pytest.approx(1.0)

    def test_single_transfer_out(self, spark):
        from cms.transfers import add_dyadProportionTransfersOut
        df = make_proportion_out_df(spark, [(1, 1)])
        result = add_dyadProportionTransfersOut(df).collect()[0]
        assert result["dyadProportionTransfersOut"] == pytest.approx(1.0)

    def test_node_out_vol_zero_returns_0(self, spark):
        # The .otherwise(0.) branch protects against divide-by-zero.
        from cms.transfers import add_dyadProportionTransfersOut
        df = make_proportion_out_df(spark, [(0, 0)])
        result = add_dyadProportionTransfersOut(df).collect()[0]
        assert result["dyadProportionTransfersOut"] == pytest.approx(0.0)

    def test_dyad_transfer_vol_zero_returns_0(self, spark):
        # 0 / nonzero -> 0.
        from cms.transfers import add_dyadProportionTransfersOut
        df = make_proportion_out_df(spark, [(0, 7)])
        result = add_dyadProportionTransfersOut(df).collect()[0]
        assert result["dyadProportionTransfersOut"] == pytest.approx(0.0)

    def test_result_in_unit_interval(self, spark):
        from cms.transfers import add_dyadProportionTransfersOut
        df = make_proportion_out_df(spark, [(1, 4), (2, 4), (4, 4)])
        results = [r["dyadProportionTransfersOut"] for r in add_dyadProportionTransfersOut(df).collect()]
        assert results == [pytest.approx(0.25), pytest.approx(0.5), pytest.approx(1.0)]
        for v in results:
            assert 0.0 <= v <= 1.0

    def test_multiple_rows_mixed(self, spark):
        from cms.transfers import add_dyadProportionTransfersOut
        df = make_proportion_out_df(spark, [
            (3, 10),   # 0.3
            (5, 5),    # 1.0
            (0, 0),    # otherwise -> 0.
            (0, 7),    # 0.
            (1, 4),    # 0.25
        ])
        results = [r["dyadProportionTransfersOut"] for r in add_dyadProportionTransfersOut(df).collect()]
        assert results == [
            pytest.approx(0.3),
            pytest.approx(1.0),
            pytest.approx(0.0),
            pytest.approx(0.0),
            pytest.approx(0.25),
        ]

    def test_column_added(self, spark):
        from cms.transfers import add_dyadProportionTransfersOut
        df = make_proportion_out_df(spark, [(1, 2)])
        result_df = add_dyadProportionTransfersOut(df)
        assert "dyadProportionTransfersOut" in result_df.columns
        assert "dyadTransferVol" in result_df.columns
        assert "nodeOutVol" in result_df.columns


# ============================================================
# End-to-end pipeline test: raw transfer rows -> upstream
# functions -> add_dyadProportionTransfersOut
# ============================================================

class TestDyadProportionTransfersOutPipeline:
    """Build a synthetic transfers DataFrame, run the upstream functions
    (add_node_volume_info, add_dyad, add_dyadTransferVol), and verify that
    add_dyadProportionTransfersOut yields (A->B transfers) / (all transfers out of A)."""

    def test_pipeline_proportions(self, spark):
        from cms.transfers import (
            add_node_volume_info,
            add_dyad,
            add_dyadTransferVol,
            add_dyadProportionTransfersOut,
        )

        # NPIs: A=100, B=200, C=300, D=400.
        # Year 2020:
        #   A -> B (claim a1)        Node A in 2020 sends 2 transfers (to B, to D)
        #   C -> B (claim c1)        Dyad A->B = 1  => 1/2
        #   C -> B (claim c2)        Node C in 2020 sends 2 transfers (both to B); C->B = 2 => 2/2 = 1.0
        #   A -> D (claim a2)        Dyad A->D = 1  => 1/2
        # Year 2021 (independent year so partitions don't bleed):
        #   A -> B (claim a3)        Node A in 2021 sends 1 transfer; A->B = 1 => 1/1
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (300, 200, 2020, 2020, "c1", "b2"),
            (300, 200, 2020, 2020, "c2", "b3"),
            (100, 400, 2020, 2020, "a2", "d1"),
            (100, 200, 2021, 2021, "a3", "b4"),
        ]
        df = make_raw_transfers_df(spark, rows)

        df = add_node_volume_info(df)
        df = add_dyad(df)
        df = add_dyadTransferVol(df)
        df = add_dyadProportionTransfersOut(df)

        out = {(r["fromORGNPINM"], r["toORGNPINM"], r["fromTHRU_DT_YEAR"], r["fromCLAIMNO"]):
               r["dyadProportionTransfersOut"] for r in df.collect()}

        assert out[(100, 200, 2020, "a1")] == pytest.approx(0.5)
        assert out[(300, 200, 2020, "c1")] == pytest.approx(1.0)
        assert out[(300, 200, 2020, "c2")] == pytest.approx(1.0)
        assert out[(100, 400, 2020, "a2")] == pytest.approx(0.5)
        # 2021 partition stays separate from 2020 even though A->B exists in both
        assert out[(100, 200, 2021, "a3")] == pytest.approx(1.0)

    def test_pipeline_all_values_in_unit_interval(self, spark):
        from cms.transfers import (
            add_node_volume_info,
            add_dyad,
            add_dyadTransferVol,
            add_dyadProportionTransfersOut,
        )
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (300, 200, 2020, 2020, "c1", "b2"),
            (300, 200, 2020, 2020, "c2", "b3"),
            (100, 400, 2020, 2020, "a2", "d1"),
        ]
        df = make_raw_transfers_df(spark, rows)
        df = add_node_volume_info(df)
        df = add_dyad(df)
        df = add_dyadTransferVol(df)
        df = add_dyadProportionTransfersOut(df)
        values = [r["dyadProportionTransfersOut"] for r in df.collect()]
        for v in values:
            assert 0.0 <= v <= 1.0

    def test_pipeline_proportions_out_of_a_sum_to_one(self, spark):
        # Sanity-check the semantic: for a given (from-node, year), the
        # dyad-level proportions should sum to 1 once duplicates within a
        # dyad are collapsed.
        from cms.transfers import (
            add_node_volume_info,
            add_dyad,
            add_dyadTransferVol,
            add_dyadProportionTransfersOut,
        )
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 400, 2020, 2020, "a2", "d1"),
            (100, 400, 2020, 2020, "a3", "d2"),
            (100, 500, 2020, 2020, "a4", "e1"),
        ]
        df = make_raw_transfers_df(spark, rows)
        df = add_node_volume_info(df)
        df = add_dyad(df)
        df = add_dyadTransferVol(df)
        df = add_dyadProportionTransfersOut(df)
        # Collapse to unique dyads for node A in 2020.
        unique = {(r["toORGNPINM"]): r["dyadProportionTransfersOut"]
                  for r in df.filter("fromORGNPINM = 100 AND fromTHRU_DT_YEAR = 2020").collect()}
        assert unique[200] == pytest.approx(0.25)  # 1/4 of A's transfers went to B
        assert unique[400] == pytest.approx(0.5)   # 2/4 went to D
        assert unique[500] == pytest.approx(0.25)  # 1/4 went to E
        assert sum(unique.values()) == pytest.approx(1.0)


# ============================================================
# End-to-end pipeline tests for add_nodeHhi
# ============================================================

def _run_hhi_pipeline(spark, rows):
    """Build a raw transfers DF, run all upstream functions needed by
    add_nodeHhi, and return the resulting DF."""
    from cms.transfers import (
        add_node_volume_info,
        add_dyad,
        add_dyadTransferVol,
        add_dyadProportionTransfersOut,
        add_nodeHhi,
    )
    df = make_raw_transfers_df(spark, rows)
    df = add_node_volume_info(df)
    df = add_dyad(df)
    df = add_dyadTransferVol(df)
    df = add_dyadProportionTransfersOut(df)
    df = add_nodeHhi(df)
    return df


def _run_hhi_info_pipeline(spark, rows):
    """Like _run_hhi_pipeline but runs add_node_hhi_info (add_nodeHhi + the
    prior-year lag) so both nodeHhi and nodeHhiPrior are present."""
    from cms.transfers import (
        add_node_volume_info,
        add_dyad,
        add_dyadTransferVol,
        add_dyadProportionTransfersOut,
        add_node_hhi_info,
    )
    df = make_raw_transfers_df(spark, rows)
    df = add_node_volume_info(df)
    df = add_dyad(df)
    df = add_dyadTransferVol(df)
    df = add_dyadProportionTransfersOut(df)
    df = add_node_hhi_info(df)
    return df


def _hhi_for(df, from_npi, year):
    """Return the unique nodeHhi for (from_npi, year). Asserts that all rows
    for this (node, year) carry the same HHI value."""
    rows = df.filter((df.fromORGNPINM == from_npi) & (df.fromTHRU_DT_YEAR == year)).collect()
    assert len(rows) > 0, f"no rows for {from_npi} in {year}"
    values = {r["nodeHhi"] for r in rows}
    assert len(values) == 1, f"nodeHhi differs across rows for {from_npi} in {year}: {values}"
    return next(iter(values))


class TestAddNodeHhi:

    def test_single_destination_returns_1(self, spark):
        # All of A's transfers in 2020 go to B -> HHI = 1.0
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 200, 2020, 2020, "a2", "b2"),
            (100, 200, 2020, 2020, "a3", "b3"),
        ]
        df = _run_hhi_pipeline(spark, rows)
        assert _hhi_for(df, 100, 2020) == pytest.approx(1.0)

    def test_evenly_distributed_three_destinations(self, spark):
        # A -> B, A -> C, A -> D (1 each) -> each prop = 1/3 -> HHI = 3 * (1/3)^2 = 1/3
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 300, 2020, 2020, "a2", "c1"),
            (100, 400, 2020, 2020, "a3", "d1"),
        ]
        df = _run_hhi_pipeline(spark, rows)
        assert _hhi_for(df, 100, 2020) == pytest.approx(1.0 / 3.0)

    def test_concentrated_two_destinations(self, spark):
        # A -> B (3), A -> C (1) -> props 0.75, 0.25 -> HHI = 0.5625 + 0.0625 = 0.625
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 200, 2020, 2020, "a2", "b2"),
            (100, 200, 2020, 2020, "a3", "b3"),
            (100, 300, 2020, 2020, "a4", "c1"),
        ]
        df = _run_hhi_pipeline(spark, rows)
        assert _hhi_for(df, 100, 2020) == pytest.approx(0.625)

    def test_duplicate_dyad_rows_not_double_counted(self, spark):
        # 4 transfers A -> B in 2020 -> dyadProportionTransfersOut = 1.0 on every row.
        # Without row_number dedup, HHI would be 4 * 1.0^2 = 4.0; with dedup it is 1.0.
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 200, 2020, 2020, "a2", "b2"),
            (100, 200, 2020, 2020, "a3", "b3"),
            (100, 200, 2020, 2020, "a4", "b4"),
        ]
        df = _run_hhi_pipeline(spark, rows)
        assert _hhi_for(df, 100, 2020) == pytest.approx(1.0)

    def test_year_partitions_are_independent(self, spark):
        # 2020: A -> B (1), A -> C (1) -> HHI = 0.5
        # 2021: all A transfers go to B -> HHI = 1.0
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 300, 2020, 2020, "a2", "c1"),
            (100, 200, 2021, 2021, "a3", "b2"),
            (100, 200, 2021, 2021, "a4", "b3"),
        ]
        df = _run_hhi_pipeline(spark, rows)
        assert _hhi_for(df, 100, 2020) == pytest.approx(0.5)
        assert _hhi_for(df, 100, 2021) == pytest.approx(1.0)

    def test_multiple_source_nodes_independent(self, spark):
        # A: 2 destinations evenly (HHI = 0.5)
        # C: all to B (HHI = 1.0)
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 300, 2020, 2020, "a2", "c1"),
            (300, 200, 2020, 2020, "c2", "b2"),
            (300, 200, 2020, 2020, "c3", "b3"),
        ]
        df = _run_hhi_pipeline(spark, rows)
        assert _hhi_for(df, 100, 2020) == pytest.approx(0.5)
        assert _hhi_for(df, 300, 2020) == pytest.approx(1.0)

    def test_hhi_in_unit_interval(self, spark):
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 200, 2020, 2020, "a2", "b2"),
            (100, 300, 2020, 2020, "a3", "c1"),
            (100, 400, 2020, 2020, "a4", "d1"),
            (100, 500, 2020, 2020, "a5", "e1"),
        ]
        df = _run_hhi_pipeline(spark, rows)
        values = [r["nodeHhi"] for r in df.collect()]
        for v in values:
            assert 0.0 < v <= 1.0

    def test_hhi_lower_bound_one_over_n(self, spark):
        # 5 destinations, 1 transfer each -> HHI = 5 * (1/5)^2 = 1/5
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 300, 2020, 2020, "a2", "c1"),
            (100, 400, 2020, 2020, "a3", "d1"),
            (100, 500, 2020, 2020, "a4", "e1"),
            (100, 600, 2020, 2020, "a5", "f1"),
        ]
        df = _run_hhi_pipeline(spark, rows)
        assert _hhi_for(df, 100, 2020) == pytest.approx(0.2)

    def test_column_added_and_intermediates_dropped(self, spark):
        rows = [(100, 200, 2020, 2020, "a1", "b1")]
        df = _run_hhi_pipeline(spark, rows)
        assert "nodeHhi" in df.columns
        # add_nodeHhi drops these intermediate columns.
        assert "dyadRowNumber" not in df.columns
        assert "dyadProportionTransfersOutSquared" not in df.columns


# ============================================================
# End-to-end pipeline tests for add_dyadTransferVol
# ============================================================

def _run_dyad_vol_pipeline(spark, rows):
    """Build a raw transfers DF, run add_dyad and add_dyadTransferVol."""
    from cms.transfers import add_dyad, add_dyadTransferVol
    df = make_raw_transfers_df(spark, rows)
    df = add_dyad(df)
    df = add_dyadTransferVol(df)
    return df


def _vol_for(df, from_npi, to_npi, year):
    """Return the unique dyadTransferVol for (from_npi, to_npi, year).
    Asserts all rows in the dyad carry the same volume."""
    sub = df.filter((df.fromORGNPINM == from_npi) &
                    (df.toORGNPINM == to_npi) &
                    (df.fromTHRU_DT_YEAR == year)).collect()
    assert len(sub) > 0, f"no rows for {from_npi}->{to_npi} in {year}"
    values = {r["dyadTransferVol"] for r in sub}
    assert len(values) == 1, f"dyadTransferVol differs across rows for {from_npi}->{to_npi} in {year}: {values}"
    return next(iter(values))


class TestAddDyadTransferVol:

    def test_single_transfer_returns_1(self, spark):
        rows = [(100, 200, 2020, 2020, "a1", "b1")]
        df = _run_dyad_vol_pipeline(spark, rows)
        assert _vol_for(df, 100, 200, 2020) == 1

    def test_counts_all_transfers_in_dyad(self, spark):
        # 4 A->B transfers in 2020 -> dyadTransferVol = 4 on every row
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 200, 2020, 2020, "a2", "b2"),
            (100, 200, 2020, 2020, "a3", "b3"),
            (100, 200, 2020, 2020, "a4", "b4"),
        ]
        df = _run_dyad_vol_pipeline(spark, rows)
        assert _vol_for(df, 100, 200, 2020) == 4

    def test_value_broadcast_to_every_row(self, spark):
        # Window function (not aggregate): every row of the dyad carries the count.
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 200, 2020, 2020, "a2", "b2"),
            (100, 200, 2020, 2020, "a3", "b3"),
        ]
        df = _run_dyad_vol_pipeline(spark, rows)
        vols = [r["dyadTransferVol"] for r in df.collect()]
        assert vols == [3, 3, 3]

    def test_different_dyads_counted_independently(self, spark):
        # A->B: 3 transfers; A->C: 1 transfer; C->B: 2 transfers (all 2020)
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 200, 2020, 2020, "a2", "b2"),
            (100, 200, 2020, 2020, "a3", "b3"),
            (100, 300, 2020, 2020, "a4", "c1"),
            (300, 200, 2020, 2020, "c2", "b4"),
            (300, 200, 2020, 2020, "c3", "b5"),
        ]
        df = _run_dyad_vol_pipeline(spark, rows)
        assert _vol_for(df, 100, 200, 2020) == 3
        assert _vol_for(df, 100, 300, 2020) == 1
        assert _vol_for(df, 300, 200, 2020) == 2

    def test_same_pair_different_years_counted_separately(self, spark):
        # A->B in 2020 (2 transfers) and A->B in 2021 (3 transfers) are distinct dyads
        # because add_dyad encodes the year in the dyad array.
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 200, 2020, 2020, "a2", "b2"),
            (100, 200, 2021, 2021, "a3", "b3"),
            (100, 200, 2021, 2021, "a4", "b4"),
            (100, 200, 2021, 2021, "a5", "b5"),
        ]
        df = _run_dyad_vol_pipeline(spark, rows)
        assert _vol_for(df, 100, 200, 2020) == 2
        assert _vol_for(df, 100, 200, 2021) == 3

    def test_reverse_direction_is_distinct_dyad(self, spark):
        # A->B and B->A are different dyads (dyad is ordered).
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (200, 100, 2020, 2020, "b2", "a2"),
            (200, 100, 2020, 2020, "b3", "a3"),
        ]
        df = _run_dyad_vol_pipeline(spark, rows)
        assert _vol_for(df, 100, 200, 2020) == 1
        assert _vol_for(df, 200, 100, 2020) == 2

    def test_column_added(self, spark):
        rows = [(100, 200, 2020, 2020, "a1", "b1")]
        df = _run_dyad_vol_pipeline(spark, rows)
        assert "dyadTransferVol" in df.columns
        assert "dyad" in df.columns
        assert "fromCLAIMNO" in df.columns


# ============================================================
# Helper for transfer-flag tests (add_transfertpa / ct / mri /
# nihss / nihssGroup). These functions take an already-joined
# transfers DF with prefixed columns.
# ============================================================

def make_two_flag_df(spark, rows, from_col, to_col):
    """rows: list of (from_value, to_value) tuples."""
    schema = StructType([
        StructField(from_col, IntegerType(), True),
        StructField(to_col, IntegerType(), True),
    ])
    data = [{from_col: f, to_col: t} for f, t in rows]
    return spark.createDataFrame(data, schema=schema)


class TestAddTransfertpa:

    def test_either_side_one_returns_1(self, spark):
        from cms.transfers import add_transfertpa
        df = make_two_flag_df(spark, [(1, 0), (0, 1), (1, 1)], "fromtpa", "totpa")
        results = [r["transfertpa"] for r in add_transfertpa(df).collect()]
        assert results == [1, 1, 1]

    def test_both_zero_returns_0(self, spark):
        from cms.transfers import add_transfertpa
        df = make_two_flag_df(spark, [(0, 0)], "fromtpa", "totpa")
        assert add_transfertpa(df).collect()[0]["transfertpa"] == 0

    def test_null_treated_as_not_one(self, spark):
        # NULL OR 0 -> falls through to .otherwise(0)
        from cms.transfers import add_transfertpa
        df = make_two_flag_df(spark, [(None, 0), (0, None), (None, None)], "fromtpa", "totpa")
        results = [r["transfertpa"] for r in add_transfertpa(df).collect()]
        assert results == [0, 0, 0]

    def test_column_added(self, spark):
        from cms.transfers import add_transfertpa
        df = make_two_flag_df(spark, [(1, 0)], "fromtpa", "totpa")
        result_df = add_transfertpa(df)
        assert "transfertpa" in result_df.columns


class TestAddTransferct:

    def test_either_side_one_returns_1(self, spark):
        from cms.transfers import add_transferct
        df = make_two_flag_df(spark, [(1, 0), (0, 1), (1, 1)], "fromct", "toct")
        results = [r["transferct"] for r in add_transferct(df).collect()]
        assert results == [1, 1, 1]

    def test_both_zero_returns_0(self, spark):
        from cms.transfers import add_transferct
        df = make_two_flag_df(spark, [(0, 0)], "fromct", "toct")
        assert add_transferct(df).collect()[0]["transferct"] == 0

    def test_column_added(self, spark):
        from cms.transfers import add_transferct
        df = make_two_flag_df(spark, [(1, 0)], "fromct", "toct")
        assert "transferct" in add_transferct(df).columns


class TestAddTransfermri:

    def test_either_side_one_returns_1(self, spark):
        from cms.transfers import add_transfermri
        df = make_two_flag_df(spark, [(1, 0), (0, 1), (1, 1)], "frommri", "tomri")
        results = [r["transfermri"] for r in add_transfermri(df).collect()]
        assert results == [1, 1, 1]

    def test_both_zero_returns_0(self, spark):
        from cms.transfers import add_transfermri
        df = make_two_flag_df(spark, [(0, 0)], "frommri", "tomri")
        assert add_transfermri(df).collect()[0]["transfermri"] == 0

    def test_column_added(self, spark):
        from cms.transfers import add_transfermri
        df = make_two_flag_df(spark, [(0, 1)], "frommri", "tomri")
        assert "transfermri" in add_transfermri(df).columns


class TestAddTransfernihss:

    def test_from_null_falls_back_to_to(self, spark):
        from cms.transfers import add_transfernihss
        df = make_two_flag_df(spark, [(None, 12)], "fromnihss", "tonihss")
        assert add_transfernihss(df).collect()[0]["transfernihss"] == 12

    def test_from_non_null_wins_over_to(self, spark):
        # When fromnihss is present we keep it even if tonihss differs.
        from cms.transfers import add_transfernihss
        df = make_two_flag_df(spark, [(5, 20)], "fromnihss", "tonihss")
        assert add_transfernihss(df).collect()[0]["transfernihss"] == 5

    def test_both_null_returns_null(self, spark):
        from cms.transfers import add_transfernihss
        df = make_two_flag_df(spark, [(None, None)], "fromnihss", "tonihss")
        assert add_transfernihss(df).collect()[0]["transfernihss"] is None

    def test_from_zero_is_preserved(self, spark):
        # 0 is non-null, so the .otherwise branch keeps the fromnihss value
        # rather than reading tonihss.
        from cms.transfers import add_transfernihss
        df = make_two_flag_df(spark, [(0, 30)], "fromnihss", "tonihss")
        assert add_transfernihss(df).collect()[0]["transfernihss"] == 0


class TestAddTransfernihssGroup:

    def test_from_null_falls_back_to_to(self, spark):
        from cms.transfers import add_transfernihssGroup
        df = make_two_flag_df(spark, [(None, 2)], "fromnihssGroup", "tonihssGroup")
        assert add_transfernihssGroup(df).collect()[0]["transfernihssGroup"] == 2

    def test_from_non_null_wins(self, spark):
        from cms.transfers import add_transfernihssGroup
        df = make_two_flag_df(spark, [(1, 4)], "fromnihssGroup", "tonihssGroup")
        assert add_transfernihssGroup(df).collect()[0]["transfernihssGroup"] == 1

    def test_both_null(self, spark):
        from cms.transfers import add_transfernihssGroup
        df = make_two_flag_df(spark, [(None, None)], "fromnihssGroup", "tonihssGroup")
        assert add_transfernihssGroup(df).collect()[0]["transfernihssGroup"] is None


# ============================================================
# Tests for add_firstTransfer — first transfer per beneficiary,
# defined by the minimum fromTHRU_DT_DAY over a fromDSYSRTKY window.
# ============================================================

def _first_transfer_schema():
    return StructType([
        StructField("fromDSYSRTKY", IntegerType(), True),
        StructField("fromTHRU_DT_DAY", IntegerType(), True),
        StructField("fromCLAIMNO", StringType(), True),
    ])


def make_first_transfer_df(spark, rows):
    """rows: list of (fromDSYSRTKY, fromTHRU_DT_DAY, fromCLAIMNO) tuples."""
    data = [{"fromDSYSRTKY": d, "fromTHRU_DT_DAY": t, "fromCLAIMNO": c} for d, t, c in rows]
    return spark.createDataFrame(data, schema=_first_transfer_schema())


class TestAddFirstTransfer:

    def test_single_transfer_is_first(self, spark):
        from cms.transfers import add_firstTransfer
        df = make_first_transfer_df(spark, [(1, 100, "a1")])
        assert add_firstTransfer(df).collect()[0]["firstTransfer"] == 1

    def test_earliest_thru_dt_marked_first(self, spark):
        from cms.transfers import add_firstTransfer
        df = make_first_transfer_df(spark, [
            (1, 100, "a1"),
            (1, 200, "a2"),
            (1, 300, "a3"),
        ])
        out = {r["fromCLAIMNO"]: r["firstTransfer"] for r in add_firstTransfer(df).collect()}
        assert out == {"a1": 1, "a2": 0, "a3": 0}

    def test_per_beneficiary_independence(self, spark):
        # Two beneficiaries; each gets their own "first transfer" mark.
        from cms.transfers import add_firstTransfer
        df = make_first_transfer_df(spark, [
            (1, 200, "a1"),
            (1, 300, "a2"),
            (2, 50,  "b1"),
            (2, 60,  "b2"),
        ])
        out = {r["fromCLAIMNO"]: r["firstTransfer"] for r in add_firstTransfer(df).collect()}
        assert out == {"a1": 1, "a2": 0, "b1": 1, "b2": 0}

    def test_ties_both_marked_first(self, spark):
        # Two transfers on the same earliest day -> both equal the min, both are 1.
        from cms.transfers import add_firstTransfer
        df = make_first_transfer_df(spark, [
            (1, 100, "a1"),
            (1, 100, "a2"),
            (1, 200, "a3"),
        ])
        out = {r["fromCLAIMNO"]: r["firstTransfer"] for r in add_firstTransfer(df).collect()}
        assert out == {"a1": 1, "a2": 1, "a3": 0}


# ============================================================
# Helper for closest-claim / clean-transfers / get_clean_transfers
# tests. These functions operate on a DF that already has from*
# and to* prefixed columns (i.e. after the join in get_transfers).
# ============================================================

def _closest_schema():
    return StructType([
        StructField("fromDSYSRTKY", IntegerType(), True),
        StructField("fromCLAIMNO", StringType(), True),
        StructField("fromTHRU_DT_DAY", IntegerType(), True),
        StructField("toCLAIMNO", StringType(), True),
        StructField("toADMSN_DT_DAY", IntegerType(), True),
    ])


def make_closest_df(spark, rows):
    """rows: list of (fromDSYSRTKY, fromCLAIMNO, fromTHRU_DT_DAY, toCLAIMNO, toADMSN_DT_DAY)."""
    data = [
        {"fromDSYSRTKY": d, "fromCLAIMNO": fc, "fromTHRU_DT_DAY": ft, "toCLAIMNO": tc, "toADMSN_DT_DAY": ta}
        for d, fc, ft, tc, ta in rows
    ]
    return spark.createDataFrame(data, schema=_closest_schema())


class TestGetClosestToClaim:

    def test_keeps_only_earliest_to_admission(self, spark):
        # One from-claim joined to three to-claims: the earliest toADMSN_DT_DAY survives.
        from cms.transfers import get_closest_to_claim
        df = make_closest_df(spark, [
            (1, "f1", 100, "t1", 101),
            (1, "f1", 100, "t2", 110),
            (1, "f1", 100, "t3", 120),
        ])
        kept = {r["toCLAIMNO"] for r in get_closest_to_claim(df).collect()}
        assert kept == {"t1"}

    def test_ties_keep_all_tied_rows(self, spark):
        # Two to-claims share the earliest toADMSN_DT_DAY; both are kept.
        from cms.transfers import get_closest_to_claim
        df = make_closest_df(spark, [
            (1, "f1", 100, "t1", 101),
            (1, "f1", 100, "t2", 101),
            (1, "f1", 100, "t3", 110),
        ])
        kept = {r["toCLAIMNO"] for r in get_closest_to_claim(df).collect()}
        assert kept == {"t1", "t2"}

    def test_partition_independent_per_from_claim(self, spark):
        # Two from-claims with their own to-claims — each picks its own earliest.
        from cms.transfers import get_closest_to_claim
        df = make_closest_df(spark, [
            (1, "f1", 100, "t1", 105),
            (1, "f1", 100, "t2", 101),
            (2, "f2", 200, "t3", 210),
            (2, "f2", 200, "t4", 215),
        ])
        kept = {r["fromCLAIMNO"]: r["toCLAIMNO"] for r in get_closest_to_claim(df).collect()}
        assert kept == {"f1": "t2", "f2": "t3"}

    def test_helper_column_dropped(self, spark):
        from cms.transfers import get_closest_to_claim
        df = make_closest_df(spark, [(1, "f1", 100, "t1", 101)])
        result = get_closest_to_claim(df)
        assert "isClosestToClaim" not in result.columns


class TestGetClosestFromClaim:

    def test_keeps_only_latest_from_through(self, spark):
        # One to-claim joined to three from-claims: latest fromTHRU_DT_DAY survives.
        from cms.transfers import get_closest_from_claim
        df = make_closest_df(spark, [
            (1, "f1", 90,  "t1", 101),
            (1, "f2", 100, "t1", 101),
            (1, "f3", 80,  "t1", 101),
        ])
        kept = {r["fromCLAIMNO"] for r in get_closest_from_claim(df).collect()}
        assert kept == {"f2"}

    def test_ties_keep_all_tied_rows(self, spark):
        from cms.transfers import get_closest_from_claim
        df = make_closest_df(spark, [
            (1, "f1", 100, "t1", 101),
            (1, "f2", 100, "t1", 101),
            (1, "f3", 90,  "t1", 101),
        ])
        kept = {r["fromCLAIMNO"] for r in get_closest_from_claim(df).collect()}
        assert kept == {"f1", "f2"}

    def test_helper_column_dropped(self, spark):
        from cms.transfers import get_closest_from_claim
        df = make_closest_df(spark, [(1, "f1", 100, "t1", 101)])
        result = get_closest_from_claim(df)
        assert "isClosestFromClaim" not in result.columns


class TestRemoveUncertainTransfers:

    def test_drops_one_to_many_in_either_direction(self, spark):
        # f1 has 2 to-claims (uncertain). f2 has 1 to-claim but t-claim has 2 from-claims (uncertain).
        # f3 -> t3 is a clean 1:1 transfer and should survive.
        from cms.transfers import remove_uncertain_transfers
        df = make_closest_df(spark, [
            (1, "f1", 100, "t1", 101),
            (1, "f1", 100, "t2", 101),
            (1, "f2", 100, "t3", 110),
            (1, "f4", 100, "t3", 110),
            (1, "f3", 200, "t4", 210),
        ])
        survivors = {(r["fromCLAIMNO"], r["toCLAIMNO"]) for r in remove_uncertain_transfers(df).collect()}
        assert survivors == {("f3", "t4")}

    def test_clean_pairs_all_kept(self, spark):
        from cms.transfers import remove_uncertain_transfers
        df = make_closest_df(spark, [
            (1, "f1", 100, "t1", 110),
            (1, "f2", 200, "t2", 210),
        ])
        survivors = {(r["fromCLAIMNO"], r["toCLAIMNO"]) for r in remove_uncertain_transfers(df).collect()}
        assert survivors == {("f1", "t1"), ("f2", "t2")}

    def test_helper_columns_dropped(self, spark):
        from cms.transfers import remove_uncertain_transfers
        df = make_closest_df(spark, [(1, "f1", 100, "t1", 110)])
        result = remove_uncertain_transfers(df)
        assert "numberOfToClaimsForFromClaim" not in result.columns
        assert "numberOfFromClaimsForToClaim" not in result.columns


class TestGetCleanTransfers:

    def test_combines_closest_and_uncertain_filters(self, spark):
        # f1 joins to t1 (admit=101) and t2 (admit=110) -> closest=t1
        # Independently t1 is also joined to from-claim f9 with later fromTHRU_DT_DAY=105
        # -> closest from = f9; remove_uncertain then drops the f1->t1 row because
        # t1 still has two candidate from-claims after closest filtering.
        # Net: only the clean f3->t4 pair survives.
        from cms.transfers import get_clean_transfers
        df = make_closest_df(spark, [
            (1, "f1", 100, "t1", 101),
            (1, "f1", 100, "t2", 110),
            (1, "f9", 105, "t1", 101),
            (1, "f3", 200, "t4", 210),
        ])
        survivors = {(r["fromCLAIMNO"], r["toCLAIMNO"]) for r in get_clean_transfers(df).collect()}
        assert survivors == {("f9", "t1"), ("f3", "t4")}

    def test_helper_columns_dropped(self, spark):
        from cms.transfers import get_clean_transfers
        df = make_closest_df(spark, [(1, "f1", 100, "t1", 110)])
        result = get_clean_transfers(df)
        for c in ("isClosestToClaim", "isClosestFromClaim",
                  "numberOfToClaimsForFromClaim", "numberOfFromClaimsForToClaim"):
            assert c not in result.columns


# ============================================================
# Tests for add_dyad (column construction).
# ============================================================

class TestAddDyad:

    def test_dyad_array_structure(self, spark):
        from cms.transfers import add_dyad
        df = make_raw_transfers_df(spark, [(100, 200, 2020, 2020, "a1", "b1")])
        row = add_dyad(df).collect()[0]
        assert row["dyad"] == [100, 200, 2020]

    def test_same_pair_different_year_distinct(self, spark):
        from cms.transfers import add_dyad
        df = make_raw_transfers_df(spark, [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 200, 2021, 2021, "a2", "b2"),
        ])
        dyads = [tuple(r["dyad"]) for r in add_dyad(df).collect()]
        assert (100, 200, 2020) in dyads
        assert (100, 200, 2021) in dyads
        assert dyads[0] != dyads[1]

    def test_reverse_pair_distinct(self, spark):
        # add_dyad is direction-aware: A->B and B->A produce different dyad arrays.
        from cms.transfers import add_dyad
        df = make_raw_transfers_df(spark, [
            (100, 200, 2020, 2020, "a1", "b1"),
            (200, 100, 2020, 2020, "b2", "a2"),
        ])
        dyads = [tuple(r["dyad"]) for r in add_dyad(df).collect()]
        assert (100, 200, 2020) in dyads
        assert (200, 100, 2020) in dyads


# ============================================================
# Tests for add_node_volume_info — counts of transfers out of
# fromORGNPINM and in to toORGNPINM, partitioned by year.
# ============================================================

class TestAddNodeVolumeInfo:

    def test_single_node_pair(self, spark):
        from cms.transfers import add_node_volume_info
        df = make_raw_transfers_df(spark, [(100, 200, 2020, 2020, "a1", "b1")])
        row = add_node_volume_info(df).collect()[0]
        assert row["nodeOutVol"] == 1
        assert row["nodeInVol"] == 1

    def test_out_and_in_volumes_independent(self, spark):
        # Node A in 2020 sends 3 transfers (out=3). Node B in 2020 receives 2 (from A and C).
        from cms.transfers import add_node_volume_info
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),  # A->B
            (100, 300, 2020, 2020, "a2", "c1"),  # A->C
            (100, 400, 2020, 2020, "a3", "d1"),  # A->D
            (500, 200, 2020, 2020, "e1", "b2"),  # E->B
        ]
        df = make_raw_transfers_df(spark, rows)
        out = {(r["fromCLAIMNO"]): (r["nodeOutVol"], r["nodeInVol"])
               for r in add_node_volume_info(df).collect()}
        # Every row from A in 2020 sees nodeOutVol=3
        assert out["a1"][0] == 3
        assert out["a2"][0] == 3
        assert out["a3"][0] == 3
        # nodeInVol per row depends on the destination node
        assert out["a1"][1] == 2  # B receives 2
        assert out["a2"][1] == 1  # C receives 1
        assert out["a3"][1] == 1  # D receives 1
        assert out["e1"][1] == 2  # B receives 2

    def test_year_partitions_independent(self, spark):
        from cms.transfers import add_node_volume_info
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 200, 2020, 2020, "a2", "b2"),
            (100, 200, 2021, 2021, "a3", "b3"),
        ]
        df = make_raw_transfers_df(spark, rows)
        out = {r["fromCLAIMNO"]: (r["nodeOutVol"], r["nodeInVol"])
               for r in add_node_volume_info(df).collect()}
        assert out["a1"] == (2, 2)
        assert out["a2"] == (2, 2)
        assert out["a3"] == (1, 1)


# ============================================================
# Tests for add_dyadVi — same-system flag for the two providers.
# ============================================================

def _dyad_vi_schema():
    return StructType([
        StructField("fromproviderSysId", IntegerType(), True),
        StructField("toproviderSysId", IntegerType(), True),
    ])


def make_dyad_vi_df(spark, rows):
    """rows: list of (fromproviderSysId, toproviderSysId) tuples."""
    data = [{"fromproviderSysId": f, "toproviderSysId": t} for f, t in rows]
    return spark.createDataFrame(data, schema=_dyad_vi_schema())


class TestAddDyadVi:

    def test_same_sysid_returns_1(self, spark):
        from cms.transfers import add_dyadVi
        df = make_dyad_vi_df(spark, [(7, 7)])
        assert add_dyadVi(df).collect()[0]["dyadVi"] == 1

    def test_different_sysid_returns_0(self, spark):
        from cms.transfers import add_dyadVi
        df = make_dyad_vi_df(spark, [(7, 8)])
        assert add_dyadVi(df).collect()[0]["dyadVi"] == 0

    def test_null_either_side_returns_0(self, spark):
        # Per the function, only non-null + equal -> 1, everything else -> 0.
        from cms.transfers import add_dyadVi
        df = make_dyad_vi_df(spark, [(None, 7), (7, None), (None, None)])
        results = [r["dyadVi"] for r in add_dyadVi(df).collect()]
        assert results == [0, 0, 0]


# ============================================================
# Tests for add_dyad_evt_info and add_dyad_tpa_info.
# These compute sum/mean/max over a dyad partition, so we need
# rows with the dyad column already added.
# ============================================================

def _dyad_evt_schema():
    return StructType([
        StructField("fromORGNPINM", IntegerType(), True),
        StructField("toORGNPINM", IntegerType(), True),
        StructField("fromTHRU_DT_YEAR", IntegerType(), True),
        StructField("toevt", IntegerType(), True),
    ])


def _dyad_tpa_schema():
    return StructType([
        StructField("fromORGNPINM", IntegerType(), True),
        StructField("toORGNPINM", IntegerType(), True),
        StructField("fromTHRU_DT_YEAR", IntegerType(), True),
        StructField("transfertpa", IntegerType(), True),
    ])


class TestAddDyadEvtInfo:

    def test_aggregates_over_dyad(self, spark):
        # A->B 2020 with toevt values [1, 0, 1] => Vol=2, Mean=2/3, includes=1
        from cms.transfers import add_dyad, add_dyad_evt_info
        rows = [
            {"fromORGNPINM": 100, "toORGNPINM": 200, "fromTHRU_DT_YEAR": 2020, "toevt": 1},
            {"fromORGNPINM": 100, "toORGNPINM": 200, "fromTHRU_DT_YEAR": 2020, "toevt": 0},
            {"fromORGNPINM": 100, "toORGNPINM": 200, "fromTHRU_DT_YEAR": 2020, "toevt": 1},
        ]
        df = spark.createDataFrame(rows, schema=_dyad_evt_schema())
        df = add_dyad(df)
        df = add_dyad_evt_info(df)
        result = df.collect()
        for r in result:
            assert r["dyadEvtVol"] == 2
            assert r["dyadEvtMean"] == pytest.approx(2.0 / 3.0)
            assert r["dyadIncludesEvt"] == 1

    def test_no_evt_returns_zero(self, spark):
        from cms.transfers import add_dyad, add_dyad_evt_info
        rows = [{"fromORGNPINM": 100, "toORGNPINM": 200, "fromTHRU_DT_YEAR": 2020, "toevt": 0}]
        df = spark.createDataFrame(rows, schema=_dyad_evt_schema())
        df = add_dyad(df)
        df = add_dyad_evt_info(df)
        r = df.collect()[0]
        assert r["dyadEvtVol"] == 0
        assert r["dyadEvtMean"] == pytest.approx(0.0)
        assert r["dyadIncludesEvt"] == 0

    def test_partition_by_dyad_includes_year(self, spark):
        # Two years -> two dyad partitions.
        from cms.transfers import add_dyad, add_dyad_evt_info
        rows = [
            {"fromORGNPINM": 100, "toORGNPINM": 200, "fromTHRU_DT_YEAR": 2020, "toevt": 1},
            {"fromORGNPINM": 100, "toORGNPINM": 200, "fromTHRU_DT_YEAR": 2020, "toevt": 1},
            {"fromORGNPINM": 100, "toORGNPINM": 200, "fromTHRU_DT_YEAR": 2021, "toevt": 0},
        ]
        df = spark.createDataFrame(rows, schema=_dyad_evt_schema())
        df = add_dyad(df)
        df = add_dyad_evt_info(df)
        by_year = {r["fromTHRU_DT_YEAR"]: r["dyadEvtVol"] for r in df.collect()}
        assert by_year[2020] == 2
        assert by_year[2021] == 0


class TestAddDyadTpaInfo:

    def test_aggregates_over_dyad(self, spark):
        from cms.transfers import add_dyad, add_dyad_tpa_info
        rows = [
            {"fromORGNPINM": 100, "toORGNPINM": 200, "fromTHRU_DT_YEAR": 2020, "transfertpa": 1},
            {"fromORGNPINM": 100, "toORGNPINM": 200, "fromTHRU_DT_YEAR": 2020, "transfertpa": 1},
            {"fromORGNPINM": 100, "toORGNPINM": 200, "fromTHRU_DT_YEAR": 2020, "transfertpa": 0},
            {"fromORGNPINM": 100, "toORGNPINM": 200, "fromTHRU_DT_YEAR": 2020, "transfertpa": 0},
        ]
        df = spark.createDataFrame(rows, schema=_dyad_tpa_schema())
        df = add_dyad(df)
        df = add_dyad_tpa_info(df)
        for r in df.collect():
            assert r["dyadTpaVol"] == 2
            assert r["dyadTpaMean"] == pytest.approx(0.5)
            assert r["dyadIncludesTpa"] == 1


class TestAddDyadStrokeTreatmentInfo:
    """Composition: add_dyad_tpa_info + add_dyad_evt_info."""

    def test_adds_all_six_columns(self, spark):
        from cms.transfers import add_dyad, add_dyad_stroke_treatment_info
        schema = StructType([
            StructField("fromORGNPINM", IntegerType(), True),
            StructField("toORGNPINM", IntegerType(), True),
            StructField("fromTHRU_DT_YEAR", IntegerType(), True),
            StructField("toevt", IntegerType(), True),
            StructField("transfertpa", IntegerType(), True),
        ])
        rows = [{"fromORGNPINM": 100, "toORGNPINM": 200, "fromTHRU_DT_YEAR": 2020,
                 "toevt": 1, "transfertpa": 0}]
        df = spark.createDataFrame(rows, schema=schema)
        df = add_dyad(df)
        df = add_dyad_stroke_treatment_info(df)
        for c in ("dyadEvtVol", "dyadEvtMean", "dyadIncludesEvt",
                  "dyadTpaVol", "dyadTpaMean", "dyadIncludesTpa"):
            assert c in df.columns


# ============================================================
# Tests for add_dyadAcrossCounties / add_dyadAcrossStates.
# ============================================================

def _across_counties_schema():
    return StructType([
        StructField("fromproviderFIPS", StringType(), True),
        StructField("toproviderFIPS", StringType(), True),
    ])


def _across_states_schema():
    return StructType([
        StructField("fromproviderStateFIPS", StringType(), True),
        StructField("toproviderStateFIPS", StringType(), True),
    ])


class TestAddDyadAcrossCounties:

    def test_different_counties_returns_1(self, spark):
        from cms.transfers import add_dyadAcrossCounties
        rows = [{"fromproviderFIPS": "01001", "toproviderFIPS": "01003"}]
        df = spark.createDataFrame(rows, schema=_across_counties_schema())
        assert add_dyadAcrossCounties(df).collect()[0]["dyadAcrossCounties"] == 1

    def test_same_county_returns_0(self, spark):
        from cms.transfers import add_dyadAcrossCounties
        rows = [{"fromproviderFIPS": "01001", "toproviderFIPS": "01001"}]
        df = spark.createDataFrame(rows, schema=_across_counties_schema())
        assert add_dyadAcrossCounties(df).collect()[0]["dyadAcrossCounties"] == 0

    def test_null_either_side_returns_null(self, spark):
        from cms.transfers import add_dyadAcrossCounties
        rows = [
            {"fromproviderFIPS": None, "toproviderFIPS": "01001"},
            {"fromproviderFIPS": "01001", "toproviderFIPS": None},
            {"fromproviderFIPS": None, "toproviderFIPS": None},
        ]
        df = spark.createDataFrame(rows, schema=_across_counties_schema())
        results = [r["dyadAcrossCounties"] for r in add_dyadAcrossCounties(df).collect()]
        assert results == [None, None, None]


class TestAddDyadAcrossStates:

    def test_different_states_returns_1(self, spark):
        from cms.transfers import add_dyadAcrossStates
        rows = [{"fromproviderStateFIPS": "01", "toproviderStateFIPS": "02"}]
        df = spark.createDataFrame(rows, schema=_across_states_schema())
        assert add_dyadAcrossStates(df).collect()[0]["dyadAcrossStates"] == 1

    def test_same_state_returns_0(self, spark):
        from cms.transfers import add_dyadAcrossStates
        rows = [{"fromproviderStateFIPS": "01", "toproviderStateFIPS": "01"}]
        df = spark.createDataFrame(rows, schema=_across_states_schema())
        assert add_dyadAcrossStates(df).collect()[0]["dyadAcrossStates"] == 0

    def test_null_either_side_returns_null(self, spark):
        from cms.transfers import add_dyadAcrossStates
        rows = [
            {"fromproviderStateFIPS": None, "toproviderStateFIPS": "01"},
            {"fromproviderStateFIPS": "01", "toproviderStateFIPS": None},
            {"fromproviderStateFIPS": None, "toproviderStateFIPS": None},
        ]
        df = spark.createDataFrame(rows, schema=_across_states_schema())
        results = [r["dyadAcrossStates"] for r in add_dyadAcrossStates(df).collect()]
        assert results == [None, None, None]


# ============================================================
# Tests for add_node_stroke_treatment_info — window aggregations
# over (fromORGNPINM, fromTHRU_DT_YEAR) and (toORGNPINM, fromTHRU_DT_YEAR).
# ============================================================

def _node_stroke_schema():
    return StructType([
        StructField("fromORGNPINM", IntegerType(), True),
        StructField("toORGNPINM", IntegerType(), True),
        StructField("fromTHRU_DT_YEAR", IntegerType(), True),
        StructField("toTHRU_DT_YEAR", IntegerType(), True),
        StructField("toevt", IntegerType(), True),
        StructField("transfertpa", IntegerType(), True),
    ])


class TestAddNodeStrokeTreatmentInfo:

    def test_adds_eight_columns(self, spark):
        from cms.transfers import add_node_stroke_treatment_info
        rows = [{"fromORGNPINM": 100, "toORGNPINM": 200,
                 "fromTHRU_DT_YEAR": 2020, "toTHRU_DT_YEAR": 2020,
                 "toevt": 1, "transfertpa": 0}]
        df = spark.createDataFrame(rows, schema=_node_stroke_schema())
        result = add_node_stroke_treatment_info(df)
        for c in ("nodeFromEvtVol", "nodeFromTpaVol", "nodeFromEvtMean", "nodeFromTpaMean",
                  "nodeToEvtVol", "nodeToTpaVol", "nodeToEvtMean", "nodeToTpaMean"):
            assert c in result.columns

    def test_aggregations_per_provider(self, spark):
        # From-provider 100 in 2020 sends 3 transfers: toevt = [1, 0, 1], transfertpa = [1, 1, 0]
        from cms.transfers import add_node_stroke_treatment_info
        rows = [
            {"fromORGNPINM": 100, "toORGNPINM": 200, "fromTHRU_DT_YEAR": 2020, "toTHRU_DT_YEAR": 2020, "toevt": 1, "transfertpa": 1},
            {"fromORGNPINM": 100, "toORGNPINM": 200, "fromTHRU_DT_YEAR": 2020, "toTHRU_DT_YEAR": 2020, "toevt": 0, "transfertpa": 1},
            {"fromORGNPINM": 100, "toORGNPINM": 300, "fromTHRU_DT_YEAR": 2020, "toTHRU_DT_YEAR": 2020, "toevt": 1, "transfertpa": 0},
        ]
        df = spark.createDataFrame(rows, schema=_node_stroke_schema())
        result = add_node_stroke_treatment_info(df).collect()
        for r in result:
            assert r["nodeFromEvtVol"] == 2
            assert r["nodeFromTpaVol"] == 2
            assert r["nodeFromEvtMean"] == pytest.approx(2.0 / 3.0)
            assert r["nodeFromTpaMean"] == pytest.approx(2.0 / 3.0)


# ============================================================
# Tests for add_node_revenue_info.
# ============================================================

def _node_revenue_schema():
    return StructType([
        StructField("fromORGNPINM", IntegerType(), True),
        StructField("toORGNPINM", IntegerType(), True),
        StructField("fromTHRU_DT_YEAR", IntegerType(), True),
        StructField("toTHRU_DT_YEAR", IntegerType(), True),
        StructField("fromed", IntegerType(), True),
        StructField("fromct", IntegerType(), True),
        StructField("frommri", IntegerType(), True),
        StructField("toed", IntegerType(), True),
        StructField("toct", IntegerType(), True),
        StructField("tomri", IntegerType(), True),
    ])


class TestAddNodeRevenueInfo:

    def test_adds_twelve_columns(self, spark):
        from cms.transfers import add_node_revenue_info
        rows = [{"fromORGNPINM": 100, "toORGNPINM": 200,
                 "fromTHRU_DT_YEAR": 2020, "toTHRU_DT_YEAR": 2020,
                 "fromed": 1, "fromct": 0, "frommri": 0,
                 "toed": 1, "toct": 1, "tomri": 1}]
        df = spark.createDataFrame(rows, schema=_node_revenue_schema())
        result = add_node_revenue_info(df)
        for c in ("nodeFromEdVol", "nodeFromEdMean", "nodeFromCtVol", "nodeFromCtMean",
                  "nodeFromMriVol", "nodeFromMriMean",
                  "nodeToEdVol", "nodeToEdMean", "nodeToCtVol", "nodeToCtMean",
                  "nodeToMriVol", "nodeToMriMean"):
            assert c in result.columns

    def test_aggregations_per_provider(self, spark):
        # From-provider 100 in 2020 has 3 transfers; fromed = [1, 0, 1], frommri = [1, 1, 1]
        from cms.transfers import add_node_revenue_info
        rows = [
            {"fromORGNPINM": 100, "toORGNPINM": 200, "fromTHRU_DT_YEAR": 2020, "toTHRU_DT_YEAR": 2020,
             "fromed": 1, "fromct": 1, "frommri": 1, "toed": 0, "toct": 0, "tomri": 0},
            {"fromORGNPINM": 100, "toORGNPINM": 300, "fromTHRU_DT_YEAR": 2020, "toTHRU_DT_YEAR": 2020,
             "fromed": 0, "fromct": 0, "frommri": 1, "toed": 0, "toct": 0, "tomri": 0},
            {"fromORGNPINM": 100, "toORGNPINM": 400, "fromTHRU_DT_YEAR": 2020, "toTHRU_DT_YEAR": 2020,
             "fromed": 1, "fromct": 0, "frommri": 1, "toed": 0, "toct": 0, "tomri": 0},
        ]
        df = spark.createDataFrame(rows, schema=_node_revenue_schema())
        result = add_node_revenue_info(df).collect()
        for r in result:
            assert r["nodeFromEdVol"] == 2
            assert r["nodeFromCtVol"] == 1
            assert r["nodeFromMriVol"] == 3
            assert r["nodeFromEdMean"] == pytest.approx(2.0 / 3.0)
            assert r["nodeFromMriMean"] == pytest.approx(1.0)


# ============================================================
# Tests for add_node_from_to_info — set and size of partners
# per node and year.
# ============================================================

class TestAddNodeFromToInfo:

    def test_adds_four_columns(self, spark):
        from cms.transfers import add_node_from_to_info
        rows = [(100, 200, 2020, 2020, "a1", "b1")]
        df = make_raw_transfers_df(spark, rows)
        result = add_node_from_to_info(df)
        for c in ("nodeFromSetOfToNodes", "nodeFromSizeOfToNodes",
                  "nodeToSetOfFromNodes", "nodeToSizeOfFromNodes"):
            assert c in result.columns

    def test_set_collects_partners(self, spark):
        # From-provider 100 in 2020 sends to {200, 300, 400}
        # To-provider 200 in 2020 receives from {100, 500}
        from cms.transfers import add_node_from_to_info
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 300, 2020, 2020, "a2", "c1"),
            (100, 400, 2020, 2020, "a3", "d1"),
            (500, 200, 2020, 2020, "e1", "b2"),
        ]
        df = make_raw_transfers_df(spark, rows)
        result = add_node_from_to_info(df).collect()
        by_claim = {r["fromCLAIMNO"]: r for r in result}
        assert set(by_claim["a1"]["nodeFromSetOfToNodes"]) == {200, 300, 400}
        assert by_claim["a1"]["nodeFromSizeOfToNodes"] == 3
        assert set(by_claim["a1"]["nodeToSetOfFromNodes"]) == {100, 500}
        assert by_claim["a1"]["nodeToSizeOfFromNodes"] == 2

    def test_year_partitions_independent(self, spark):
        from cms.transfers import add_node_from_to_info
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 300, 2020, 2020, "a2", "c1"),
            (100, 200, 2021, 2021, "a3", "b2"),
        ]
        df = make_raw_transfers_df(spark, rows)
        result = add_node_from_to_info(df).collect()
        by_claim = {r["fromCLAIMNO"]: r for r in result}
        assert by_claim["a1"]["nodeFromSizeOfToNodes"] == 2
        assert by_claim["a2"]["nodeFromSizeOfToNodes"] == 2
        assert by_claim["a3"]["nodeFromSizeOfToNodes"] == 1


# ============================================================
# Tests for add_node_hhi_info (composition: add_nodeHhi + prior-year lag).
# ============================================================

class TestAddNodeHhiInfo:

    def test_columns_added(self, spark):
        from cms.transfers import (
            add_node_volume_info, add_dyad, add_dyadTransferVol,
            add_dyadProportionTransfersOut, add_node_hhi_info,
        )
        rows = [(100, 200, 2020, 2020, "a1", "b1")]
        df = make_raw_transfers_df(spark, rows)
        df = add_node_volume_info(df)
        df = add_dyad(df)
        df = add_dyadTransferVol(df)
        df = add_dyadProportionTransfersOut(df)
        df = add_node_hhi_info(df)
        assert "nodeHhi" in df.columns
        assert "nodeHhiPrior" in df.columns

    def test_prior_year_value_propagated(self, spark):
        # A has HHI=1.0 in both 2020 and 2021 -> nodeHhiPrior for 2021 is 1.0
        # and for 2020 is None (no prior).
        from cms.transfers import (
            add_node_volume_info, add_dyad, add_dyadTransferVol,
            add_dyadProportionTransfersOut, add_node_hhi_info,
        )
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 200, 2021, 2021, "a2", "b2"),
        ]
        df = make_raw_transfers_df(spark, rows)
        df = add_node_volume_info(df)
        df = add_dyad(df)
        df = add_dyadTransferVol(df)
        df = add_dyadProportionTransfersOut(df)
        df = add_node_hhi_info(df)
        out = {r["fromTHRU_DT_YEAR"]: r["nodeHhiPrior"] for r in df.collect()}
        assert out[2020] is None
        assert out[2021] == pytest.approx(1.0)

    def test_prior_carries_distinct_value_not_current(self, spark):
        # 2020: A -> B, A -> C (one each) -> HHI = 0.5
        # 2021: all of A's transfers go to B          -> HHI = 1.0
        # The prior for 2021 must be 0.5 (the 2020 value), NOT 1.0 (the current
        # year). The all-1.0 fixture in test_prior_year_value_propagated cannot
        # distinguish these two; this one can.
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 300, 2020, 2020, "a2", "c1"),
            (100, 200, 2021, 2021, "a3", "b2"),
            (100, 200, 2021, 2021, "a4", "b3"),
        ]
        df = _run_hhi_info_pipeline(spark, rows)
        out = {r["fromTHRU_DT_YEAR"]: r["nodeHhiPrior"] for r in df.collect()}
        assert out[2020] is None
        assert out[2021] == pytest.approx(0.5)

    def test_year_gap_yields_null_prior(self, spark):
        # A is present in 2020 and 2022 only (no 2021). add_column_prior keeps the
        # lagged value only when the year delta is exactly 1, so 2022's prior must
        # be None -- NOT 2020's HHI -- because the gap is 2 years.
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 300, 2020, 2020, "a2", "c1"),  # 2020 HHI = 0.5
            (100, 200, 2022, 2022, "a3", "b2"),  # 2022 HHI = 1.0, prior must be None
        ]
        df = _run_hhi_info_pipeline(spark, rows)
        out = {r["fromTHRU_DT_YEAR"]: r["nodeHhiPrior"] for r in df.collect()}
        assert out[2020] is None
        assert out[2022] is None

    def test_prior_is_isolated_per_node(self, spark):
        # Two sending nodes with different trajectories share one DataFrame.
        # Each node's prior must come from its own history, not the other node's.
        #   A: 2020 HHI=0.5 (B,C), 2021 HHI=1.0 (all B)   -> prior[2021]=0.5
        #   D: 2020 HHI=1.0 (all E), 2021 HHI=0.5 (E,F)   -> prior[2021]=1.0
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 300, 2020, 2020, "a2", "c1"),
            (100, 200, 2021, 2021, "a3", "b2"),
            (100, 200, 2021, 2021, "a4", "b3"),
            (400, 500, 2020, 2020, "d1", "e1"),
            (400, 500, 2020, 2020, "d2", "e2"),
            (400, 500, 2021, 2021, "d3", "e3"),
            (400, 600, 2021, 2021, "d4", "f1"),
        ]
        df = _run_hhi_info_pipeline(spark, rows)
        rows_out = df.collect()
        a_prior = {r["fromTHRU_DT_YEAR"]: r["nodeHhiPrior"]
                   for r in rows_out if r["fromORGNPINM"] == 100}
        d_prior = {r["fromTHRU_DT_YEAR"]: r["nodeHhiPrior"]
                   for r in rows_out if r["fromORGNPINM"] == 400}
        assert a_prior[2021] == pytest.approx(0.5)
        assert d_prior[2021] == pytest.approx(1.0)

    def test_prior_broadcast_uniform_across_rows_in_node_year(self, spark):
        # When a node-year has several transfer rows, add_column_prior broadcasts
        # the prior value to every row via max() over (who, when). Assert all 2021
        # rows for A carry the same nodeHhiPrior.
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 300, 2020, 2020, "a2", "c1"),  # 2020 HHI = 0.5
            (100, 200, 2021, 2021, "a3", "b2"),
            (100, 200, 2021, 2021, "a4", "b3"),
            (100, 300, 2021, 2021, "a5", "c2"),  # multiple 2021 rows
        ]
        df = _run_hhi_info_pipeline(spark, rows)
        priors_2021 = {r["nodeHhiPrior"]
                       for r in df.collect() if r["fromTHRU_DT_YEAR"] == 2021}
        assert len(priors_2021) == 1
        assert next(iter(priors_2021)) == pytest.approx(0.5)


# ============================================================
# Invariant / property tests for the HHI math. These do not check a
# single hard-coded value but the mathematical properties that make
# nodeHhi a genuine Herfindahl-Hirschman index.
# ============================================================

def _distinct_props_for(df, from_npi, year):
    """Return the set of distinct dyadProportionTransfersOut values for a
    (from_npi, year). Each destination dyad has one proportion, repeated on
    every transfer row of that dyad, so dedup recovers the per-destination shares."""
    rows = df.filter((df.fromORGNPINM == from_npi)
                     & (df.fromTHRU_DT_YEAR == year)).collect()
    # key by toORGNPINM to get one share per destination
    by_dest = {}
    for r in rows:
        by_dest[r["toORGNPINM"]] = r["dyadProportionTransfersOut"]
    return by_dest


class TestNodeHhiInvariants:

    def test_proportions_sum_to_one(self, spark):
        # The shares of transfers across destinations must sum to 1.0 for each
        # sending node-year -- this is what guarantees nodeHhi is a true HHI and
        # validates nodeOutVol as the correct denominator.
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 200, 2020, 2020, "a2", "b2"),
            (100, 200, 2020, 2020, "a3", "b3"),
            (100, 300, 2020, 2020, "a4", "c1"),
            (100, 400, 2020, 2020, "a5", "d1"),
        ]
        df = _run_hhi_pipeline(spark, rows)
        shares = _distinct_props_for(df, 100, 2020)
        assert sum(shares.values()) == pytest.approx(1.0)

    @pytest.mark.parametrize("rows,n_dest", [
        # single destination
        ([(100, 200, 2020, 2020, "a1", "b1")], 1),
        # two even
        ([(100, 200, 2020, 2020, "a1", "b1"),
          (100, 300, 2020, 2020, "a2", "c1")], 2),
        # three skewed
        ([(100, 200, 2020, 2020, "a1", "b1"),
          (100, 200, 2020, 2020, "a2", "b2"),
          (100, 300, 2020, 2020, "a3", "c1"),
          (100, 400, 2020, 2020, "a4", "d1")], 3),
        # four even
        ([(100, 200, 2020, 2020, "a1", "b1"),
          (100, 300, 2020, 2020, "a2", "c1"),
          (100, 400, 2020, 2020, "a3", "d1"),
          (100, 500, 2020, 2020, "a4", "e1")], 4),
    ])
    def test_hhi_within_one_over_n_and_one(self, spark, rows, n_dest):
        # For any distribution over N distinct destinations: 1/N <= HHI <= 1.
        df = _run_hhi_pipeline(spark, rows)
        hhi = _hhi_for(df, 100, 2020)
        assert (1.0 / n_dest) - 1e-9 <= hhi <= 1.0 + 1e-9

    def test_more_concentrated_has_higher_hhi(self, spark):
        # Same number of destinations (2) for both nodes, but A is evenly split
        # (HHI=0.5) and D is skewed 3:1 (HHI=0.625). More concentration -> higher HHI.
        rows = [
            # node A: 1 + 1 (even)
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 300, 2020, 2020, "a2", "c1"),
            # node D: 3 + 1 (skewed)
            (400, 200, 2020, 2020, "d1", "b2"),
            (400, 200, 2020, 2020, "d2", "b3"),
            (400, 200, 2020, 2020, "d3", "b4"),
            (400, 300, 2020, 2020, "d4", "c2"),
        ]
        df = _run_hhi_pipeline(spark, rows)
        even = _hhi_for(df, 100, 2020)
        skewed = _hhi_for(df, 400, 2020)
        assert even == pytest.approx(0.5)
        assert skewed == pytest.approx(0.625)
        assert skewed > even


# ============================================================
# Numerical robustness and determinism of the HHI computation.
# ============================================================

class TestNodeHhiNumerical:

    def test_repeating_fraction_seven_destinations(self, spark):
        # 7 destinations, one transfer each -> each share = 1/7 (a non-terminating
        # binary fraction). HHI = 7 * (1/7)^2 = 1/7. Confirms no exact-equality
        # fragility around repeating fractions.
        rows = [(100, 200 + i, 2020, 2020, f"a{i}", f"b{i}") for i in range(7)]
        df = _run_hhi_pipeline(spark, rows)
        assert _hhi_for(df, 100, 2020) == pytest.approx(1.0 / 7.0)

    def test_large_n_concentration_matches_closed_form(self, spark):
        # 50 transfers to B and 1 to C -> nodeOutVol = 51.
        # shares 50/51 and 1/51 -> HHI = (50/51)^2 + (1/51)^2, and <= 1.0.
        rows = ([(100, 200, 2020, 2020, f"a{i}", f"b{i}") for i in range(50)]
                + [(100, 300, 2020, 2020, "a50", "c1")])
        df = _run_hhi_pipeline(spark, rows)
        expected = (50.0 / 51.0) ** 2 + (1.0 / 51.0) ** 2
        hhi = _hhi_for(df, 100, 2020)
        assert hhi == pytest.approx(expected)
        assert hhi <= 1.0

    def test_dedup_is_order_independent(self, spark):
        # 4 transfers A->B + 1 A->C in 2020. The dyad-dedup window tie-breaks
        # arbitrarily among identical-year rows; because all rows in a dyad carry
        # the same proportion, HHI must be identical regardless of input row order.
        rows = [
            (100, 200, 2020, 2020, "a1", "b1"),
            (100, 200, 2020, 2020, "a2", "b2"),
            (100, 200, 2020, 2020, "a3", "b3"),
            (100, 200, 2020, 2020, "a4", "b4"),
            (100, 300, 2020, 2020, "a5", "c1"),
        ]
        hhi_forward = _hhi_for(_run_hhi_pipeline(spark, rows), 100, 2020)
        hhi_reversed = _hhi_for(_run_hhi_pipeline(spark, list(reversed(rows))), 100, 2020)
        # 4/5 and 1/5 -> 0.64 + 0.04 = 0.68
        assert hhi_forward == pytest.approx(0.68)
        assert hhi_reversed == pytest.approx(hhi_forward)


# ============================================================
# Tests for composition wrappers: add_dyad_info, add_node_info,
# add_node_and_dyad_info. These chain many primitives; verify the
# expected columns are present after the chain.
# ============================================================

class TestAddDyadInfo:

    def test_adds_expected_columns(self, spark):
        from cms.transfers import add_node_volume_info, add_dyad_info
        schema = StructType([
            StructField("fromORGNPINM", IntegerType(), True),
            StructField("toORGNPINM", IntegerType(), True),
            StructField("fromTHRU_DT_YEAR", IntegerType(), True),
            StructField("toTHRU_DT_YEAR", IntegerType(), True),
            StructField("fromCLAIMNO", StringType(), True),
            StructField("toCLAIMNO", StringType(), True),
            StructField("fromproviderSysId", IntegerType(), True),
            StructField("toproviderSysId", IntegerType(), True),
            StructField("fromproviderFIPS", StringType(), True),
            StructField("toproviderFIPS", StringType(), True),
            StructField("fromproviderStateFIPS", StringType(), True),
            StructField("toproviderStateFIPS", StringType(), True),
        ])
        rows = [{"fromORGNPINM": 100, "toORGNPINM": 200,
                 "fromTHRU_DT_YEAR": 2020, "toTHRU_DT_YEAR": 2020,
                 "fromCLAIMNO": "a1", "toCLAIMNO": "b1",
                 "fromproviderSysId": 7, "toproviderSysId": 7,
                 "fromproviderFIPS": "01001", "toproviderFIPS": "01001",
                 "fromproviderStateFIPS": "01", "toproviderStateFIPS": "01"}]
        df = spark.createDataFrame(rows, schema=schema)
        df = add_node_volume_info(df)  # supplies nodeOutVol/nodeInVol needed downstream
        df = add_dyad_info(df)
        for c in ("dyad", "dyadVi", "dyadTransferVol",
                  "dyadProportionTransfersOut", "dyadProportionTransfersIn",
                  "dyadAcrossCounties", "dyadAcrossStates",
                  "dyadProportionTransfersOutPrior", "dyadProportionTransfersInPrior"):
            assert c in df.columns

    def test_dyad_prior_is_keyed_on_the_dyad_pair_not_the_sender(self, spark):
        # Sender 100 sends to two receivers (200, 300) in both 2020 and 2021, with the
        # mix flipping between years. The prior-year column must carry each *dyad's* own
        # value a year earlier -- keying on the sender alone (the old bug) would broadcast
        # one arbitrary partner's value to both dyads and fail this test.
        from cms.transfers import add_node_volume_info, add_dyad_info
        schema = StructType([
            StructField("fromORGNPINM", IntegerType(), True),
            StructField("toORGNPINM", IntegerType(), True),
            StructField("fromTHRU_DT_YEAR", IntegerType(), True),
            StructField("toTHRU_DT_YEAR", IntegerType(), True),
            StructField("fromCLAIMNO", StringType(), True),
            StructField("toCLAIMNO", StringType(), True),
            StructField("fromproviderSysId", IntegerType(), True),
            StructField("toproviderSysId", IntegerType(), True),
            StructField("fromproviderFIPS", StringType(), True),
            StructField("toproviderFIPS", StringType(), True),
            StructField("fromproviderStateFIPS", StringType(), True),
            StructField("toproviderStateFIPS", StringType(), True),
        ])
        # (year, to, n_claims): 2020 -> 200 gets 3/4, 300 gets 1/4; 2021 flips.
        spec = [(2020, 200, 3), (2020, 300, 1), (2021, 200, 1), (2021, 300, 3)]
        rows, k = [], 0
        for year, to, n in spec:
            for _ in range(n):
                k += 1
                rows.append({"fromORGNPINM": 100, "toORGNPINM": to,
                             "fromTHRU_DT_YEAR": year, "toTHRU_DT_YEAR": year,
                             "fromCLAIMNO": f"f{k}", "toCLAIMNO": f"t{k}",
                             "fromproviderSysId": 7, "toproviderSysId": 7,
                             "fromproviderFIPS": "01001", "toproviderFIPS": "01001",
                             "fromproviderStateFIPS": "01", "toproviderStateFIPS": "01"})
        df = spark.createDataFrame(rows, schema=schema)
        df = add_node_volume_info(df)
        df = add_dyad_info(df)
        out = {(r["toORGNPINM"], r["fromTHRU_DT_YEAR"]):
               (r["dyadProportionTransfersOut"], r["dyadProportionTransfersOutPrior"])
               for r in df.collect()}
        # current-year proportions
        assert out[(200, 2020)][0] == 0.75 and out[(300, 2020)][0] == 0.25
        assert out[(200, 2021)][0] == 0.25 and out[(300, 2021)][0] == 0.75
        # 2020 is each dyad's first year -> no prior
        assert out[(200, 2020)][1] is None and out[(300, 2020)][1] is None
        # 2021 prior must equal the *same* dyad's 2020 value, not the partner's
        assert out[(200, 2021)][1] == 0.75
        assert out[(300, 2021)][1] == 0.25

    def test_year_gap_prior_is_zero_not_null(self, spark):
        # add_dyad_info passes gapFill=0 for the dyad proportions. A dyad row exists
        # only when the pair had >=1 transfer that year, so a >1-year gap means the
        # pair had zero transfers -- a true 0 numerator (0/nodeVol). The gap-year
        # prior must therefore be 0.0, while each dyad's first observed year stays
        # null and contiguous years still carry the real prior value.
        #   100->200: present 2020 and 2022 (GAP in 2021)
        #   100->300: present 2020 and 2021 (contiguous, sender stays active in 2021)
        from cms.transfers import add_node_volume_info, add_dyad_info
        schema = StructType([
            StructField("fromORGNPINM", IntegerType(), True),
            StructField("toORGNPINM", IntegerType(), True),
            StructField("fromTHRU_DT_YEAR", IntegerType(), True),
            StructField("toTHRU_DT_YEAR", IntegerType(), True),
            StructField("fromCLAIMNO", StringType(), True),
            StructField("toCLAIMNO", StringType(), True),
            StructField("fromproviderSysId", IntegerType(), True),
            StructField("toproviderSysId", IntegerType(), True),
            StructField("fromproviderFIPS", StringType(), True),
            StructField("toproviderFIPS", StringType(), True),
            StructField("fromproviderStateFIPS", StringType(), True),
            StructField("toproviderStateFIPS", StringType(), True),
        ])
        # (year, to): the (100->200) dyad skips 2021; (100->300) covers 2020 and 2021.
        spec = [(2020, 200), (2020, 300), (2021, 300), (2022, 200)]
        rows = []
        for k, (year, to) in enumerate(spec):
            rows.append({"fromORGNPINM": 100, "toORGNPINM": to,
                         "fromTHRU_DT_YEAR": year, "toTHRU_DT_YEAR": year,
                         "fromCLAIMNO": f"f{k}", "toCLAIMNO": f"t{k}",
                         "fromproviderSysId": 7, "toproviderSysId": 7,
                         "fromproviderFIPS": "01001", "toproviderFIPS": "01001",
                         "fromproviderStateFIPS": "01", "toproviderStateFIPS": "01"})
        df = spark.createDataFrame(rows, schema=schema)
        df = add_node_volume_info(df)
        df = add_dyad_info(df)
        out = {(r["toORGNPINM"], r["fromTHRU_DT_YEAR"]):
               (r["dyadProportionTransfersOutPrior"], r["dyadProportionTransfersInPrior"])
               for r in df.collect()}
        # (100->200): first year 2020 -> null prior; 2022 follows a 1-year gap -> gapFill 0.0
        assert out[(200, 2020)] == (None, None)
        assert out[(200, 2022)] == (0.0, 0.0)
        # (100->300): contiguous, so the real 2020 prior still flows to 2021 (not overwritten by 0)
        assert out[(300, 2020)] == (None, None)
        assert out[(300, 2021)][0] is not None and out[(300, 2021)][0] != 0.0


class TestAddNodeInfo:

    def test_adds_expected_columns(self, spark):
        # add_node_info = add_node_volume_info + add_node_from_to_info + add_node_revenue_info
        from cms.transfers import add_node_info
        schema = StructType([
            StructField("fromORGNPINM", IntegerType(), True),
            StructField("toORGNPINM", IntegerType(), True),
            StructField("fromTHRU_DT_YEAR", IntegerType(), True),
            StructField("toTHRU_DT_YEAR", IntegerType(), True),
            StructField("fromCLAIMNO", StringType(), True),
            StructField("toCLAIMNO", StringType(), True),
            StructField("fromed", IntegerType(), True),
            StructField("fromct", IntegerType(), True),
            StructField("frommri", IntegerType(), True),
            StructField("toed", IntegerType(), True),
            StructField("toct", IntegerType(), True),
            StructField("tomri", IntegerType(), True),
        ])
        rows = [{"fromORGNPINM": 100, "toORGNPINM": 200,
                 "fromTHRU_DT_YEAR": 2020, "toTHRU_DT_YEAR": 2020,
                 "fromCLAIMNO": "a1", "toCLAIMNO": "b1",
                 "fromed": 1, "fromct": 1, "frommri": 0,
                 "toed": 0, "toct": 0, "tomri": 1}]
        df = spark.createDataFrame(rows, schema=schema)
        df = add_node_info(df)
        for c in ("nodeOutVol", "nodeInVol",
                  "nodeFromSetOfToNodes", "nodeFromSizeOfToNodes",
                  "nodeToSetOfFromNodes", "nodeToSizeOfFromNodes",
                  "nodeFromEdVol", "nodeFromCtVol", "nodeFromMriVol",
                  "nodeToEdVol", "nodeToCtVol", "nodeToMriVol"):
            assert c in df.columns


class TestAddNodeAndDyadInfo:
    """End-to-end test of the top-level composition that get_transfers calls."""

    def test_adds_node_dyad_and_hhi_columns(self, spark):
        from cms.transfers import add_node_and_dyad_info
        schema = StructType([
            StructField("fromORGNPINM", IntegerType(), True),
            StructField("toORGNPINM", IntegerType(), True),
            StructField("fromTHRU_DT_YEAR", IntegerType(), True),
            StructField("toTHRU_DT_YEAR", IntegerType(), True),
            StructField("fromCLAIMNO", StringType(), True),
            StructField("toCLAIMNO", StringType(), True),
            StructField("fromed", IntegerType(), True),
            StructField("fromct", IntegerType(), True),
            StructField("frommri", IntegerType(), True),
            StructField("toed", IntegerType(), True),
            StructField("toct", IntegerType(), True),
            StructField("tomri", IntegerType(), True),
            StructField("fromproviderSysId", IntegerType(), True),
            StructField("toproviderSysId", IntegerType(), True),
            StructField("fromproviderFIPS", StringType(), True),
            StructField("toproviderFIPS", StringType(), True),
            StructField("fromproviderStateFIPS", StringType(), True),
            StructField("toproviderStateFIPS", StringType(), True),
        ])
        rows = [{"fromORGNPINM": 100, "toORGNPINM": 200,
                 "fromTHRU_DT_YEAR": 2020, "toTHRU_DT_YEAR": 2020,
                 "fromCLAIMNO": "a1", "toCLAIMNO": "b1",
                 "fromed": 1, "fromct": 1, "frommri": 0,
                 "toed": 0, "toct": 0, "tomri": 1,
                 "fromproviderSysId": 7, "toproviderSysId": 8,
                 "fromproviderFIPS": "01001", "toproviderFIPS": "01003",
                 "fromproviderStateFIPS": "01", "toproviderStateFIPS": "02"}]
        df = spark.createDataFrame(rows, schema=schema)
        df = add_node_and_dyad_info(df)
        for c in ("nodeOutVol", "nodeInVol",
                  "dyad", "dyadVi", "dyadTransferVol",
                  "dyadAcrossCounties", "dyadAcrossStates",
                  "nodeHhi", "nodeHhiPrior"):
            assert c in df.columns
        # Sanity: a single A->B transfer means everything out of A goes to B
        row = df.collect()[0]
        assert row["dyadVi"] == 0
        assert row["dyadAcrossCounties"] == 1
        assert row["dyadAcrossStates"] == 1
        assert row["nodeHhi"] == pytest.approx(1.0)


# ============================================================
# Test for add_stroke_info — composes transfer-level treatment
# flags with node and dyad aggregates.
# ============================================================

class TestAddStrokeInfo:

    def test_adds_all_expected_columns(self, spark):
        from cms.transfers import add_stroke_info
        schema = StructType([
            # transfer-level inputs
            StructField("fromORGNPINM", IntegerType(), True),
            StructField("toORGNPINM", IntegerType(), True),
            StructField("fromTHRU_DT_YEAR", IntegerType(), True),
            StructField("toTHRU_DT_YEAR", IntegerType(), True),
            StructField("fromCLAIMNO", StringType(), True),
            StructField("toCLAIMNO", StringType(), True),
            StructField("fromtpa", IntegerType(), True),
            StructField("totpa", IntegerType(), True),
            StructField("fromct", IntegerType(), True),
            StructField("toct", IntegerType(), True),
            StructField("frommri", IntegerType(), True),
            StructField("tomri", IntegerType(), True),
            StructField("fromnihss", IntegerType(), True),
            StructField("tonihss", IntegerType(), True),
            StructField("fromnihssGroup", IntegerType(), True),
            StructField("tonihssGroup", IntegerType(), True),
            StructField("toevt", IntegerType(), True),
            # downstream functions used inside add_stroke_info
            StructField("dyad", StringType(), True),  # placeholder, will be overwritten
        ])
        # add_stroke_info calls add_dyad_stroke_treatment_info which references the
        # dyad column. Build one explicitly via add_dyad before calling.
        from cms.transfers import add_dyad
        base_schema = StructType([f for f in schema.fields if f.name != "dyad"])
        rows = [{
            "fromORGNPINM": 100, "toORGNPINM": 200,
            "fromTHRU_DT_YEAR": 2020, "toTHRU_DT_YEAR": 2020,
            "fromCLAIMNO": "a1", "toCLAIMNO": "b1",
            "fromtpa": 1, "totpa": 0,
            "fromct": 0, "toct": 1,
            "frommri": 0, "tomri": 1,
            "fromnihss": None, "tonihss": 7,
            "fromnihssGroup": None, "tonihssGroup": 2,
            "toevt": 1,
        }]
        df = spark.createDataFrame(rows, schema=base_schema)
        df = add_dyad(df)
        df = add_stroke_info(df)
        for c in ("transfertpa", "transferct", "transfermri",
                  "transfernihss", "transfernihssGroup",
                  "nodeFromEvtVol", "nodeFromTpaVol", "nodeToEvtVol", "nodeToTpaVol",
                  "dyadEvtVol", "dyadTpaVol", "dyadIncludesEvt", "dyadIncludesTpa"):
            assert c in df.columns
        row = df.collect()[0]
        assert row["transfertpa"] == 1   # fromtpa=1
        assert row["transferct"] == 1    # toct=1
        assert row["transfermri"] == 1   # tomri=1
        assert row["transfernihss"] == 7  # fromnihss null -> takes tonihss
        assert row["transfernihssGroup"] == 2


# ============================================================
# End-to-end tests of get_transfers built from raw single-provider
# "stays" DataFrames. Each side starts as ipBase rows for ONE provider,
# is run through the upstream date pipeline (add_admission_date_info +
# add_through_date_info), and is enriched with per-stay revenue flags
# (ed/ct/mri) plus the provider-level attributes (providerSysId,
# providerFIPS, providerStateFIPS) that downstream functions read.
# ============================================================

def _make_real_claim_df(spark, claim_type, rows):
    """Build a DF using the production cms.schemas claim schema, padding
    unspecified fields with NULL. Duplicated locally to keep this file
    self-contained (mirrors the helper in test_base.py / test_stays.py)."""
    from cms.schemas import schemas
    schema = schemas[claim_type]
    field_names = [f.name for f in schema.fields]
    padded = [{name: r.get(name) for name in field_names} for r in rows]
    return spark.createDataFrame(padded, schema=schema)


def _build_single_provider_stays_df(spark, orgnpinm, sysid, fips, state_fips, stays):
    """Build a stays DataFrame for ONE provider, starting from raw ipBase rows.

    Pipeline:
        1) raw ipBase rows (DSYSRTKY, CLAIMNO, ORGNPINM, ADMSN_DT, THRU_DT)
        2) add_admission_date_info(claimType="ip")   -> ADMSN_DT_DAY, ADMSN_DT_YEAR, ADMSN_DT_MONTH
        3) add_through_date_info                      -> THRU_DT_DAY, THRU_DT_YEAR, THRU_DT_MONTH
        4) join in per-claim revenue flags (ed, ct, mri)
        5) tag every row with the provider-level attributes (sysid/fips/statefips)

    Args:
        spark: SparkSession
        orgnpinm: int provider NPI, broadcast to every stay in this DF
        sysid: providerSysId (int, broadcast)
        fips: providerFIPS (5-char county FIPS, broadcast)
        state_fips: providerStateFIPS (2-char state FIPS, broadcast)
        stays: list of dicts with keys DSYSRTKY, CLAIMNO, ADMSN_DT, THRU_DT
               and optional ed/ct/mri (default 0).
    """
    from cms.base import add_admission_date_info
    from cms.utilities import add_through_date_info

    raw = [
        {"DSYSRTKY": s["DSYSRTKY"], "CLAIMNO": s["CLAIMNO"],
         "ORGNPINM": orgnpinm,
         "ADMSN_DT": s["ADMSN_DT"], "THRU_DT": s["THRU_DT"]}
        for s in stays
    ]
    df = _make_real_claim_df(spark, "ipBase", raw)
    df = add_admission_date_info(df, claimType="ip")
    df = add_through_date_info(df)

    flags_rows = [{"DSYSRTKY": s["DSYSRTKY"], "CLAIMNO": s["CLAIMNO"],
                   "ed": s.get("ed", 0), "ct": s.get("ct", 0), "mri": s.get("mri", 0)}
                  for s in stays]
    flags_schema = StructType([
        StructField("DSYSRTKY", IntegerType(), True),
        StructField("CLAIMNO", IntegerType(), True),
        StructField("ed", IntegerType(), True),
        StructField("ct", IntegerType(), True),
        StructField("mri", IntegerType(), True),
    ])
    flags_df = spark.createDataFrame(flags_rows, schema=flags_schema)
    df = df.join(flags_df, on=["DSYSRTKY", "CLAIMNO"], how="inner")

    df = (df.withColumn("providerSysId", F.lit(sysid))
            .withColumn("providerFIPS", F.lit(fips))
            .withColumn("providerStateFIPS", F.lit(state_fips)))
    return df


def _stay(dsysrtky, claimno, admsn_dt, thru_dt, ed=0, ct=0, mri=0):
    """Compact helper to build a single stay row dict."""
    return {"DSYSRTKY": dsysrtky, "CLAIMNO": claimno,
            "ADMSN_DT": admsn_dt, "THRU_DT": thru_dt,
            "ed": ed, "ct": ct, "mri": mri}


class TestGetTransfersFromSingleProviderStays:
    """get_transfers takes two stay DataFrames (one per facility) and
    produces the transfer rows that connect them. Each test below builds
    both sides from raw ipBase rows for a single hospital and walks the
    real preprocessing pipeline before invoking get_transfers."""

    # Two providers in the same state/county (intra-system transfer)
    FROM_NPI = 100
    FROM_SYS = 7
    FROM_FIPS = "01001"
    FROM_STATE = "01"

    TO_NPI = 200
    TO_SYS = 8
    TO_FIPS = "01003"
    TO_STATE = "02"

    def _from_df(self, spark, stays, *, sysid=None, fips=None, state=None):
        return _build_single_provider_stays_df(
            spark, self.FROM_NPI,
            sysid if sysid is not None else self.FROM_SYS,
            fips if fips is not None else self.FROM_FIPS,
            state if state is not None else self.FROM_STATE,
            stays,
        )

    def _to_df(self, spark, stays, *, orgnpinm=None, sysid=None, fips=None, state=None):
        return _build_single_provider_stays_df(
            spark, orgnpinm if orgnpinm is not None else self.TO_NPI,
            sysid if sysid is not None else self.TO_SYS,
            fips if fips is not None else self.TO_FIPS,
            state if state is not None else self.TO_STATE,
            stays,
        )

    def test_single_transfer_same_day(self, spark):
        # Beneficiary 1 is discharged from A on 2020-01-15 and admitted to B
        # on the same day -> exactly one transfer row.
        from cms.transfers import get_transfers
        from_df = self._from_df(spark, [_stay(1, 11, 20200110, 20200115, ed=1)])
        to_df = self._to_df(spark, [_stay(1, 21, 20200115, 20200120, ct=1, mri=1)])
        result = get_transfers(from_df, to_df).collect()
        assert len(result) == 1
        row = result[0]
        assert row["fromDSYSRTKY"] == 1
        assert row["fromCLAIMNO"] == 11
        assert row["toCLAIMNO"] == 21
        assert row["fromORGNPINM"] == self.FROM_NPI
        assert row["toORGNPINM"] == self.TO_NPI
        assert row["firstTransfer"] == 1

    def test_next_day_admission_counted(self, spark):
        # Discharge from A on day X, admission to B on day X+1 is allowed
        # (join uses fromTHRU_DT_DAY >= toADMSN_DT_DAY - 1).
        from cms.transfers import get_transfers
        from_df = self._from_df(spark, [_stay(1, 11, 20200110, 20200115)])
        to_df = self._to_df(spark, [_stay(1, 21, 20200116, 20200120)])
        result = get_transfers(from_df, to_df).collect()
        assert len(result) == 1
        assert result[0]["toCLAIMNO"] == 21

    def test_two_day_gap_rejected(self, spark):
        # Discharge day vs admission day differ by 2 -> not a transfer.
        from cms.transfers import get_transfers
        from_df = self._from_df(spark, [_stay(1, 11, 20200110, 20200115)])
        to_df = self._to_df(spark, [_stay(1, 21, 20200117, 20200120)])
        assert get_transfers(from_df, to_df).count() == 0

    def test_different_beneficiaries_do_not_transfer(self, spark):
        # The join requires fromDSYSRTKY == toDSYSRTKY.
        from cms.transfers import get_transfers
        from_df = self._from_df(spark, [_stay(1, 11, 20200110, 20200115)])
        to_df = self._to_df(spark, [_stay(2, 21, 20200115, 20200120)])
        assert get_transfers(from_df, to_df).count() == 0

    def test_same_provider_on_both_sides_yields_no_transfers(self, spark):
        # If both stays DFs are at the same provider, the fromORGNPINM!=toORGNPINM
        # clause drops every potential match.
        from cms.transfers import get_transfers
        from_df = self._from_df(spark, [_stay(1, 11, 20200110, 20200115)])
        to_df = self._to_df(
            spark, [_stay(1, 21, 20200115, 20200120)],
            orgnpinm=self.FROM_NPI,  # both sides at provider 100
        )
        assert get_transfers(from_df, to_df).count() == 0

    def test_multiple_candidate_to_stays_keeps_closest(self, spark):
        # Beneficiary 1 discharges from A on 2020-01-15; two candidate stays
        # at B: admission on 01-15 (closer) and on 01-16 (later). get_clean_transfers
        # should keep only the earliest toADMSN_DT_DAY.
        from cms.transfers import get_transfers
        from_df = self._from_df(spark, [_stay(1, 11, 20200110, 20200115)])
        to_df = self._to_df(spark, [
            _stay(1, 21, 20200115, 20200120),  # earlier admission - kept
            _stay(1, 22, 20200116, 20200121),  # later admission   - dropped
        ])
        result = get_transfers(from_df, to_df).collect()
        assert len(result) == 1
        assert result[0]["toCLAIMNO"] == 21

    def test_multiple_candidate_from_stays_keeps_latest(self, spark):
        # Two candidate stays at A both end at/before B's admission;
        # remove_uncertain_transfers will drop the toCLAIMNO=21 row because it
        # still has two valid from-claims, leaving no transfers.
        from cms.transfers import get_transfers
        from_df = self._from_df(spark, [
            _stay(1, 11, 20200110, 20200114),  # earlier discharge
            _stay(1, 12, 20200111, 20200115),  # latest discharge
        ])
        to_df = self._to_df(spark, [_stay(1, 21, 20200115, 20200120)])
        result = get_transfers(from_df, to_df).collect()
        # After get_closest_from_claim, fromCLAIMNO=12 is the closest; the
        # f1->t1 row survives because each side now has a unique partner.
        assert len(result) == 1
        assert result[0]["fromCLAIMNO"] == 12

    def test_first_transfer_flag_per_beneficiary(self, spark):
        # One patient transfers twice in 2020 (mid-Jan and mid-Feb). The earlier
        # of the two transfers should be flagged firstTransfer=1.
        from cms.transfers import get_transfers
        from_df = self._from_df(spark, [
            _stay(1, 11, 20200110, 20200115),
            _stay(1, 12, 20200210, 20200215),
        ])
        to_df = self._to_df(spark, [
            _stay(1, 21, 20200115, 20200120),
            _stay(1, 22, 20200215, 20200220),
        ])
        result = get_transfers(from_df, to_df).collect()
        by_claim = {r["fromCLAIMNO"]: r["firstTransfer"] for r in result}
        assert by_claim == {11: 1, 12: 0}

    def test_node_and_dyad_volumes_populated(self, spark):
        # Three patients each transfer once from A to B in 2020 -> nodeOutVol = 3,
        # nodeInVol = 3, dyadTransferVol = 3, dyadProportion[Out|In] = 1.0.
        from cms.transfers import get_transfers
        from_df = self._from_df(spark, [
            _stay(1, 11, 20200110, 20200115),
            _stay(2, 12, 20200210, 20200215),
            _stay(3, 13, 20200310, 20200315),
        ])
        to_df = self._to_df(spark, [
            _stay(1, 21, 20200115, 20200120),
            _stay(2, 22, 20200215, 20200220),
            _stay(3, 23, 20200315, 20200320),
        ])
        result = get_transfers(from_df, to_df).collect()
        assert len(result) == 3
        for r in result:
            assert r["nodeOutVol"] == 3
            assert r["nodeInVol"] == 3
            assert r["dyadTransferVol"] == 3
            assert r["dyadProportionTransfersOut"] == pytest.approx(1.0)
            assert r["dyadProportionTransfersIn"] == pytest.approx(1.0)
            # Single-destination sending hospital -> HHI = 1
            assert r["nodeHhi"] == pytest.approx(1.0)

    def test_dyad_attribute_columns_propagated(self, spark):
        # Provider attributes attached to each stays DF flow through to the
        # transfer-level dyad flags.
        from cms.transfers import get_transfers
        from_df = self._from_df(spark, [_stay(1, 11, 20200110, 20200115)])
        to_df = self._to_df(spark, [_stay(1, 21, 20200115, 20200120)])
        row = get_transfers(from_df, to_df).collect()[0]
        # Same system flag: FROM_SYS=7, TO_SYS=8 -> not same system
        assert row["dyadVi"] == 0
        # Counties differ (01001 vs 01003)
        assert row["dyadAcrossCounties"] == 1
        # States differ (01 vs 02)
        assert row["dyadAcrossStates"] == 1

    def test_intra_system_intra_county_dyad(self, spark):
        # Same sysid, same county, same state -> dyadVi=1, both across=0.
        from cms.transfers import get_transfers
        from_df = self._from_df(spark, [_stay(1, 11, 20200110, 20200115)])
        to_df = self._to_df(
            spark, [_stay(1, 21, 20200115, 20200120)],
            sysid=self.FROM_SYS,  # same system as the from-side
            fips=self.FROM_FIPS,
            state=self.FROM_STATE,
        )
        row = get_transfers(from_df, to_df).collect()[0]
        assert row["dyadVi"] == 1
        assert row["dyadAcrossCounties"] == 0
        assert row["dyadAcrossStates"] == 0

    def test_year_partitions_isolate_node_and_dyad_aggregates(self, spark):
        # A transfers to B in both 2020 and 2021. Each year should have its own
        # nodeOutVol/nodeInVol/dyadTransferVol since add_dyad encodes the year.
        from cms.transfers import get_transfers
        from_df = self._from_df(spark, [
            _stay(1, 11, 20200110, 20200115),
            _stay(2, 12, 20210110, 20210115),
            _stay(3, 13, 20210210, 20210215),
        ])
        to_df = self._to_df(spark, [
            _stay(1, 21, 20200115, 20200120),
            _stay(2, 22, 20210115, 20210120),
            _stay(3, 23, 20210215, 20210220),
        ])
        result = get_transfers(from_df, to_df).collect()
        by_year = {r["fromTHRU_DT_YEAR"]: r for r in result}
        assert by_year[2020]["nodeOutVol"] == 1
        assert by_year[2020]["dyadTransferVol"] == 1
        # In 2021 there are 2 transfers between the same pair.
        rows_2021 = [r for r in result if r["fromTHRU_DT_YEAR"] == 2021]
        assert len(rows_2021) == 2
        for r in rows_2021:
            assert r["nodeOutVol"] == 2
            assert r["dyadTransferVol"] == 2

    def test_empty_to_dataframe_yields_no_transfers(self, spark):
        from cms.transfers import get_transfers
        from_df = self._from_df(spark, [_stay(1, 11, 20200110, 20200115)])
        to_df = self._to_df(spark, [])
        assert get_transfers(from_df, to_df).count() == 0

    def test_empty_from_dataframe_yields_no_transfers(self, spark):
        from cms.transfers import get_transfers
        from_df = self._from_df(spark, [])
        to_df = self._to_df(spark, [_stay(1, 21, 20200115, 20200120)])
        assert get_transfers(from_df, to_df).count() == 0

    def test_multi_year_multi_destination_hhi_and_prior_end_to_end(self, spark):
        # Full real-schema pipeline (raw ipBase -> get_transfers -> add_node_and_dyad_info)
        # spanning two consecutive years for one sending hospital A, with a
        # different destination mix each year:
        #   2020: A -> B (bene 1) and A -> C (bene 2)        -> 2 even dests -> HHI = 0.5
        #   2021: A -> B (benes 3 and 4)                     -> single dest  -> HHI = 1.0
        # Therefore nodeHhiPrior[2020] is None (first year) and nodeHhiPrior[2021] = 0.5.
        from cms.transfers import get_transfers
        C_NPI = 300
        from_df = self._from_df(spark, [
            _stay(1, 11, 20200110, 20200115),  # 2020 -> B
            _stay(2, 12, 20200210, 20200215),  # 2020 -> C
            _stay(3, 13, 20210110, 20210115),  # 2021 -> B
            _stay(4, 14, 20210210, 20210215),  # 2021 -> B
        ])
        to_b = self._to_df(spark, [
            _stay(1, 21, 20200115, 20200120),
            _stay(3, 23, 20210115, 20210120),
            _stay(4, 24, 20210215, 20210220),
        ])
        to_c = self._to_df(spark, [
            _stay(2, 22, 20200215, 20200220),
        ], orgnpinm=C_NPI)
        to_df = to_b.unionByName(to_c)

        result = get_transfers(from_df, to_df).collect()
        # one transfer per beneficiary
        assert len(result) == 4

        hhi_by_year = {}
        prior_by_year = {}
        for r in result:
            assert r["fromORGNPINM"] == self.FROM_NPI
            hhi_by_year.setdefault(r["fromTHRU_DT_YEAR"], set()).add(r["nodeHhi"])
            prior_by_year.setdefault(r["fromTHRU_DT_YEAR"], set()).add(r["nodeHhiPrior"])

        # each (node, year) carries a single HHI / prior value
        assert {v for s in hhi_by_year.values() for v in s} and all(len(s) == 1 for s in hhi_by_year.values())
        assert all(len(s) == 1 for s in prior_by_year.values())

        assert next(iter(hhi_by_year[2020])) == pytest.approx(0.5)
        assert next(iter(hhi_by_year[2021])) == pytest.approx(1.0)
        assert next(iter(prior_by_year[2020])) is None
        assert next(iter(prior_by_year[2021])) == pytest.approx(0.5)


# ============================================================
# Tests for the transfers-level add_prior_hospitalization_info,
# which wraps base.add_prior_hospitalization_info by temporarily
# renaming to* columns.
# ============================================================

class TestAddPriorHospitalizationInfoTransfers:

    def test_counts_prior_inpatient_stays(self, spark):
        # Transfer's to-claim admitted at day 1000; the same beneficiary had two
        # ip stays in the prior 12 months (day 900, day 850) and one outside the
        # window (day 600). Expected: hospitalizationsIn12Months == 2.
        from cms.transfers import add_prior_hospitalization_info
        transfers_schema = StructType([
            StructField("toDSYSRTKY", IntegerType(), True),
            StructField("toADMSN_DT_DAY", IntegerType(), True),
            StructField("toADMSN_DT_MONTH", IntegerType(), True),
            StructField("toCLAIMNO", IntegerType(), True),
            StructField("toffsFirstMonth", IntegerType(), True),
        ])
        transfers_rows = [{
            "toDSYSRTKY": 1, "toADMSN_DT_DAY": 1000,
            "toADMSN_DT_MONTH": 50, "toCLAIMNO": 1,
            "toffsFirstMonth": 1,  # plenty of FFS coverage
        }]
        transfers_df = spark.createDataFrame(transfers_rows, schema=transfers_schema)

        ip_schema = StructType([
            StructField("DSYSRTKY", IntegerType(), True),
            StructField("THRU_DT_DAY", IntegerType(), True),
        ])
        ip_rows = [
            {"DSYSRTKY": 1, "THRU_DT_DAY": 900},   # 100 days prior -> counted
            {"DSYSRTKY": 1, "THRU_DT_DAY": 850},   # 150 days prior -> counted
            {"DSYSRTKY": 1, "THRU_DT_DAY": 600},   # 400 days prior -> outside 365
        ]
        ip_df = spark.createDataFrame(ip_rows, schema=ip_schema)

        result = add_prior_hospitalization_info(transfers_df, ip_df).collect()
        assert len(result) == 1
        r = result[0]
        assert r["hospitalizationsIn12Months"] == 2
        assert r["hospitalizedIn12Months"] == 1
        assert r["hospitalizationsIn6Months"] == 2  # 100 and 150 days both <= 182
        assert r["hospitalizedIn6Months"] == 1
        # And the to* columns were restored.
        for c in ("toDSYSRTKY", "toADMSN_DT_DAY", "toADMSN_DT_MONTH", "toCLAIMNO", "toffsFirstMonth"):
            assert c in result[0].asDict()

    def test_insufficient_ffs_coverage_returns_null(self, spark):
        # ADMSN_DT_MONTH - ffsFirstMonth < 12 -> hospitalizationsIn12Months is null.
        from cms.transfers import add_prior_hospitalization_info
        transfers_schema = StructType([
            StructField("toDSYSRTKY", IntegerType(), True),
            StructField("toADMSN_DT_DAY", IntegerType(), True),
            StructField("toADMSN_DT_MONTH", IntegerType(), True),
            StructField("toCLAIMNO", IntegerType(), True),
            StructField("toffsFirstMonth", IntegerType(), True),
        ])
        transfers_rows = [{
            "toDSYSRTKY": 1, "toADMSN_DT_DAY": 1000,
            "toADMSN_DT_MONTH": 5, "toCLAIMNO": 1,
            "toffsFirstMonth": 1,  # only 4 months of coverage
        }]
        transfers_df = spark.createDataFrame(transfers_rows, schema=transfers_schema)
        ip_schema = StructType([
            StructField("DSYSRTKY", IntegerType(), True),
            StructField("THRU_DT_DAY", IntegerType(), True),
        ])
        ip_df = spark.createDataFrame([{"DSYSRTKY": 1, "THRU_DT_DAY": 900}], schema=ip_schema)
        r = add_prior_hospitalization_info(transfers_df, ip_df).collect()[0]
        assert r["hospitalizationsIn12Months"] is None
        assert r["hospitalizationsIn6Months"] is None

    def test_extra_to_columns_round_trip(self, spark):
        # baseF.add_prior_hospitalization_info reads only the five documented to-prefixed
        # columns. Additional to* columns (toORGNPINM, toevt here) must pass through with
        # original names and values, not be touched by the rename map.
        from cms.transfers import add_prior_hospitalization_info
        transfers_schema = StructType([
            StructField("toDSYSRTKY", IntegerType(), True),
            StructField("toADMSN_DT_DAY", IntegerType(), True),
            StructField("toADMSN_DT_MONTH", IntegerType(), True),
            StructField("toCLAIMNO", IntegerType(), True),
            StructField("toffsFirstMonth", IntegerType(), True),
            StructField("toORGNPINM", IntegerType(), True),
            StructField("toevt", IntegerType(), True),
        ])
        transfers_rows = [{
            "toDSYSRTKY": 1, "toADMSN_DT_DAY": 1000,
            "toADMSN_DT_MONTH": 50, "toCLAIMNO": 1,
            "toffsFirstMonth": 1,
            "toORGNPINM": 9001, "toevt": 1,
        }]
        transfers_df = spark.createDataFrame(transfers_rows, schema=transfers_schema)
        ip_schema = StructType([
            StructField("DSYSRTKY", IntegerType(), True),
            StructField("THRU_DT_DAY", IntegerType(), True),
        ])
        ip_df = spark.createDataFrame([{"DSYSRTKY": 1, "THRU_DT_DAY": 900}], schema=ip_schema)
        result = add_prior_hospitalization_info(transfers_df, ip_df).collect()
        assert len(result) == 1
        r = result[0]
        for c in ("toDSYSRTKY", "toADMSN_DT_DAY", "toADMSN_DT_MONTH", "toCLAIMNO",
                  "toffsFirstMonth", "toORGNPINM", "toevt"):
            assert c in r.asDict()
        assert r["toORGNPINM"] == 9001
        assert r["toevt"] == 1
        assert r["hospitalizationsIn12Months"] == 1


# ============================================================
# Test for the transfers-level add_days_at_home_info, which
# renames to* columns, delegates to base.add_days_at_home_info,
# then renames back.
# ============================================================

#the admissions in these tests sit on day ~1000, so this lastObservableDay leaves every window observed
_FULLY_OBSERVED_DAY = 100000


class TestAddDaysAtHomeInfoTransfers:

    def test_columns_added_and_to_prefix_restored(self, spark):
        # Minimal smoke test: the function should add homeDays90, homeDays365 etc.
        # AND preserve the to* column naming on the input columns.
        from cms.transfers import add_days_at_home_info
        # The base implementation reads (after the rename to strip the "to" prefix):
        # DSYSRTKY, CLAIMNO, ADMSN_DT_DAY, THRU_DT_DAY, DEATH_DT_DAY, STUS_CD,
        # 90DaysAfterAdmissionDateDead, 365DaysAfterAdmissionDateDead.
        transfers_schema = StructType([
            StructField("toDSYSRTKY", IntegerType(), True),
            StructField("toCLAIMNO", IntegerType(), True),
            StructField("toADMSN_DT_DAY", IntegerType(), True),
            StructField("toTHRU_DT_DAY", IntegerType(), True),
            StructField("toDEATH_DT_DAY", IntegerType(), True),
            StructField("toSTUS_CD", IntegerType(), True),
            StructField("to90DaysAfterAdmissionDateDead", IntegerType(), True),
            StructField("to365DaysAfterAdmissionDateDead", IntegerType(), True),
        ])
        transfers_rows = [{
            "toDSYSRTKY": 1, "toCLAIMNO": 1,
            "toADMSN_DT_DAY": 1000, "toTHRU_DT_DAY": 1005,
            "toDEATH_DT_DAY": None, "toSTUS_CD": 1,
            "to90DaysAfterAdmissionDateDead": 0,
            "to365DaysAfterAdmissionDateDead": 0,
        }]
        transfers_df = spark.createDataFrame(transfers_rows, schema=transfers_schema)
        small_schema = StructType([
            StructField("DSYSRTKY", IntegerType(), True),
            StructField("ADMSN_DT_DAY", IntegerType(), True),
            StructField("THRU_DT_DAY", IntegerType(), True),
        ])
        empty = spark.createDataFrame([], schema=small_schema)
        # lastObservableDay far past the admission day, so nothing is right-censored.
        result = add_days_at_home_info(transfers_df, empty, empty, empty, empty, _FULLY_OBSERVED_DAY)
        # to* columns should still be named to* after the round-trip.
        for c in ("toDSYSRTKY", "toCLAIMNO", "toADMSN_DT_DAY", "toTHRU_DT_DAY",
                  "toDEATH_DT_DAY", "toSTUS_CD",
                  "to90DaysAfterAdmissionDateDead",
                  "to365DaysAfterAdmissionDateDead"):
            assert c in result.columns
        # Days-at-home columns added.
        for c in ("homeDays90", "homeDays365",
                  "homeDays90Group", "homeDays365Group",
                  "homeDaysIndependent90", "homeDaysIndependent365",
                  "homeDaysIndependent90Group", "homeDaysIndependent365Group"):
            assert c in result.columns
        # With no SNF/HHA/hosp/ip stays, los is 0 -> homeDays90 = 90, homeDays365 = 365.
        row = result.collect()[0]
        assert row["homeDays90"] == 90
        assert row["homeDays365"] == 365

    def _days_at_home(self, spark, lastObservableDay, stus=1, death=None, dead90=0, dead365=0):
        """Run the transfers wrapper on one alive-at-discharge row admitted on day 1000, no facility stays."""
        from cms.transfers import add_days_at_home_info
        transfers_schema = StructType([
            StructField("toDSYSRTKY", IntegerType(), True),
            StructField("toCLAIMNO", IntegerType(), True),
            StructField("toADMSN_DT_DAY", IntegerType(), True),
            StructField("toTHRU_DT_DAY", IntegerType(), True),
            StructField("toDEATH_DT_DAY", IntegerType(), True),
            StructField("toSTUS_CD", IntegerType(), True),
            StructField("to90DaysAfterAdmissionDateDead", IntegerType(), True),
            StructField("to365DaysAfterAdmissionDateDead", IntegerType(), True),
        ])
        rows = [{"toDSYSRTKY": 1, "toCLAIMNO": 1, "toADMSN_DT_DAY": 1000, "toTHRU_DT_DAY": 1005,
                 "toDEATH_DT_DAY": death, "toSTUS_CD": stus,
                 "to90DaysAfterAdmissionDateDead": dead90, "to365DaysAfterAdmissionDateDead": dead365}]
        small_schema = StructType([
            StructField("DSYSRTKY", IntegerType(), True),
            StructField("ADMSN_DT_DAY", IntegerType(), True),
            StructField("THRU_DT_DAY", IntegerType(), True),
        ])
        empty = spark.createDataFrame([], schema=small_schema)
        result = add_days_at_home_info(spark.createDataFrame(rows, schema=transfers_schema),
                                       empty, empty, empty, empty, lastObservableDay=lastObservableDay)
        return result.collect()[0]

    def test_truncated_window_is_null(self, spark):
        # Admitted on day 1000, data ends on day 1050: neither the 90- nor the 365-day window is observed.
        row = self._days_at_home(spark, lastObservableDay=1050)
        for c in ("homeDays90", "homeDays365", "homeDaysIndependent90", "homeDaysIndependent365"):
            assert row[c] is None
        for c in ("homeDays90Group", "homeDays365Group",
                  "homeDaysIndependent90Group", "homeDaysIndependent365Group"):
            assert row[c] is None
        # The underlying losAt* counts must be NULL too, not filled 0, since the window is unobserved.
        for c in ("losAtall90", "losAtall365", "losAtallMinusHha90", "losAtallMinusHha365"):
            assert row[c] is None

    def test_partially_truncated_window_nulls_only_the_longer_horizon(self, spark):
        # Data ends on day 1100, so the 90-day window closes inside it but the 365-day window does not.
        row = self._days_at_home(spark, lastObservableDay=1100)
        assert row["homeDays90"] == 90
        assert row["homeDaysIndependent90"] == 90
        assert row["homeDays365"] is None
        assert row["homeDaysIndependent365"] is None
        # observed 90-day window with no facility stay -> count filled 0; unobserved 365-day window -> NULL.
        assert row["losAtall90"] == 0
        assert row["losAtall365"] is None

    def test_window_ending_exactly_on_last_observable_day_is_observed(self, spark):
        row = self._days_at_home(spark, lastObservableDay=1090)
        assert row["homeDays90"] == 90

    def test_fully_observed_window_matches_uncensored_result(self, spark):
        row = self._days_at_home(spark, lastObservableDay=2000)
        assert row["homeDays90"] == 90
        assert row["homeDays365"] == 365
        assert row["losAtall90"] == 0
        assert row["losAtall365"] == 0

    def test_died_in_visit_is_0_even_when_window_is_truncated(self, spark):
        # STUS_CD==20 settles the outcome on the spot, no follow-up needed.
        row = self._days_at_home(spark, lastObservableDay=1050, stus=20)
        assert row["homeDays90"] == 0
        assert row["homeDays365"] == 0

    def test_death_inside_truncated_window_is_computed(self, spark):
        # Died on day 1030, so every day of the window that could count is already observed:
        # the recorded death makes even the truncated window complete, so losAt is 0 rather than NULL.
        row = self._days_at_home(spark, lastObservableDay=1050, death=1030, dead90=1, dead365=1)
        assert row["homeDays90"] == 31   # 1030 - 1000 + 1, minus 0 facility days
        assert row["homeDays365"] == 31
        assert row["losAtall90"] == 0
        assert row["losAtall365"] == 0

    def test_end_to_end_eight_columns_sufficient_with_extras_round_tripping(self, spark):
        # Stronger end-to-end check that:
        #   1. baseF.add_days_at_home_info reads only the eight documented to-prefixed
        #      columns (DSYSRTKY, CLAIMNO, ADMSN_DT_DAY, THRU_DT_DAY, DEATH_DT_DAY,
        #      STUS_CD, 90DaysAfterAdmissionDateDead, 365DaysAfterAdmissionDateDead).
        #      If it consulted anything else, the AnalysisException would surface here.
        #   2. The losAtallMinusHha* (excludes HHA) vs losAtall* (includes HHA)
        #      distinction propagates correctly: HHA stays affect homeDaysIndependent*
        #      but not homeDays*.
        #   3. Arbitrary additional to-prefixed columns round-trip with their original
        #      names and values intact.
        from cms.transfers import add_days_at_home_info

        transfers_schema = StructType([
            StructField("toDSYSRTKY", IntegerType(), True),
            StructField("toCLAIMNO", IntegerType(), True),
            StructField("toADMSN_DT_DAY", IntegerType(), True),
            StructField("toTHRU_DT_DAY", IntegerType(), True),
            StructField("toDEATH_DT_DAY", IntegerType(), True),
            StructField("toSTUS_CD", IntegerType(), True),
            StructField("to90DaysAfterAdmissionDateDead", IntegerType(), True),
            StructField("to365DaysAfterAdmissionDateDead", IntegerType(), True),
            # Extra to* columns baseF does NOT read; must round-trip unchanged.
            StructField("toORGNPINM", IntegerType(), True),
            StructField("toevt", IntegerType(), True),
        ])
        transfers_rows = [
            # Patient 1: alive, no prior stays anywhere.
            {"toDSYSRTKY": 1, "toCLAIMNO": 11,
             "toADMSN_DT_DAY": 1000, "toTHRU_DT_DAY": 1005,
             "toDEATH_DT_DAY": None, "toSTUS_CD": 1,
             "to90DaysAfterAdmissionDateDead": 0,
             "to365DaysAfterAdmissionDateDead": 0,
             "toORGNPINM": 9001, "toevt": 1},
            # Patient 2: alive, 5-day SNF stay starting day 1010 (after to-discharge 1005).
            # SNF is in allMinusHha, so it shrinks both homeDays* and homeDaysIndependent*.
            {"toDSYSRTKY": 2, "toCLAIMNO": 22,
             "toADMSN_DT_DAY": 1000, "toTHRU_DT_DAY": 1005,
             "toDEATH_DT_DAY": None, "toSTUS_CD": 1,
             "to90DaysAfterAdmissionDateDead": 0,
             "to365DaysAfterAdmissionDateDead": 0,
             "toORGNPINM": 9002, "toevt": 0},
            # Patient 3: alive, 7-day HHA stay starting day 1010. HHA is excluded
            # from allMinusHha but included in all, so it shrinks homeDaysIndependent*
            # but not homeDays*.
            {"toDSYSRTKY": 3, "toCLAIMNO": 33,
             "toADMSN_DT_DAY": 1000, "toTHRU_DT_DAY": 1005,
             "toDEATH_DT_DAY": None, "toSTUS_CD": 1,
             "to90DaysAfterAdmissionDateDead": 0,
             "to365DaysAfterAdmissionDateDead": 0,
             "toORGNPINM": 9003, "toevt": 1},
        ]
        transfers_df = spark.createDataFrame(transfers_rows, schema=transfers_schema)

        small_schema = StructType([
            StructField("DSYSRTKY", IntegerType(), True),
            StructField("ADMSN_DT_DAY", IntegerType(), True),
            StructField("THRU_DT_DAY", IntegerType(), True),
        ])
        snf_df = spark.createDataFrame(
            [{"DSYSRTKY": 2, "ADMSN_DT_DAY": 1010, "THRU_DT_DAY": 1014}],
            schema=small_schema)
        hha_df = spark.createDataFrame(
            [{"DSYSRTKY": 3, "ADMSN_DT_DAY": 1010, "THRU_DT_DAY": 1016}],
            schema=small_schema)
        empty = spark.createDataFrame([], schema=small_schema)
        result = add_days_at_home_info(transfers_df, snf_df, hha_df, empty, empty, _FULLY_OBSERVED_DAY)

        for c in ("toDSYSRTKY", "toCLAIMNO", "toADMSN_DT_DAY", "toTHRU_DT_DAY",
                  "toDEATH_DT_DAY", "toSTUS_CD",
                  "to90DaysAfterAdmissionDateDead",
                  "to365DaysAfterAdmissionDateDead",
                  "toORGNPINM", "toevt"):
            assert c in result.columns, f"missing input column {c}"
        for c in ("homeDays90", "homeDays365",
                  "homeDays90Group", "homeDays365Group",
                  "homeDaysIndependent90", "homeDaysIndependent365",
                  "homeDaysIndependent90Group", "homeDaysIndependent365Group"):
            assert c in result.columns, f"missing output column {c}"

        rows = {r["toDSYSRTKY"]: r for r in result.collect()}

        # Patient 1: no prior stays. losAt* = 0.
        assert rows[1]["homeDays90"] == 90
        assert rows[1]["homeDays365"] == 365
        assert rows[1]["homeDaysIndependent90"] == 90
        assert rows[1]["homeDaysIndependent365"] == 365

        # Patient 2: SNF (5 days). losAtallMinusHha90/365 = losAtall90/365 = 5.
        assert rows[2]["homeDays90"] == 85
        assert rows[2]["homeDays365"] == 360
        assert rows[2]["homeDaysIndependent90"] == 85
        assert rows[2]["homeDaysIndependent365"] == 360

        # Patient 3: HHA (7 days). losAtallMinusHha90/365 = 0; losAtall90/365 = 7.
        assert rows[3]["homeDays90"] == 90
        assert rows[3]["homeDays365"] == 365
        assert rows[3]["homeDaysIndependent90"] == 83
        assert rows[3]["homeDaysIndependent365"] == 358

        # Extra to* columns round-trip with original values.
        assert rows[1]["toORGNPINM"] == 9001 and rows[1]["toevt"] == 1
        assert rows[2]["toORGNPINM"] == 9002 and rows[2]["toevt"] == 0
        assert rows[3]["toORGNPINM"] == 9003 and rows[3]["toevt"] == 1


# ============================================================
# End-to-end pipeline: raw ipBase + ipRevenue rows through the
# REAL revenue + get_stays chain into get_transfers.
#
# Distinct from TestGetTransfersFromSingleProviderStays above,
# which builds the stays DF by directly joining a per-claim
# ed/ct/mri flags DF into baseDF and skips:
#   - get_revenue_info -> get_revenue_summary -> get_claims
#   - propagate_stay_info / get_unique_stays
#   - add_provider_revenue_info (window sums over real flags)
#
# The helper deliberately runs only the dates + revenue +
# get_stays + provider_revenue_info chain. The clinical
# condition enrichers (add_septicShock, add_ishStroke, add_tpa,
# add_evt, ...) are already exercised end-to-end in test_base.py
# and test_stays.py and don't need to be retested here -- adding
# them to this helper produces a logical plan deep enough to
# saturate the catalyst optimizer at the default JVM heap.
# ============================================================

def _ip_base_row(dsysrtky, claimno, admsn_dt, thru_dt):
    """Minimal ipBase row. ORGNPINM is injected by the helper since each
    stays DF is built per-provider. PROVIDER defaults to "P1" so all rows
    for the same beneficiary/admission share the propagate_stay_info
    partition key (DSYSRTKY, PROVIDER, ORGNPINM, ADMSN_DT_DAY)."""
    return {
        "DSYSRTKY": dsysrtky,
        "CLAIMNO": claimno,
        "PROVIDER": "P1",
        "ADMSN_DT": admsn_dt,
        "THRU_DT": thru_dt,
    }


def _ip_rev_row(dsysrtky, claimno, thru_dt, rev_cntr=100):
    """Minimal ipRevenue row. REV_CNTR=100 by default falls outside every
    revenue range (ED 450-459, ICU 200-209, CT 350-359, MRI 610-619), so
    rows that don't care about revenue contribute 0 to every flag."""
    return {"DSYSRTKY": dsysrtky, "CLAIMNO": claimno,
            "THRU_DT": thru_dt, "REV_CNTR": rev_cntr}


def _real_pipeline_stays_for_provider(spark, *, orgnpinm, sysid, fips, state_fips,
                                      base_rows, rev_rows):
    """Build a stays DataFrame for ONE provider by running the revenue +
    get_stays chain from raw ipBase + ipRevenue rows.

    Stages, matching production for the integration links not otherwise
    covered by test_base.py / test_stays.py:
        1. ipBase -> add_through_date_info, add_admission_date_info("ip")
        2. ipRevenue -> get_revenue_info(inClaim=True) -> revenue summary
        3. get_stays(base, summary, "ip") -- get_claims + propagate_stay_info
           + get_unique_stays
        4. add_provider_revenue_info
        5. Tag with providerSysId / providerFIPS / providerStateFIPS so the
           downstream dyadVi / dyadAcrossCounties / dyadAcrossStates branches
           in get_transfers have inputs to read.

    base_rows: list of _ip_base_row dicts. ORGNPINM is injected by this
        helper (not by the caller) so a single provider's rows share an NPI.
    rev_rows: list of _ip_rev_row dicts. Caller must provide one rev row per
        base CLAIMNO (the left join in get_claims would otherwise leave
        revenue flags NULL, breaking add_provider_revenue_info's window sums).
    """
    from cms.utilities import add_through_date_info
    from cms.base import add_admission_date_info
    from cms.revenue import get_revenue_info
    from cms.stays import get_stays, add_provider_revenue_info

    base_rows = [dict(r, ORGNPINM=orgnpinm) for r in base_rows]
    baseDF = _make_real_claim_df(spark, "ipBase", base_rows)
    baseDF = add_through_date_info(baseDF)
    baseDF = add_admission_date_info(baseDF, "ip")

    revDF = _make_real_claim_df(spark, "ipRevenue", rev_rows)
    summary = get_revenue_info(revDF, inClaim=True)
    staysDF = get_stays(baseDF, summary, claimType="ip")
    staysDF = add_provider_revenue_info(staysDF)

    staysDF = (staysDF
               .withColumn("providerSysId", F.lit(sysid).cast("int"))
               .withColumn("providerFIPS", F.lit(fips).cast("string"))
               .withColumn("providerStateFIPS", F.lit(state_fips).cast("string")))
    return staysDF


class TestEndToEndBaseToTransfersPipeline:
    """Full-chain tests: raw ipBase + ipRevenue rows -> get_transfers.
    Complements TestGetTransfersFromSingleProviderStays, which bypasses the
    real revenue summarization (get_revenue_info -> get_revenue_summary ->
    get_claims) and the propagate_stay_info / get_unique_stays dedup step."""

    FROM_NPI = 100
    TO_NPI = 200

    def _from_stays(self, spark, base_rows, rev_rows):
        return _real_pipeline_stays_for_provider(
            spark, orgnpinm=self.FROM_NPI, sysid=7,
            fips="01001", state_fips="01",
            base_rows=base_rows, rev_rows=rev_rows,
        )

    def _to_stays(self, spark, base_rows, rev_rows):
        return _real_pipeline_stays_for_provider(
            spark, orgnpinm=self.TO_NPI, sysid=8,
            fips="01003", state_fips="02",
            base_rows=base_rows, rev_rows=rev_rows,
        )

    def test_revenue_pipeline_propagates_to_transfer(self, spark):
        # ED flag on a from-side ipRevenue row (REV_CNTR=455) must traverse
        # get_revenue_info -> get_revenue_summary -> get_claims ->
        # propagate_stay_info -> get_unique_stays -> add_provider_revenue_info
        # -> get_transfers, and land as fromed=1 / fromproviderEdVol=1.
        from cms.transfers import get_transfers
        from_df = self._from_stays(
            spark,
            base_rows=[_ip_base_row(1, 11, 20200110, 20200115)],
            rev_rows=[_ip_rev_row(1, 11, 20200115, rev_cntr=455)],
        )
        to_df = self._to_stays(
            spark,
            base_rows=[_ip_base_row(1, 21, 20200115, 20200120)],
            rev_rows=[_ip_rev_row(1, 21, 20200120)],
        )
        result = get_transfers(from_df, to_df).collect()
        assert len(result) == 1
        r = result[0]
        assert r["fromed"] == 1
        assert r["toed"] == 0
        assert r["fromproviderEdVol"] == 1
        assert r["fromproviderEdMean"] == pytest.approx(1.0)
        assert r["toproviderEdVol"] == 0

    def test_multi_claim_stay_uses_final_discharge_for_transfer(self, spark):
        # Regression test for commit 7eb53f1 (get_unique_stays picks
        # max(THRU_DT_DAY), not min(CLAIMNO)).
        # From-side: TWO ipBase claims for the SAME stay (same DSYSRTKY,
        # PROVIDER, ORGNPINM, ADMSN_DT). The interim claim has the SMALLER
        # CLAIMNO (50) and the EARLIER THRU_DT (20200112). The final claim
        # has the LARGER CLAIMNO (9999) and the LATER THRU_DT (20200120).
        # To-stay admits 20200121 (1 day after the FINAL discharge, 9 days
        # after the interim).
        #   Old min(CLAIMNO) rule -> interim row wins -> 9-day gap -> 0 transfers.
        #   New max(THRU_DT_DAY) rule -> final row wins -> 1-day gap -> 1 transfer.
        from cms.transfers import get_transfers
        from_df = self._from_stays(
            spark,
            base_rows=[
                _ip_base_row(1, 50, 20200110, 20200112),
                _ip_base_row(1, 9999, 20200110, 20200120),
            ],
            rev_rows=[
                _ip_rev_row(1, 50, 20200112),
                _ip_rev_row(1, 9999, 20200120),
            ],
        )
        to_df = self._to_stays(
            spark,
            base_rows=[_ip_base_row(1, 21, 20200121, 20200125)],
            rev_rows=[_ip_rev_row(1, 21, 20200125)],
        )
        result = get_transfers(from_df, to_df).collect()
        assert len(result) == 1
        r = result[0]
        assert r["fromCLAIMNO"] == 9999  # the FINAL (max THRU_DT_DAY) row
        assert r["fromTHRU_DT"] == 20200120
        assert r["fromTHRU_DT_YEAR"] == 2020

    def test_dyad_aggregation_with_multiple_destinations(self, spark):
        # 4 patients all leave hospital 100 in 2020: 3 go to hospital 200,
        # 1 goes to hospital 300. Verifies node/dyad partitioning when a
        # sending hospital has multiple destinations -- the existing
        # TestGetTransfersFromSingleProviderStays only covers a single
        # destination.
        #   nodeOutVol(100, 2020) = 4 on every row
        #   dyadTransferVol(100->200) = 3,  dyadProportionTransfersOut = 0.75
        #   dyadTransferVol(100->300) = 1,  dyadProportionTransfersOut = 0.25
        #   nodeHhi(100, 2020) = 0.75^2 + 0.25^2 = 0.625
        from cms.transfers import get_transfers
        from_df = self._from_stays(
            spark,
            base_rows=[
                _ip_base_row(1, 11, 20200110, 20200115),
                _ip_base_row(2, 12, 20200210, 20200215),
                _ip_base_row(3, 13, 20200310, 20200315),
                _ip_base_row(4, 14, 20200410, 20200415),
            ],
            rev_rows=[
                _ip_rev_row(1, 11, 20200115),
                _ip_rev_row(2, 12, 20200215),
                _ip_rev_row(3, 13, 20200315),
                _ip_rev_row(4, 14, 20200415),
            ],
        )
        to_b = _real_pipeline_stays_for_provider(
            spark, orgnpinm=200, sysid=8, fips="01003", state_fips="02",
            base_rows=[
                _ip_base_row(1, 21, 20200115, 20200120),
                _ip_base_row(2, 22, 20200215, 20200220),
                _ip_base_row(3, 23, 20200315, 20200320),
            ],
            rev_rows=[
                _ip_rev_row(1, 21, 20200120),
                _ip_rev_row(2, 22, 20200220),
                _ip_rev_row(3, 23, 20200320),
            ],
        )
        to_c = _real_pipeline_stays_for_provider(
            spark, orgnpinm=300, sysid=9, fips="01005", state_fips="03",
            base_rows=[_ip_base_row(4, 24, 20200415, 20200420)],
            rev_rows=[_ip_rev_row(4, 24, 20200420)],
        )
        to_df = to_b.unionByName(to_c)

        result = get_transfers(from_df, to_df).collect()
        assert len(result) == 4
        for r in result:
            assert r["nodeOutVol"] == 4
            assert r["nodeHhi"] == pytest.approx(0.625)
        by_to = {200: [], 300: []}
        for r in result:
            by_to[r["toORGNPINM"]].append(r)
        assert len(by_to[200]) == 3
        assert len(by_to[300]) == 1
        for r in by_to[200]:
            assert r["dyadTransferVol"] == 3
            assert r["dyadProportionTransfersOut"] == pytest.approx(0.75)
            assert r["dyadProportionTransfersIn"] == pytest.approx(1.0)
        for r in by_to[300]:
            assert r["dyadTransferVol"] == 1
            assert r["dyadProportionTransfersOut"] == pytest.approx(0.25)
            assert r["dyadProportionTransfersIn"] == pytest.approx(1.0)

    def test_propagate_stay_info_max_across_claims_within_stay(self, spark):
        # propagate_stay_info should max() the ICU flag across all claims of
        # the same stay. Setup: the SURVIVING (final) claim has REV_CNTR=100
        # (no icu); the DROPPED interim claim has REV_CNTR=200 (icu range).
        # Without propagation, the surviving stay would have icu=0; with
        # propagation it carries icu=1 from the interim sibling. The
        # transfer to hospital B should see fromicu=1.
        from cms.transfers import get_transfers
        from_df = self._from_stays(
            spark,
            base_rows=[
                _ip_base_row(1, 50, 20200110, 20200112),     # interim
                _ip_base_row(1, 9999, 20200110, 20200120),   # final (survives)
            ],
            rev_rows=[
                _ip_rev_row(1, 50, 20200112, rev_cntr=200),   # ICU on the interim
                _ip_rev_row(1, 9999, 20200120, rev_cntr=100), # no ICU on the final
            ],
        )
        to_df = self._to_stays(
            spark,
            base_rows=[_ip_base_row(1, 21, 20200121, 20200125)],
            rev_rows=[_ip_rev_row(1, 21, 20200125)],
        )
        result = get_transfers(from_df, to_df).collect()
        assert len(result) == 1
        r = result[0]
        assert r["fromCLAIMNO"] == 9999  # final row survived the dedup
        # If propagate_stay_info ran, icu was maxed onto the surviving row.
        assert r["fromicu"] == 1


# ============================================================
# Chunked end-to-end pipelines: each test class below adds ONE
# condition-specific base enricher group (~5 enricher calls,
# including the two date enrichers) on top of get_revenue_info
# -> get_stays -> get_transfers. Each helper rebuilds from raw
# ipBase rows so its catalyst plan stays shallow enough not to
# saturate the optimizer.
#
# Helpers that touch the 25-element array columns dgnsCodeAll /
# prcdrCodeAll (used by add_septicShock / add_tpa / add_evt)
# pay an extra round-trip through Parquet at the baseDF
# boundary -- the arrays_overlap / filter expressions on those
# columns + propagate_stay_info + get_transfers produce a plan
# catalyst can't process even at large heap, and
# localCheckpoint(eager=True) is not enough to break the
# lineage cleanly. The ishStroke chunk uses only scalar
# PRNCPAL_DGNS_CD / DRG_CD inputs and a single
# localCheckpoint suffices.
#
# Together the four classes cover the clinical enricher chain
# production uses for stroke / septic-shock transfer analyses.
# ============================================================

def _ip_base_row_with_codes(dsysrtky, claimno, admsn_dt, thru_dt, *,
                            prncpal_dgns_cd=None, drg_cd=None,
                            icd_dgns_cd1=None, icd_prcdr_cd1=None):
    """ipBase row with the optional code columns the clinical enrichers read.
    Distinct from _ip_base_row so the simpler revenue-only / multi-claim /
    dyad / propagate tests above keep their tight row shape."""
    return {
        "DSYSRTKY": dsysrtky,
        "CLAIMNO": claimno,
        "PROVIDER": "P1",
        "ADMSN_DT": admsn_dt,
        "THRU_DT": thru_dt,
        "PRNCPAL_DGNS_CD": prncpal_dgns_cd,
        "DRG_CD": drg_cd,
        "ICD_DGNS_CD1": icd_dgns_cd1,
        "ICD_PRCDR_CD1": icd_prcdr_cd1,
    }


def _setup_base_and_revenue(spark, *, orgnpinm, base_rows, rev_rows):
    """Shared head of each chunked pipeline: build baseDF with dates and
    revenue summary. Returns (baseDF_after_dates, revenue_summary). Caller
    adds its condition-specific enrichers in between."""
    from cms.utilities import add_through_date_info
    from cms.base import add_admission_date_info
    from cms.revenue import get_revenue_info

    base_rows = [dict(r, ORGNPINM=orgnpinm) for r in base_rows]
    baseDF = _make_real_claim_df(spark, "ipBase", base_rows)
    baseDF = add_through_date_info(baseDF)
    baseDF = add_admission_date_info(baseDF, "ip")
    revDF = _make_real_claim_df(spark, "ipRevenue", rev_rows)
    summary = get_revenue_info(revDF, inClaim=True)
    return baseDF, summary


def _tag_provider_attrs(staysDF, sysid, fips, state_fips):
    """Attach the provider attributes that get_transfers' dyad chain reads
    (dyadVi -> providerSysId, dyadAcrossCounties -> providerFIPS,
    dyadAcrossStates -> providerStateFIPS)."""
    return (staysDF
            .withColumn("providerSysId", F.lit(sysid).cast("int"))
            .withColumn("providerFIPS", F.lit(fips).cast("string"))
            .withColumn("providerStateFIPS", F.lit(state_fips).cast("string")))


# ============================================================
# Septic shock chunk: dates + dgnsCodeAll + add_septicShock
# -> get_stays -> add_provider_septic_shock_info -> get_transfers
# ============================================================

def _run_septic_shock_stays_for_provider(spark, *, orgnpinm, sysid, fips, state_fips,
                                         base_rows, rev_rows):
    from cms.base import add_dgnsCodeAll, add_septicShock
    from cms.stays import get_stays, add_provider_septic_shock_info

    baseDF, summary = _setup_base_and_revenue(
        spark, orgnpinm=orgnpinm, base_rows=base_rows, rev_rows=rev_rows)
    baseDF = add_dgnsCodeAll(baseDF)
    baseDF = add_septicShock(baseDF)
    # dgnsCodeAll's 25-element-array + arrays_overlap expression saturates
    # catalyst when combined with propagate_stay_info + get_transfers. Write
    # baseDF to Parquet and read it back: this is the most reliable way to
    # cut catalyst lineage cleanly. localCheckpoint(eager=True) is not
    # enough -- the optimizer still walks the array expression somehow.
    import tempfile, shutil, os as _os
    tmpdir = tempfile.mkdtemp(prefix="septic_base_")
    parquet_path = _os.path.join(tmpdir, "base.parquet")
    baseDF.drop("dgnsCodeAll").write.mode("overwrite").parquet(parquet_path)
    baseDF = spark.read.parquet(parquet_path)

    staysDF = get_stays(baseDF, summary, claimType="ip")
    staysDF = add_provider_septic_shock_info(staysDF)
    return _tag_provider_attrs(staysDF, sysid, fips, state_fips)


class TestSepticShockPipeline:
    """End-to-end: ICD_DGNS_CD1='R6521' on a base claim flows through
    add_dgnsCodeAll -> add_septicShock -> propagate_stay_info ->
    add_provider_septic_shock_info -> get_transfers."""

    FROM_NPI = 100
    TO_NPI = 200

    def _from(self, spark, base_rows, rev_rows):
        return _run_septic_shock_stays_for_provider(
            spark, orgnpinm=self.FROM_NPI, sysid=7,
            fips="01001", state_fips="01",
            base_rows=base_rows, rev_rows=rev_rows)

    def _to(self, spark, base_rows, rev_rows):
        return _run_septic_shock_stays_for_provider(
            spark, orgnpinm=self.TO_NPI, sysid=8,
            fips="01003", state_fips="02",
            base_rows=base_rows, rev_rows=rev_rows)

    def test_septic_shock_flag_propagates_to_transfer(self, spark):
        from cms.transfers import get_transfers
        from_df = self._from(
            spark,
            base_rows=[_ip_base_row_with_codes(1, 11, 20200110, 20200115,
                                               icd_dgns_cd1="R6521")],
            rev_rows=[_ip_rev_row(1, 11, 20200115)],
        )
        to_df = self._to(
            spark,
            base_rows=[_ip_base_row_with_codes(1, 21, 20200115, 20200120)],
            rev_rows=[_ip_rev_row(1, 21, 20200120)],
        )
        result = get_transfers(from_df, to_df).collect()
        assert len(result) == 1
        r = result[0]
        assert r["fromsepticShock"] == 1
        assert r["tosepticShock"] == 0
        assert r["fromproviderSepticShockAnnualVolume"] == 1
        assert r["toproviderSepticShockAnnualVolume"] == 0

    def test_provider_volume_aggregates_across_patients_in_year(self, spark):
        # 3 patients at hospital 100 in 2020: patients 1 & 2 are R6521 (septic
        # shock), patient 3 is not. Each transfers to hospital 200. After
        # provider_septic_shock_info windows the count over (ORGNPINM,
        # THRU_DT_YEAR), every from-row should see providerSepticShockAnnualVolume == 2.
        from cms.transfers import get_transfers
        from_df = self._from(
            spark,
            base_rows=[
                _ip_base_row_with_codes(1, 11, 20200110, 20200115,
                                        icd_dgns_cd1="R6521"),
                _ip_base_row_with_codes(2, 12, 20200210, 20200215,
                                        icd_dgns_cd1="R6521"),
                _ip_base_row_with_codes(3, 13, 20200310, 20200315,
                                        icd_dgns_cd1="I10"),
            ],
            rev_rows=[
                _ip_rev_row(1, 11, 20200115),
                _ip_rev_row(2, 12, 20200215),
                _ip_rev_row(3, 13, 20200315),
            ],
        )
        to_df = self._to(
            spark,
            base_rows=[
                _ip_base_row_with_codes(1, 21, 20200115, 20200120),
                _ip_base_row_with_codes(2, 22, 20200215, 20200220),
                _ip_base_row_with_codes(3, 23, 20200315, 20200320),
            ],
            rev_rows=[
                _ip_rev_row(1, 21, 20200120),
                _ip_rev_row(2, 22, 20200220),
                _ip_rev_row(3, 23, 20200320),
            ],
        )
        result = get_transfers(from_df, to_df).collect()
        assert len(result) == 3
        for r in result:
            assert r["fromproviderSepticShockAnnualVolume"] == 2
        septic_claims = {r["fromCLAIMNO"] for r in result if r["fromsepticShock"] == 1}
        assert septic_claims == {11, 12}

    def test_septic_shock_propagates_across_multi_claim_stay(self, spark):
        # Multi-claim stay at hospital 100 (same DSYSRTKY=1, PROVIDER='P1',
        # ORGNPINM=100, ADMSN_DT=20200110):
        #   Interim claim (CLAIMNO=50, THRU_DT=20200112) has ICD_DGNS_CD1='R6521' -> septicShock=1
        #   Final claim   (CLAIMNO=9999, THRU_DT=20200120) has ICD_DGNS_CD1='I10' -> septicShock=0
        # get_unique_stays picks the latest THRU_DT_DAY row (CLAIMNO=9999),
        # but propagate_stay_info maxes septicShock across the stay BEFORE
        # the dedup -- so the surviving row carries the flag from its
        # interim sibling.
        # Without propagate_stay_info, fromsepticShock would be 0 and
        # fromproviderSepticShockAnnualVolume would be 0.
        from cms.transfers import get_transfers
        from_df = self._from(
            spark,
            base_rows=[
                _ip_base_row_with_codes(1, 50, 20200110, 20200112,
                                        icd_dgns_cd1="R6521"),    # interim, septic shock
                _ip_base_row_with_codes(1, 9999, 20200110, 20200120,
                                        icd_dgns_cd1="I10"),       # final, no septic shock
            ],
            rev_rows=[
                _ip_rev_row(1, 50, 20200112),
                _ip_rev_row(1, 9999, 20200120),
            ],
        )
        to_df = self._to(
            spark,
            base_rows=[_ip_base_row_with_codes(1, 21, 20200121, 20200125)],
            rev_rows=[_ip_rev_row(1, 21, 20200125)],
        )
        result = get_transfers(from_df, to_df).collect()
        assert len(result) == 1
        r = result[0]
        # Final row survived the dedup (max THRU_DT_DAY).
        assert r["fromCLAIMNO"] == 9999
        # propagate_stay_info maxed septicShock onto the surviving row.
        assert r["fromsepticShock"] == 1
        # And the septic-shock annual-volume window sum (= 1 surviving stay
        # with septicShock=1) reflects the propagation.
        assert r["fromproviderSepticShockAnnualVolume"] == 1


# ============================================================
# Ischemic-stroke chunk: dates + add_ishStroke(inpatient=True)
# + add_anyStroke -> get_stays -> add_providerAnnualVolume(col="anyStroke") -> get_transfers
# ============================================================

def _run_ish_stroke_stays_for_provider(spark, *, orgnpinm, sysid, fips, state_fips,
                                       base_rows, rev_rows):
    from cms.base import add_ishStroke, add_anyStroke
    from cms.stays import get_stays, add_providerAnnualVolume

    baseDF, summary = _setup_base_and_revenue(
        spark, orgnpinm=orgnpinm, base_rows=base_rows, rev_rows=rev_rows)
    # add_ishStroke(inpatient=True) reads PRNCPAL_DGNS_CD and DRG_CD; adds
    # ishStrokeDgns + ishStrokeDrg + ishStroke (3 withColumns).
    baseDF = add_ishStroke(baseDF, inpatient=True)
    # add_anyStroke ORs whichever stroke flags are already present; with only
    # ishStroke set, anyStroke == ishStroke.
    baseDF = add_anyStroke(baseDF)
    baseDF = baseDF.localCheckpoint(eager=True)

    staysDF = get_stays(baseDF, summary, claimType="ip")
    staysDF = add_providerAnnualVolume(staysDF, col="anyStroke")
    return _tag_provider_attrs(staysDF, sysid, fips, state_fips)


class TestIshStrokePipeline:
    """End-to-end: PRNCPAL_DGNS_CD='I63...' OR DRG_CD in {61,62,63} flows
    through add_ishStroke -> add_anyStroke -> propagate_stay_info ->
    add_providerAnnualVolume(col="anyStroke") -> get_transfers. Both detection paths
    (diagnosis-driven and DRG-driven) are exercised."""

    FROM_NPI = 100
    TO_NPI = 200

    def _from(self, spark, base_rows, rev_rows):
        return _run_ish_stroke_stays_for_provider(
            spark, orgnpinm=self.FROM_NPI, sysid=7,
            fips="01001", state_fips="01",
            base_rows=base_rows, rev_rows=rev_rows)

    def _to(self, spark, base_rows, rev_rows):
        return _run_ish_stroke_stays_for_provider(
            spark, orgnpinm=self.TO_NPI, sysid=8,
            fips="01003", state_fips="02",
            base_rows=base_rows, rev_rows=rev_rows)

    def test_ish_stroke_via_principal_diagnosis(self, spark):
        from cms.transfers import get_transfers
        from_df = self._from(
            spark,
            base_rows=[_ip_base_row_with_codes(1, 11, 20200110, 20200115,
                                               prncpal_dgns_cd="I63")],
            rev_rows=[_ip_rev_row(1, 11, 20200115)],
        )
        to_df = self._to(
            spark,
            base_rows=[_ip_base_row_with_codes(1, 21, 20200115, 20200120)],
            rev_rows=[_ip_rev_row(1, 21, 20200120)],
        )
        result = get_transfers(from_df, to_df).collect()
        assert len(result) == 1
        r = result[0]
        assert r["fromishStrokeDgns"] == 1
        assert r["fromishStroke"] == 1
        assert r["fromanyStroke"] == 1
        assert r["fromproviderAnyStrokeAnnualVolume"] == 1
        assert r["toishStroke"] == 0
        assert r["toproviderAnyStrokeAnnualVolume"] == 0

    def test_ish_stroke_via_drg_code(self, spark):
        # DRG_CD 61 sets ishStrokeDrg=1 even with no PRNCPAL_DGNS_CD;
        # add_ishStroke(inpatient=True) ORs the two predicates.
        from cms.transfers import get_transfers
        from_df = self._from(
            spark,
            base_rows=[_ip_base_row_with_codes(1, 11, 20200110, 20200115,
                                               drg_cd=61)],
            rev_rows=[_ip_rev_row(1, 11, 20200115)],
        )
        to_df = self._to(
            spark,
            base_rows=[_ip_base_row_with_codes(1, 21, 20200115, 20200120)],
            rev_rows=[_ip_rev_row(1, 21, 20200120)],
        )
        result = get_transfers(from_df, to_df).collect()
        assert len(result) == 1
        r = result[0]
        assert r["fromishStrokeDgns"] == 0
        assert r["fromishStrokeDrg"] == 1
        assert r["fromishStroke"] == 1
        assert r["fromanyStroke"] == 1
        assert r["fromproviderAnyStrokeAnnualVolume"] == 1

    def test_ish_stroke_propagates_across_multi_claim_stay(self, spark):
        # Multi-claim stay at hospital 100 (same DSYSRTKY=1, ADMSN_DT=20200110):
        #   Interim (CLAIMNO=50, THRU_DT=20200112): PRNCPAL_DGNS_CD='I63'  -> ishStroke=1, anyStroke=1
        #   Final   (CLAIMNO=9999, THRU_DT=20200120): no stroke code        -> ishStroke=0, anyStroke=0
        # propagate_stay_info now uses schema-diff (any enricher-added
        # numeric column that's not in the raw ipBase schema), so
        # ishStrokeDgns / ishStroke / anyStroke ALL get maxed onto the
        # surviving final row -- not just the columns from the old
        # hardcoded allowlist. providerAnyStrokeAnnualVolume's window sum reflects
        # the propagated anyStroke.
        from cms.transfers import get_transfers
        from_df = self._from(
            spark,
            base_rows=[
                _ip_base_row_with_codes(1, 50, 20200110, 20200112,
                                        prncpal_dgns_cd="I63"),
                _ip_base_row_with_codes(1, 9999, 20200110, 20200120),
            ],
            rev_rows=[
                _ip_rev_row(1, 50, 20200112),
                _ip_rev_row(1, 9999, 20200120),
            ],
        )
        to_df = self._to(
            spark,
            base_rows=[_ip_base_row_with_codes(1, 21, 20200121, 20200125)],
            rev_rows=[_ip_rev_row(1, 21, 20200125)],
        )
        result = get_transfers(from_df, to_df).collect()
        assert len(result) == 1
        r = result[0]
        assert r["fromCLAIMNO"] == 9999  # final row survived the dedup
        # propagate_stay_info maxed each of these onto the surviving row.
        assert r["fromishStrokeDgns"] == 1
        assert r["fromishStroke"] == 1
        assert r["fromanyStroke"] == 1
        assert r["fromproviderAnyStrokeAnnualVolume"] == 1


# ============================================================
# tPA chunk: dates + add_dgnsCodeAll + add_prcdrCodeAll +
# add_tpa(inpatient=True) -> get_stays ->
# add_provider_stroke_treatment_info(inpatient=False) -> get_transfers
#
# inpatient=False on provider_stroke_treatment_info skips the
# evt mean/vol additions (those need an evt column we don't add
# in this chunk) and gives us only providerTpaMean / providerTpaVol.
# ============================================================

def _run_tpa_stays_for_provider(spark, *, orgnpinm, sysid, fips, state_fips,
                                base_rows, rev_rows):
    from cms.base import add_dgnsCodeAll, add_prcdrCodeAll, add_tpa
    from cms.stays import get_stays, add_provider_stroke_treatment_info

    baseDF, summary = _setup_base_and_revenue(
        spark, orgnpinm=orgnpinm, base_rows=base_rows, rev_rows=rev_rows)
    baseDF = add_dgnsCodeAll(baseDF)
    baseDF = add_prcdrCodeAll(baseDF)
    # add_tpa(inpatient=True) internally adds tpaPrcdr, tpaDgns, tpaDrg, tpa.
    baseDF = add_tpa(baseDF, inpatient=True)
    # The 25-element array cols (dgnsCodeAll, prcdrCodeAll) + their filter/
    # arrays_overlap expressions through propagate_stay_info + get_transfers
    # saturate catalyst even at large heap. Parquet round-trip cleanly cuts
    # lineage; localCheckpoint alone is not enough.
    import tempfile, os as _os
    tmpdir = tempfile.mkdtemp(prefix="tpa_base_")
    parquet_path = _os.path.join(tmpdir, "base.parquet")
    baseDF.drop("dgnsCodeAll", "prcdrCodeAll").write.mode("overwrite").parquet(parquet_path)
    baseDF = spark.read.parquet(parquet_path)

    staysDF = get_stays(baseDF, summary, claimType="ip")
    staysDF = add_provider_stroke_treatment_info(staysDF, inpatient=False)
    return _tag_provider_attrs(staysDF, sysid, fips, state_fips)


class TestTpaPipeline:
    """End-to-end: the three detection paths into add_tpa flow through to
    fromtpa and fromproviderTpaVol on the transfer:
      - ICD_PRCDR_CD = '3E03317'           (tpaPrcdr)
      - ICD_DGNS_CD = 'Z9282'              (tpaDgns)
      - DRG_CD in {61, 62, 63}             (tpaDrg)"""

    FROM_NPI = 100
    TO_NPI = 200

    def _from(self, spark, base_rows, rev_rows):
        return _run_tpa_stays_for_provider(
            spark, orgnpinm=self.FROM_NPI, sysid=7,
            fips="01001", state_fips="01",
            base_rows=base_rows, rev_rows=rev_rows)

    def _to(self, spark, base_rows, rev_rows):
        return _run_tpa_stays_for_provider(
            spark, orgnpinm=self.TO_NPI, sysid=8,
            fips="01003", state_fips="02",
            base_rows=base_rows, rev_rows=rev_rows)

    def test_tpa_via_procedure_code(self, spark):
        from cms.transfers import get_transfers
        from_df = self._from(
            spark,
            base_rows=[_ip_base_row_with_codes(1, 11, 20200110, 20200115,
                                               icd_prcdr_cd1="3E03317")],
            rev_rows=[_ip_rev_row(1, 11, 20200115)],
        )
        to_df = self._to(
            spark,
            base_rows=[_ip_base_row_with_codes(1, 21, 20200115, 20200120)],
            rev_rows=[_ip_rev_row(1, 21, 20200120)],
        )
        result = get_transfers(from_df, to_df).collect()
        assert len(result) == 1
        r = result[0]
        assert r["fromtpaPrcdr"] == 1
        assert r["fromtpa"] == 1
        assert r["fromproviderTpaVol"] == 1
        assert r["totpa"] == 0
        assert r["toproviderTpaVol"] == 0

    def test_tpa_via_drg_code(self, spark):
        # DRG 62 alone triggers tpaDrg=1 (and thus tpa=1) per add_tpaDrg's
        # tpaDrgCodes=[61,62,63] branch.
        from cms.transfers import get_transfers
        from_df = self._from(
            spark,
            base_rows=[_ip_base_row_with_codes(1, 11, 20200110, 20200115,
                                               drg_cd=62)],
            rev_rows=[_ip_rev_row(1, 11, 20200115)],
        )
        to_df = self._to(
            spark,
            base_rows=[_ip_base_row_with_codes(1, 21, 20200115, 20200120)],
            rev_rows=[_ip_rev_row(1, 21, 20200120)],
        )
        result = get_transfers(from_df, to_df).collect()
        assert len(result) == 1
        r = result[0]
        assert r["fromtpaPrcdr"] == 0
        assert r["fromtpaDrg"] == 1
        assert r["fromtpa"] == 1
        assert r["fromproviderTpaVol"] == 1

    def test_tpa_propagates_across_multi_claim_stay(self, spark):
        # Multi-claim stay at hospital 100 (same DSYSRTKY=1, ADMSN_DT=20200110):
        #   Interim (CLAIMNO=50, THRU_DT=20200112): ICD_PRCDR_CD1='3E03317' -> tpa=1
        #   Final   (CLAIMNO=9999, THRU_DT=20200120): no tpa codes           -> tpa=0
        # propagate_stay_info maxes tpa across the stay before dedup; the
        # surviving final row inherits the flag. providerTpaVol = sum(tpa)
        # over (ORGNPINM, year) = 1 because the surviving stay has tpa=1.
        # Without propagate, fromtpa=0 and fromproviderTpaVol=0.
        from cms.transfers import get_transfers
        from_df = self._from(
            spark,
            base_rows=[
                _ip_base_row_with_codes(1, 50, 20200110, 20200112,
                                        icd_prcdr_cd1="3E03317"),
                _ip_base_row_with_codes(1, 9999, 20200110, 20200120),
            ],
            rev_rows=[
                _ip_rev_row(1, 50, 20200112),
                _ip_rev_row(1, 9999, 20200120),
            ],
        )
        to_df = self._to(
            spark,
            base_rows=[_ip_base_row_with_codes(1, 21, 20200121, 20200125)],
            rev_rows=[_ip_rev_row(1, 21, 20200125)],
        )
        result = get_transfers(from_df, to_df).collect()
        assert len(result) == 1
        r = result[0]
        assert r["fromCLAIMNO"] == 9999  # final row survived the dedup
        # propagate_stay_info maxed tpa onto the surviving row.
        assert r["fromtpaPrcdr"] == 1
        assert r["fromtpa"] == 1
        assert r["fromproviderTpaVol"] == 1


# ============================================================
# EVT chunk: dates + add_prcdrCodeAll + add_ishStrokeDgns +
# add_evt -> get_stays -> get_transfers
#
# add_evt -> add_evtDrg -> add_ccvPrcdr (needs prcdrCodeAll)
# AND add_evtDrg also reads ishStrokeDgns, which add_evt does NOT
# build itself -- so the helper calls add_ishStrokeDgns first.
# Providers don't get a dedicated add_providerEvtVol in stays.py;
# add_provider_stroke_treatment_info bundles evt with tpa. To
# keep this chunk independent of tpa we compute providerEvtVol
# inline with a window sum (the same shape stays.py uses).
# ============================================================

def _run_evt_stays_for_provider(spark, *, orgnpinm, sysid, fips, state_fips,
                                base_rows, rev_rows):
    from pyspark.sql.window import Window
    from cms.base import add_prcdrCodeAll, add_ishStrokeDgns, add_evt
    from cms.stays import get_stays

    baseDF, summary = _setup_base_and_revenue(
        spark, orgnpinm=orgnpinm, base_rows=base_rows, rev_rows=rev_rows)
    baseDF = add_prcdrCodeAll(baseDF)
    baseDF = add_ishStrokeDgns(baseDF)
    # add_evt internally adds ccvPrcdr, evtDrg, evtPrcdr, evt.
    baseDF = add_evt(baseDF)
    # prcdrCodeAll's 25-element-array + arrays_overlap expressions through
    # propagate_stay_info + get_transfers saturate catalyst. Parquet
    # round-trip cleanly cuts lineage; localCheckpoint alone is not enough.
    import tempfile, os as _os
    tmpdir = tempfile.mkdtemp(prefix="evt_base_")
    parquet_path = _os.path.join(tmpdir, "base.parquet")
    baseDF.drop("prcdrCodeAll").write.mode("overwrite").parquet(parquet_path)
    baseDF = spark.read.parquet(parquet_path)

    staysDF = get_stays(baseDF, summary, claimType="ip")
    # No add_providerEvtVol exists in stays.py independently of tpa, so add
    # the window sum directly -- same partition shape as add_provider_*Vol.
    eachProvider = Window.partitionBy(["ORGNPINM", "THRU_DT_YEAR"])
    staysDF = staysDF.withColumn(
        "providerEvtVol", F.sum(F.col("evt")).over(eachProvider))
    return _tag_provider_attrs(staysDF, sysid, fips, state_fips)


class TestEvtPipeline:
    """End-to-end: the two detection paths into add_evt flow through to
    fromevt and fromproviderEvtVol on the transfer:
      - ICD_PRCDR_CD in the EVT thrombectomy code set ('03CG3ZZ' etc.) -> evtPrcdr
      - DRG_CD in {23,24} with PRNCPAL_DGNS_CD ~ I63 and no ccvPrcdr -> evtDrg"""

    FROM_NPI = 100
    TO_NPI = 200

    def _from(self, spark, base_rows, rev_rows):
        return _run_evt_stays_for_provider(
            spark, orgnpinm=self.FROM_NPI, sysid=7,
            fips="01001", state_fips="01",
            base_rows=base_rows, rev_rows=rev_rows)

    def _to(self, spark, base_rows, rev_rows):
        return _run_evt_stays_for_provider(
            spark, orgnpinm=self.TO_NPI, sysid=8,
            fips="01003", state_fips="02",
            base_rows=base_rows, rev_rows=rev_rows)

    def test_evt_via_procedure_code(self, spark):
        from cms.transfers import get_transfers
        from_df = self._from(
            spark,
            base_rows=[_ip_base_row_with_codes(1, 11, 20200110, 20200115,
                                               icd_prcdr_cd1="03CG3ZZ")],
            rev_rows=[_ip_rev_row(1, 11, 20200115)],
        )
        to_df = self._to(
            spark,
            base_rows=[_ip_base_row_with_codes(1, 21, 20200115, 20200120)],
            rev_rows=[_ip_rev_row(1, 21, 20200120)],
        )
        result = get_transfers(from_df, to_df).collect()
        assert len(result) == 1
        r = result[0]
        assert r["fromevtPrcdr"] == 1
        assert r["fromevt"] == 1
        assert r["fromproviderEvtVol"] == 1
        assert r["toevt"] == 0
        assert r["toproviderEvtVol"] == 0

    def test_evt_via_drg_with_ischemic_stroke(self, spark):
        # DRG 23 + ishStrokeDgns=1 (PRNCPAL_DGNS_CD='I63') + no ccvPrcdr -> evtDrg=1.
        from cms.transfers import get_transfers
        from_df = self._from(
            spark,
            base_rows=[_ip_base_row_with_codes(1, 11, 20200110, 20200115,
                                               prncpal_dgns_cd="I63",
                                               drg_cd=23)],
            rev_rows=[_ip_rev_row(1, 11, 20200115)],
        )
        to_df = self._to(
            spark,
            base_rows=[_ip_base_row_with_codes(1, 21, 20200115, 20200120)],
            rev_rows=[_ip_rev_row(1, 21, 20200120)],
        )
        result = get_transfers(from_df, to_df).collect()
        assert len(result) == 1
        r = result[0]
        assert r["fromevtDrg"] == 1
        assert r["fromevtPrcdr"] == 0
        assert r["fromevt"] == 1
        assert r["fromproviderEvtVol"] == 1

    def test_evt_propagates_across_multi_claim_stay(self, spark):
        # Multi-claim stay at hospital 100 (same DSYSRTKY=1, ADMSN_DT=20200110):
        #   Interim (CLAIMNO=50, THRU_DT=20200112): ICD_PRCDR_CD1='03CG3ZZ' -> evt=1
        #   Final   (CLAIMNO=9999, THRU_DT=20200120): no evt codes           -> evt=0
        # propagate_stay_info maxes evt (and evtPrcdr) across the stay
        # before dedup; the surviving final row inherits the flag.
        # providerEvtVol = sum(evt) over (ORGNPINM, year) = 1.
        # Without propagate, fromevt=0 and fromproviderEvtVol=0.
        from cms.transfers import get_transfers
        from_df = self._from(
            spark,
            base_rows=[
                _ip_base_row_with_codes(1, 50, 20200110, 20200112,
                                        icd_prcdr_cd1="03CG3ZZ"),
                _ip_base_row_with_codes(1, 9999, 20200110, 20200120),
            ],
            rev_rows=[
                _ip_rev_row(1, 50, 20200112),
                _ip_rev_row(1, 9999, 20200120),
            ],
        )
        to_df = self._to(
            spark,
            base_rows=[_ip_base_row_with_codes(1, 21, 20200121, 20200125)],
            rev_rows=[_ip_rev_row(1, 21, 20200125)],
        )
        result = get_transfers(from_df, to_df).collect()
        assert len(result) == 1
        r = result[0]
        assert r["fromCLAIMNO"] == 9999  # final row survived the dedup
        # propagate_stay_info maxed evt onto the surviving row.
        assert r["fromevtPrcdr"] == 1
        assert r["fromevt"] == 1
        assert r["fromproviderEvtVol"] == 1


# ============================================================
# Tests for get_edgeList / get_nodeListWithAddress — the graph
# edge list and node list extracted from a final transfers DF.
# These operate on a transfers-shaped DF that already carries the
# from*/to* node and provider-address columns (the post-pipeline
# grain). Extra columns (year, claim no) are included to verify
# they are ignored and do not affect distinctness.
# ============================================================

def _nodes_edges_schema():
    return StructType([
        StructField("fromORGNPINM", IntegerType(), True),
        StructField("toORGNPINM", IntegerType(), True),
        StructField("fromproviderAddress", StringType(), True),
        StructField("toproviderAddress", StringType(), True),
        StructField("fromTHRU_DT_YEAR", IntegerType(), True),
        StructField("fromCLAIMNO", StringType(), True),
    ])


def make_nodes_edges_df(spark, rows):
    """rows: list of
    (fromNPI, toNPI, fromAddress, toAddress, fromYear, fromClaim) tuples."""
    data = [
        {"fromORGNPINM": f, "toORGNPINM": t,
         "fromproviderAddress": fa, "toproviderAddress": ta,
         "fromTHRU_DT_YEAR": fy, "fromCLAIMNO": fc}
        for f, t, fa, ta, fy, fc in rows
    ]
    return spark.createDataFrame(data, schema=_nodes_edges_schema())


class TestGetEdgeList:

    def test_only_from_to_columns_returned(self, spark):
        from cms.transfers import get_edgeList
        df = make_nodes_edges_df(spark, [(100, 200, "addrA", "addrB", 2020, "a1")])
        result = get_edgeList(df)
        assert result.columns == ["fromORGNPINM", "toORGNPINM"]

    def test_single_edge(self, spark):
        from cms.transfers import get_edgeList
        df = make_nodes_edges_df(spark, [(100, 200, "addrA", "addrB", 2020, "a1")])
        edges = {(r["fromORGNPINM"], r["toORGNPINM"]) for r in get_edgeList(df).collect()}
        assert edges == {(100, 200)}

    def test_duplicate_transfers_collapse_to_one_edge(self, spark):
        # Three A->B transfers (distinct claims) -> a single edge.
        from cms.transfers import get_edgeList
        df = make_nodes_edges_df(spark, [
            (100, 200, "addrA", "addrB", 2020, "a1"),
            (100, 200, "addrA", "addrB", 2020, "a2"),
            (100, 200, "addrA", "addrB", 2020, "a3"),
        ])
        result = get_edgeList(df).collect()
        assert len(result) == 1
        assert (result[0]["fromORGNPINM"], result[0]["toORGNPINM"]) == (100, 200)

    def test_direction_aware(self, spark):
        # A->B and B->A are distinct directed edges, both retained.
        from cms.transfers import get_edgeList
        df = make_nodes_edges_df(spark, [
            (100, 200, "addrA", "addrB", 2020, "a1"),
            (200, 100, "addrB", "addrA", 2020, "b1"),
        ])
        edges = {(r["fromORGNPINM"], r["toORGNPINM"]) for r in get_edgeList(df).collect()}
        assert edges == {(100, 200), (200, 100)}

    def test_edge_list_is_year_less(self, spark):
        # Same pair across two years collapses to ONE edge -- unlike the dyad
        # column (which encodes the year), the edge list is structural only.
        from cms.transfers import get_edgeList
        df = make_nodes_edges_df(spark, [
            (100, 200, "addrA", "addrB", 2020, "a1"),
            (100, 200, "addrA", "addrB", 2021, "a2"),
        ])
        result = get_edgeList(df).collect()
        assert len(result) == 1
        assert (result[0]["fromORGNPINM"], result[0]["toORGNPINM"]) == (100, 200)

    def test_multiple_distinct_edges(self, spark):
        from cms.transfers import get_edgeList
        df = make_nodes_edges_df(spark, [
            (100, 200, "addrA", "addrB", 2020, "a1"),
            (100, 300, "addrA", "addrC", 2020, "a2"),
            (300, 200, "addrC", "addrB", 2020, "c1"),
            (300, 200, "addrC", "addrB", 2020, "c2"),  # duplicate of the edge above
        ])
        edges = {(r["fromORGNPINM"], r["toORGNPINM"]) for r in get_edgeList(df).collect()}
        assert edges == {(100, 200), (100, 300), (300, 200)}


class TestGetNodeListWithAddress:

    def test_columns_renamed_to_canonical(self, spark):
        from cms.transfers import get_nodeListWithAddress
        df = make_nodes_edges_df(spark, [(100, 200, "addrA", "addrB", 2020, "a1")])
        result = get_nodeListWithAddress(df)
        assert result.columns == ["ORGNPINM", "providerAddress"]

    def test_pools_from_and_to_sides(self, spark):
        # A single A->B transfer yields two nodes: A (from side) and B (to side).
        from cms.transfers import get_nodeListWithAddress
        df = make_nodes_edges_df(spark, [(100, 200, "addrA", "addrB", 2020, "a1")])
        nodes = {(r["ORGNPINM"], r["providerAddress"]) for r in get_nodeListWithAddress(df).collect()}
        assert nodes == {(100, "addrA"), (200, "addrB")}

    def test_to_only_provider_represented(self, spark):
        # C never appears on the from side, only as a destination -> still a node.
        from cms.transfers import get_nodeListWithAddress
        df = make_nodes_edges_df(spark, [
            (100, 200, "addrA", "addrB", 2020, "a1"),
            (100, 300, "addrA", "addrC", 2020, "a2"),
        ])
        nodes = {(r["ORGNPINM"], r["providerAddress"]) for r in get_nodeListWithAddress(df).collect()}
        assert nodes == {(100, "addrA"), (200, "addrB"), (300, "addrC")}

    def test_provider_on_both_sides_deduped(self, spark):
        # B appears as a destination (A->B) and as a source (B->C) with the same
        # address -> a single node row, not two.
        from cms.transfers import get_nodeListWithAddress
        df = make_nodes_edges_df(spark, [
            (100, 200, "addrA", "addrB", 2020, "a1"),
            (200, 300, "addrB", "addrC", 2020, "b1"),
        ])
        nodes = [(r["ORGNPINM"], r["providerAddress"]) for r in get_nodeListWithAddress(df).collect()]
        assert sorted(nodes) == [(100, "addrA"), (200, "addrB"), (300, "addrC")]
        # B appears exactly once despite being on both sides.
        assert nodes.count((200, "addrB")) == 1

    def test_repeated_transfers_collapse_to_one_node_each(self, spark):
        from cms.transfers import get_nodeListWithAddress
        df = make_nodes_edges_df(spark, [
            (100, 200, "addrA", "addrB", 2020, "a1"),
            (100, 200, "addrA", "addrB", 2020, "a2"),
            (100, 200, "addrA", "addrB", 2021, "a3"),
        ])
        nodes = {(r["ORGNPINM"], r["providerAddress"]) for r in get_nodeListWithAddress(df).collect()}
        assert nodes == {(100, "addrA"), (200, "addrB")}

    def test_same_npi_different_address_kept_separate(self, spark):
        # The node identity is the (NPI, address) pair, so a provider reported at
        # two addresses yields two rows.
        from cms.transfers import get_nodeListWithAddress
        df = make_nodes_edges_df(spark, [
            (100, 200, "addrA", "addrB", 2020, "a1"),
            (100, 300, "addrA2", "addrC", 2021, "a2"),
        ])
        nodes = {(r["ORGNPINM"], r["providerAddress"]) for r in get_nodeListWithAddress(df).collect()}
        assert (100, "addrA") in nodes
        assert (100, "addrA2") in nodes
