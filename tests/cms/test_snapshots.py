'''tests/data/snapshots.py, the regression snapshots of test_regression.py, on small frames: the measures of a profile,
the comparison of two, the small cells the printed report hides, the choice of the baseline.'''
import datetime
import subprocess

import pytest

from tests.data import snapshots

@pytest.fixture
def profile(spark):
    rows = [(2018, 1, "home", [1, 2], 2.5, True), (2018, 0, "died", [3], None, False), (2018, 1, "home", None, 1.0, True),
            (2019, 1, "snf", [], 4.0, None)]
    df = spark.createDataFrame(rows, "THRU_DT_YEAR int, flag int, destination string, days array<int>, cost double, ok boolean")
    return {(year, column, measure): value for (_, year, column, measure, value) in snapshots.get_profile("ipStays", df)}

def test_rows_and_nulls_by_year(profile):
    assert (profile[(2018, "*", "rows")], profile[(2019, "*", "rows")]) == (3, 1)
    assert (profile[(2018, "cost", "nonNull")], profile[(2018, "days", "nonNull")]) == (2, 2)

def test_numbers_get_sum_min_max(profile):
    assert (profile[(2018, "flag", "sum")], profile[(2018, "flag", "min")], profile[(2018, "flag", "max")]) == (2, 0, 1)
    assert profile[(2018, "cost", "sum")] == pytest.approx(3.5)
    assert profile[(2018, "ok", "sum")] == 2

def test_arrays_get_their_total_size_not_their_order(profile):
    assert (profile[(2018, "days", "sizeSum")], profile[(2019, "days", "sizeSum")]) == (3, 0)
    assert (2018, "days", "distinctApprox") not in profile

def test_few_values_get_a_count_each_but_not_a_0_1_flag(profile):
    assert (profile[(2018, "destination", "count:home")], profile[(2018, "destination", "count:died")]) == (2, 1)
    assert profile[(2019, "destination", "count:snf")] == 1
    assert not [k for k in profile if k[1] == "flag" and k[2].startswith("count:")]

def test_revenue_parts_take_the_year_of_THRU_DT(spark):
    df = spark.createDataFrame([(20180105, 450), (20190101, 100)], "THRU_DT int, REV_CNTR int")
    assert {(year, value) for (_, year, column, _, value) in snapshots.get_profile("ipRevenue", df) if column == "*"} == \
           {(2018, 1), (2019, 1)}

def test_saved_and_loaded_as_written(tmp_path, spark):
    rows = [("ipStays", 2018, "*", "rows", 3), ("ipStays", 2018, "cost", "sum", 0.1 + 0.2), ("mbsf", None, "x", "nonNull", 0)]
    snapshots.save_snapshot(tmp_path, "s", rows, {"commit": "abc"})
    assert snapshots.load_snapshot(tmp_path, "s") == {r[:4]: r[4] for r in rows}
    assert (tmp_path / "s.json").read_text().count("abc") == 1

def test_diff_names_what_changed_and_what_appeared():
    before = {("ipStays", 2018, "tpa", "sum"): 20, ("ipStays", 2018, "cost", "sum"): 1.0, ("ipStays", 2018, "old", "nonNull"): 5}
    after = {("ipStays", 2018, "tpa", "sum"): 21, ("ipStays", 2018, "cost", "sum"): 1.0 + 1e-12,
             ("ipStays", 2018, "new", "nonNull"): 5}
    assert snapshots.diff_profiles(before, after) == [("ipStays", 2018, "new", "nonNull", None, 5),
                                                      ("ipStays", 2018, "old", "nonNull", 5, None),
                                                      ("ipStays", 2018, "tpa", "sum", 20, 21)]
    assert snapshots.diff_profiles(before, dict(before)) == []

@pytest.mark.parametrize("measure, value, nonNull, expected", [
    ("rows", 7, 100, "<11"), ("rows", 11, 100, "11"), ("count:died", 3, 100, "<11"), ("count:died", 0, 100, "0"),
    ("sum", 4, 100, "<11"), ("sum", 40, 100, "40"), ("sum", 40.5, 5, "hidden"), ("sum", 4.5, 100, "4.5"),
    ("min", 70, 100, "hidden"), ("max", 99, 100, "hidden"), ("nonNull", None, 100, "absent")])
def test_the_report_hides_small_cells(measure, value, nonNull, expected):
    profile = {("ipStays", 2018, "x", "nonNull"): nonNull}
    assert snapshots.shown(profile, "ipStays", 2018, "x", measure, value) == expected

def test_the_report_lists_changed_columns_then_the_measures():
    before = {("ipStays", 2018, "tpa", "nonNull"): 50, ("ipStays", 2018, "tpa", "sum"): 5}
    after = {("ipStays", 2018, "tpa", "nonNull"): 50, ("ipStays", 2018, "tpa", "sum"): 30}
    lines = snapshots.report_lines("b", "a", before, after, snapshots.diff_profiles(before, after))
    assert "1 measures changed in 1 columns" in lines[0] and lines[1].split()[:2] == ["ipStays", "tpa"]
    assert lines[2].split()[-3:] == ["<11", "->", "30"]
    assert snapshots.report_lines("b", "a", before, before, []) == ["b vs a: no changes"]
    assert "no earlier snapshot" in snapshots.report_lines("b", None, {}, before, [])[0]

def test_the_baseline_is_the_newest_earlier_snapshot_of_the_same_years_and_sample(tmp_path):
    for stem in ["20261101-120000_abc_2018-2019_n100", "20261105-120000_def-dirty_2018-2019_n100",
                 "20261106-120000_ghi_2016-2019_n100", "20261107-120000_jkl_2018-2019_n10",
                 "20261120-120000_mno_2018-2019_n100",
                 "20261108-120000_stu_2018-2019_n100_vs_20261105-120000_def-dirty_2018-2019_n100"]:
        (tmp_path / f"{stem}.csv").write_text("")
    current = "20261110-120000_pqr_2018-2019_n100"
    assert snapshots.find_baseline(tmp_path, current) == "20261105-120000_def-dirty_2018-2019_n100"
    assert snapshots.find_baseline(tmp_path, current, "20261101-120000_abc_2018-2019_n100.csv") == \
           "20261101-120000_abc_2018-2019_n100"
    assert snapshots.find_baseline(tmp_path, "20261001-120000_pqr_2018-2019_n100") is None

def test_the_name_has_the_date_commit_years_and_sample():
    meta = {"commit": "abc1234", "dirty": True, "years": [2018, 2019], "sample": 100}
    assert snapshots.get_name(meta, datetime.datetime(2026, 11, 9, 13, 5, 1)) == "20261109-130501_abc1234-dirty_2018-2019_n100"

def test_no_git_gives_nocommit(monkeypatch, tmp_path):
    def fail(*args, **kwargs):
        raise FileNotFoundError("git")
    monkeypatch.setattr(subprocess, "run", fail)
    assert snapshots.get_commit(tmp_path) == ("nocommit", False)
