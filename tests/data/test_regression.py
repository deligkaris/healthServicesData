'''Regression snapshot: profiles every output of the chain on the sampled claims (the get_cms_data parts, the ip and op
stays, the IP->IP and ED->IP transfers; see tests/chain.py), saves the profile under --snapshot-path and reports what
changed since the snapshot before it (snapshots.py). It reports and does not fail on a change, as some changes are meant;
a change on a rerun of the same commit means the data, the environment or a nondeterminism moved the results.'''
import shutil
from pathlib import Path

import pytest

from tests.chain import get_stays_and_transfers
from tests.data import snapshots

repo = Path(__file__).resolve().parents[2]

def test_snapshot(spark, request, years, sampledCmsData, lastObservableDay):
    config = request.config
    folder = config.getoption("--snapshot-path")
    if folder is None:
        pytest.skip("no --snapshot-path given")
    output = config.getoption("--tmp-path") + "/regressionChain"
    try:
        #the stays are cleaned before the transfers, as the analysis scripts do
        outputs = {**sampledCmsData, **get_stays_and_transfers(sampledCmsData, lastObservableDay, output, dropUnused=True)}
        rows = [row for name, df in outputs.items() for row in snapshots.get_profile(name, df)]
    finally:
        shutil.rmtree(output, ignore_errors=True)
    assert rows, "the profile is empty"
    (commit, dirty) = snapshots.get_commit(repo)
    meta = {"commit": commit, "dirty": dirty, "years": list(years), "sample": config.getoption("--beneficiary-sample"),
            "sparkVersion": spark.version, "cmsPath": config.getoption("--cms-path")}
    name = snapshots.get_name(meta)
    snapshots.save_snapshot(folder, name, rows, meta)
    baseline = snapshots.find_baseline(folder, name, config.getoption("--baseline"))
    (before, after) = (snapshots.load_snapshot(folder, baseline) if baseline else {}, snapshots.load_snapshot(folder, name))
    diff = snapshots.diff_profiles(before, after) if baseline else []
    if diff:
        snapshots.save_diff(folder, name, baseline, diff)
    snapshots.report.extend(snapshots.report_lines(name, baseline, before, after, diff))
