'''Properties that must hold for any input, not just the examples of the unit tests: the output does not depend on the order
the rows arrive in or on how they are partitioned. Each input has ties built in, rows that the code's windows and
groupings cannot tell apart by their keys, which is where order can leak through.'''
import random
from contextlib import contextmanager

import pytest
from pyspark.sql import Row

from tests.cms.test_stays import _add_null_bene_columns, _ip_src_dest_row, _prep_hhaBase, _prep_hospBase, _prep_ipBase, \
    _prep_snfBase, _snf_src_dest_row

#(input partitions, shuffle partitions): one partition keeps the rows in the order given, five spread them out
settings = ((1, "2"), (5, "7"))
arrangements = ("as given", "reversed", "shuffled 1", "shuffled 2")

def _arrange(rows, arrangement):
    if arrangement == "reversed":
        return rows[::-1]
    if arrangement.startswith("shuffled"):
        rows = list(rows)
        random.Random(int(arrangement[-1])).shuffle(rows)
    return rows

def _normal(value, digits):
    #rows compared as sorted lists, arrays kept in their order, which is what is tested; doubles to a tolerance, as a sum
    #or mean over a window can change in its last bits with the order it adds up in
    if isinstance(value, float):
        return round(value, digits)
    if isinstance(value, (Row, list, tuple)):
        return tuple(_normal(v, digits) for v in value)
    if isinstance(value, dict):
        return tuple(sorted((k, _normal(v, digits)) for k, v in value.items()))
    return value

#arrays in no particular order, left out of the comparisons: the sets, collect_sets in the order of Spark's internal hash
#set, which the library only counts (see the data dictionary), and the otherStays arrays, collect_lists that
#add_source_and_destination_info reads only through array_contains and size, and stays.drop_unused_columns drops
unsortedSets = ("losDaysAtall90", "losDaysAtall365", "losDaysAtallMinusHha90", "losDaysAtallMinusHha365",
                "nodeFromSetOfToNodes", "nodeToSetOfFromNodes")
otherStays = ("otherStays", "otherStaysOnAdmission", "otherStaysOnAdmissionPrev", "otherStaysOnThru", "otherStaysOnThruNext")

def comparable(df, ignore=(), digits=9):
    '''(columns, rows) of df without the columns in ignore or ending in one of them (the from/to copies a transfer has),
    rows sorted, so two outputs compare equal when they hold the same rows in any order.'''
    columns = sorted(c for c in df.columns if not any(c == x or c.endswith(x) for x in ignore))
    return columns, sorted((tuple(_normal(r[c], digits) for c in columns) for r in df.select(columns).collect()), key=repr)

def differing_columns(columns, rows, otherRows):
    values = lambda rows, i: sorted((r[i] for r in rows), key=repr)
    return [c for (i, c) in enumerate(columns) if values(rows, i) != values(otherRows, i)] or \
           "no one column: the same values pair up differently in the rows"

@contextmanager
def shuffle_partitions(spark, n):
    '''Runs the block with n shuffle partitions, not coalesced, and puts the settings back after, as the session is shared.'''
    keys = ("spark.sql.shuffle.partitions", "spark.sql.adaptive.coalescePartitions.enabled")
    old = {k: spark.conf.get(k) for k in keys}
    try:
        spark.conf.set(keys[0], str(n))
        spark.conf.set(keys[1], "false")
        yield
    finally:
        for k, v in old.items():
            spark.conf.set(k, v)

def in_every_order(spark, fn, *dfs, ignore=(), digits=9):
    '''Asserts fn(*dfs) gives the same rows with every df rebuilt in each of the arrangements and settings above, and
    returns them. ignore: output columns left out of the comparison.'''
    data = [(df.schema, df.collect()) for df in dfs]
    expected = None
    for (inputPartitions, shufflePartitions) in settings:
        with shuffle_partitions(spark, shufflePartitions):
            for arrangement in arrangements:
                frames = [spark.createDataFrame(spark.sparkContext.parallelize(_arrange(rows, arrangement), inputPartitions), schema)
                          for (schema, rows) in data]
                (columns, result) = comparable(fn(*frames), ignore, digits)
                label = f"{arrangement} on {inputPartitions} partition(s), {shufflePartitions} shuffle partitions"
                if expected is None:
                    expected = (label, result)
                elif result != expected[1]:
                    pytest.fail(f"{label} differs from {expected[0]} in {differing_columns(columns, result, expected[1])}")
    return expected[1]


def mixed_claims(hits, misses, seed, nLines=150):
    '''About nLines lines drawn from hits and misses (the rows that set a flag and the rows just outside it) over 18 claims
    whose keys collide one part at a time, the same CLAIMNO for another beneficiary or THRU_DT and the same beneficiary and
    THRU_DT under another CLAIMNO, each claim with its own share of hits, none for some.'''
    rng = random.Random(seed)
    claims = [(bene, claim, thru) for bene in (1, 2) for claim in (1, 2, 3) for thru in (20180101, 20180102, 20190101)]
    hitShare = {claim: rng.choice([0, 0.1, 0.5, 1]) for claim in claims}
    lines = []
    for _ in range(nLines):
        claim = rng.choice(claims)
        row = rng.choice(hits) if rng.random() < hitShare[claim] else rng.choice(misses)
        lines.append({**row, "DSYSRTKY": claim[0], "CLAIMNO": claim[1], "THRU_DT": claim[2]})
    return lines

def assert_inClaim_is_any_line(lines, column, perLine, inClaim, summary):
    '''perLine and inClaim: lines with the flag added in each mode, summary: one row per claim. The flag is the same on
    each line in both modes, and xInClaim on each line and the summary on each claim are 1 where any line of the claim is.'''
    inputs = lambda r: tuple(r[c] for c in lines.columns) + (r[column],)
    assert sorted(map(inputs, inClaim.collect()), key=repr) == sorted(map(inputs, perLine.collect()), key=repr)
    keyOf = lambda r: (r["DSYSRTKY"], r["CLAIMNO"], r["THRU_DT"])
    anyLine = {}
    for r in perLine.collect():
        anyLine[keyOf(r)] = max(anyLine.get(keyOf(r), 0), r[column])
    assert set(anyLine.values()) == {0, 1}
    assert sorted({(keyOf(r), r[f"{column}InClaim"]) for r in inClaim.collect()} - set(anyLine.items())) == []
    assert {keyOf(r): r[column] for r in summary.collect()} == anyLine


class TestSameOutputInAnyOrder:

    @pytest.mark.xfail(strict=True, reason="prep_mbsf keeps the first of a beneficiary-year's duplicate rows, ordered by a "
                                           "partition key, so which one survives depends on row order (cms/mbsf.py:484)")
    def test_prep_mbsf_keeps_the_same_duplicate(self, spark):
        from cms.mbsf import prep_mbsf
        #the duplicate beneficiary-years of 2015 and earlier, here differing in dual status and ESRD
        mbsf = spark.createDataFrame([(1, 2015, "00", "0"), (1, 2015, "02", "Y"), (2, 2015, "00", "0")],
                                     "DSYSRTKY int, RFRNC_YR int, DUAL_01 string, ESRD_IND string")
        in_every_order(spark, prep_mbsf, mbsf)

    @pytest.mark.xfail(strict=True, reason="get_ccn_jc_info orders a CCN's sites by program and site name, which two sites "
                                           "can share, so the best program's match method is any one of theirs, and "
                                           "jcSiteNames is an unsorted collect_list (utilities.py:1360)")
    def test_get_ccn_jc_info_picks_the_same_site(self, spark):
        from utilities import get_ccn_jc_info
        #two sites of one CCN with the same program under the same legal-entity name, matched differently
        sites = spark.createDataFrame([("1", "SUTTER", "Primary Stroke Center", 3, "X", "nearestSite", 0.0),
                                       ("2", "SUTTER", "Primary Stroke Center", 3, "X", "nearestParent", 0.3),
                                       ("3", "ALPHA", "Acute Stroke Ready Hospital", 4, "X", "nearestNamed", 1.0)],
                                      "jcHcoId string, jcSiteName string, jcProgram string, jcProgramRank int, posCcn string, "
                                      "posMatchMethod string, posCcnDistanceKm double")
        in_every_order(spark, get_ccn_jc_info, sites)

    def test_unique_stays_and_their_source_and_destination(self, spark):
        from cms.stays import add_source_and_destination_info, get_unique_stays
        ip = lambda claim, bene, npi, admission, through: _ip_src_dest_row(claim, dsysrtky=bene, orgnpinm=npi,
                                                                             admsn_dt=admission, thru_dt=through)
        ipRows = [# two claims of one stay through the same day, which only CLAIMNO tells apart, and a stay that ends the
                  # day a one-day stay is, which is the day a third one starts
                  ip(10, 1, 100, 20200110, 20200115), ip(11, 1, 100, 20200110, 20200115),
                  ip(12, 1, 200, 20200115, 20200115), ip(13, 1, 300, 20200115, 20200120),
                  # two one-day stays on the same day, and a stay that starts on it
                  ip(20, 2, 100, 20200301, 20200301), ip(21, 2, 200, 20200301, 20200301), ip(22, 2, 300, 20200301, 20200305)]
        snfRows = [_snf_src_dest_row(30, dsysrtky=1, orgnpinm=400, admsn_dt=20200120, thru_dt=20200130),
                   _snf_src_dest_row(31, dsysrtky=2, orgnpinm=400, admsn_dt=20200305, thru_dt=20200310)]
        hha, hosp = _prep_hhaBase(spark, []), _prep_hospBase(spark, [])

        def stays(ipDF, snfDF):
            staysDF = _add_null_bene_columns(get_unique_stays(ipDF, claimType="ip"))
            return add_source_and_destination_info(staysDF, {"ipBase": ipDF, "snfBase": snfDF, "hhaBase": hha, "hospBase": hosp})

        result = in_every_order(spark, stays, _prep_ipBase(spark, ipRows), _prep_snfBase(spark, snfRows), ignore=otherStays)
        assert len(result) == 6

    def test_node_and_dyad_columns_and_their_priors(self, spark):
        from cms.transfers import add_node_and_dyad_info, add_node_distance_info
        #the same dyads many times over two years, so the windows over a provider-year and a dyad-year hold ties, and the
        #priors lag a year that has several rows
        dyads = {2019: [(100, 200)] * 3 + [(100, 201), (101, 200), (101, 200), (101, 202), (102, 200)],
                 2020: [(100, 200), (100, 202), (100, 202), (101, 200), (103, 201)]}
        fips = {100: "39049", 101: "39041", 102: "39089", 103: "18097", 200: "39049", 201: "39035", 202: "18097"}
        rows = [(fromNpi, toNpi, year, year, f"a{year}{i}", f"b{year}{i}", i % 2, (i + 1) % 2, 0, 0, i % 2, 1,
                 fromNpi % 3, toNpi % 3, fips[fromNpi], fips[toNpi], fips[fromNpi][:2], fips[toNpi][:2],
                 (fromNpi + toNpi) / 7, (fromNpi + toNpi) / 3, (fromNpi * toNpi) / 11.0)
                for (year, pairs) in dyads.items() for (i, (fromNpi, toNpi)) in enumerate(pairs)]
        transfers = spark.createDataFrame(rows, "fromORGNPINM int, toORGNPINM int, fromTHRU_DT_YEAR int, toTHRU_DT_YEAR int, "
                                                "fromCLAIMNO string, toCLAIMNO string, fromed int, fromct int, frommri int, "
                                                "toed int, toct int, tomri int, fromproviderSysId int, toproviderSysId int, "
                                                "fromproviderFIPS string, toproviderFIPS string, fromproviderStateFIPS string, "
                                                "toproviderStateFIPS string, dyadGeodesicDistanceKm double, "
                                                "dyadDrivingDistanceKm double, dyadDrivingTimeMin double")
        in_every_order(spark, lambda df: add_node_distance_info(add_node_and_dyad_info(df)), transfers, ignore=unsortedSets)
