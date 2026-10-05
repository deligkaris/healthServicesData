'''Step by step tests of the joint commission to POS pipeline (scripts/01-04): the export prep, the address and name
lookups, the nearest hospital, the CCN assignment, the per CCN table and the join onto claims. All offline: Google is
replaced by canned responses, Spark is the local session of tests/conftest.py.'''
import io
import json
import pytest
from unittest import mock

import geocoding
import pyspark.sql.functions as F

KM = 1 / 111.195  # degrees of latitude per km, to place test points at a known distance along a meridian

JC_COLS = ["HCO ID", "Organization Name", "Organization Doing Business As (DBA) Name", "State", "City",
           "Street Address", "Postal Code", "Site Name", "Site Doing Business As (DBA) Name", "Program",
           "Effective Date", "Status"]
JC_SCHEMA = ", ".join(f"`{c}` string" for c in JC_COLS)
POS_SCHEMA = ("PRVDR_NUM string, FAC_NAME string, STATE_CD string, posHospital int, posActive int, "
              "posLat double, posLng double, posGeocodeLocationType string")
SITE_SCHEMA = ("jcHcoId string, jcSiteName string, jcAddress string, jcState string, jcSearchName string, "
               "jcLat double, jcLng double, jcSiteLat double, jcSiteLng double, jcSiteLocationSource string, "
               "jcProgram string, jcProgramRank int")


def jc_row(hco="1", org="Org", orgDba=None, state="Ohio", city="Columbus", street="1 Main St", zipCode="43215",
           site="Site", siteDba=None, program="Primary Stroke Center", date="2020-01-01", status="Certification"):
    return (hco, org, orgDba, state, city, street, zipCode, site, siteDba, program, date, status)

def pos_row(ccn, name, state="OH", hospital=1, active=1, lat=40.0, lng=-83.0, geocodeType="ROOFTOP"):
    return (ccn, name, state, hospital, active, lat, lng, geocodeType)

def site_row(hco, name, lat, lng, siteLat=None, siteLng=None, source="place", search=None, program="Primary Stroke Center",
             rank=3, state="OH", address=None):
    return (hco, name, address or f"ADDR {hco} {name}", state, search or name, lat, lng,
            lat if siteLat is None else siteLat, lng if siteLng is None else siteLng, source, program, rank)

def geocode_payload(lat=40.0, lng=-83.0, locationType="ROOFTOP"):
    return {"status": "OK", "results": [{"geometry": {"location": {"lat": lat, "lng": lng}, "location_type": locationType},
                                         "formatted_address": "x"}]}

def place_payload(lat=40.0, lng=-83.0, name="Place", types=("hospital",)):
    return {"places": [{"id": "id", "displayName": {"text": name}, "formattedAddress": "y",
                        "location": {"latitude": lat, "longitude": lng}, "types": list(types), "businessStatus": "OPERATIONAL"}]}

def fake_urlopen(payloads):
    payloads = list(payloads)
    def opener(url, timeout=None):
        response = mock.MagicMock()
        response.__enter__.return_value = io.StringIO(json.dumps(payloads.pop(0)))
        return response
    return mock.MagicMock(side_effect=opener)

def write_key(tmp_path):
    (tmp_path / "GEOCODING").mkdir(exist_ok=True)
    (tmp_path / "GEOCODING" / "key.csv").write_text("k\n")

@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr(geocoding.time, "sleep", lambda s: None)


# ============================================================
# step 3a: the export prep, column by column (prep_jcAccreditationDF without lookups)
# ============================================================

class TestPrepRenaming:

    def test_raw_columns_renamed_and_all_lowercase(self, spark):
        from utilities import prep_jcAccreditationDF
        result = prep_jcAccreditationDF(spark.createDataFrame([jc_row()], JC_SCHEMA))
        assert all(c[0].islower() for c in result.columns)
        assert {"jcHcoId", "jcOrganizationName", "jcOrganizationDbaName", "jcState", "jcCity", "jcStreetAddress",
                "jcPostalCode", "jcSiteName", "jcSiteDbaName", "jcProgram", "jcEffectiveDate", "jcStatus"} <= set(result.columns)
        assert not any(" " in c or "(" in c for c in result.columns)

    def test_raw_values_kept(self, spark):
        from utilities import prep_jcAccreditationDF
        row = prep_jcAccreditationDF(spark.createDataFrame([jc_row(hco="725474", date="2025-09-19")], JC_SCHEMA)).collect()[0]
        assert row["jcHcoId"] == "725474" and row["jcEffectiveDate"] == "2025-09-19" and row["jcStatus"] == "Certification"


class TestPrepState:

    @pytest.mark.parametrize("raw, expected", [("Alabama", "AL"), ("alabama", "AL"), (" New York ", "NY"),
                                               ("District of Columbia", "DC"), ("Puerto Rico", "PR"), ("oh", "OH"),
                                               ("OH", "OH"), ("Ontario", "ONTARIO")])
    def test_state_to_usps_code(self, spark, raw, expected):
        from utilities import prep_jcAccreditationDF
        row = prep_jcAccreditationDF(spark.createDataFrame([jc_row(state=raw)], JC_SCHEMA)).collect()[0]
        assert row["jcState"] == expected

    def test_address_uses_the_raw_state_text(self, spark):
        from utilities import prep_jcAccreditationDF
        row = prep_jcAccreditationDF(spark.createDataFrame([jc_row(state="Alabama", city="Bay Minette", street="1815 Hand Ave",
                                                                   zipCode="36507")], JC_SCHEMA)).collect()[0]
        assert row["jcAddress"] == "1815 HAND AVE, BAY MINETTE, ALABAMA 36507" and row["jcState"] == "AL"


class TestPrepZipAndNames:

    @pytest.mark.parametrize("raw, expected", [("43215", "43215"), ("43215-1234", "43215"), (" 43215 ", "43215"), ("4321", "4321")])
    def test_zip5(self, spark, raw, expected):
        from utilities import prep_jcAccreditationDF
        assert prep_jcAccreditationDF(spark.createDataFrame([jc_row(zipCode=raw)], JC_SCHEMA)).collect()[0]["jcZip"] == expected

    def test_site_name_falls_back_to_dba_then_organization(self, spark):
        from utilities import prep_jcAccreditationDF
        rows = prep_jcAccreditationDF(spark.createDataFrame(
            [jc_row(hco="1", site="Site A", siteDba="DBA A"), jc_row(hco="2", site=None, siteDba="DBA B"),
             jc_row(hco="3", site=None, siteDba=None, org="Org C")], JC_SCHEMA)).collect()
        names = {r["jcHcoId"]: r["jcSiteName"] for r in rows}
        assert names == {"1": "Site A", "2": "DBA B", "3": "Org C"}

    @pytest.mark.parametrize("site, dba, expected", [
        ("Gulf Health Hospitals, Inc.", "North Baldwin Infirmary", "North Baldwin Infirmary"),
        ("Sutter Bay Hospitals", "Novato Community Hospital", "Novato Community Hospital"),
        ("Saint John Hospital", "Hospital", "Saint John Hospital"),
        ("Kaiser Foundation Hospital - Riverside", "General Acute Care Hospital", "Kaiser Foundation Hospital - Riverside"),
        ("Legal Entity LLC", "N/A", "Legal Entity LLC"),
        ("Legal Entity LLC", "", "Legal Entity LLC"),
        ("Legal Entity LLC", "   ", "Legal Entity LLC"),
        ("Legal Entity LLC", None, "Legal Entity LLC"),
        ("Legal Entity LLC", "  Real Name  ", "Real Name")])
    def test_search_name_prefers_a_specific_dba(self, spark, site, dba, expected):
        from utilities import prep_jcAccreditationDF
        row = prep_jcAccreditationDF(spark.createDataFrame([jc_row(site=site, siteDba=dba)], JC_SCHEMA)).collect()[0]
        assert row["jcSearchName"] == expected
        assert row["jcPlaceQuery"] == f"{expected}, OH"


class TestPrepProgramRank:

    @pytest.mark.parametrize("program, rank", [
        ("Advanced Comprehensive Stroke Center", 1), ("Comprehensive Stroke Center", 1), ("COMPREHENSIVE STROKE CENTER", 1),
        ("Thrombectomy-Capable Stroke Center", 2), ("thrombectomy capable stroke center", 2),
        ("Primary Stroke Center", 3), ("Advanced Primary Stroke Center", 3),
        ("Acute Stroke Ready Hospital", 4), ("Acute  Stroke  Ready  Hospital", 4),
        ("Stroke Rehabilitation", 5), ("Stroke  Rehabilitation", 5), ("  Stroke Rehabilitation  ", 5),
        ("Hospital", None), ("Primary Care Medical Home", None), ("Comprehensive Cardiac Center", None),
        ("Stroke", None), ("Laboratory", None)])
    def test_rank(self, spark, program, rank):
        from utilities import prep_jcAccreditationDF
        assert prep_jcAccreditationDF(spark.createDataFrame([jc_row(program=program)], JC_SCHEMA)).collect()[0]["jcProgramRank"] == rank

    def test_all_rows_kept_by_default(self, spark):
        from utilities import prep_jcAccreditationDF
        df = spark.createDataFrame([jc_row(program=p) for p in ["Primary Stroke Center", "Comprehensive Stroke Center", "Hospital"]], JC_SCHEMA)
        assert prep_jcAccreditationDF(df).count() == 3


class TestPrepExcludeAndTop:

    def _df(self, spark):
        return spark.createDataFrame(
            [jc_row(hco="1", site="A", program="Primary Stroke Center", date="2018"),
             jc_row(hco="1", site="A", program="Advanced Comprehensive Stroke Center", date="2021"),
             jc_row(hco="1", site="A", program="Stroke  Rehabilitation", date="2019"),
             jc_row(hco="2", site="B", program="Stroke Rehabilitation"),
             jc_row(hco="3", site="C", program="Hospital"),
             jc_row(hco="3", site="C", program="Acute Stroke Ready Hospital"),
             jc_row(hco="4", site="D", program="Primary Stroke Center", date="2020"),
             jc_row(hco="4", site="D", program="Primary Stroke Center", date="2022"),
             jc_row(hco="5", site="E", program="Primary Stroke Center"),
             jc_row(hco="6", site="E", program="Acute Stroke Ready Hospital")], JC_SCHEMA)

    @pytest.mark.parametrize("keywords", [["stroke rehabilitation"], ["STROKE REHABILITATION"], ["rehabilitation"], ["rehab", "nothing"]])
    def test_exclude_is_case_insensitive_and_by_keyword(self, spark, keywords):
        from utilities import prep_jcAccreditationDF
        rows = prep_jcAccreditationDF(self._df(spark), excludePrograms=keywords).collect()
        assert len(rows) == 8 and not any("ehabilitation" in r["jcProgram"] for r in rows)

    def test_exclude_can_remove_a_whole_site(self, spark):
        from utilities import prep_jcAccreditationDF
        rows = prep_jcAccreditationDF(self._df(spark), excludePrograms=["stroke rehabilitation"]).collect()
        assert "2" not in {r["jcHcoId"] for r in rows}

    def test_top_program_keeps_best_rank_with_its_own_date(self, spark):
        from utilities import prep_jcAccreditationDF
        rows = {r["jcHcoId"]: r for r in prep_jcAccreditationDF(self._df(spark), topProgramOnly=True).collect()}
        assert rows["1"]["jcProgram"] == "Advanced Comprehensive Stroke Center" and rows["1"]["jcEffectiveDate"] == "2021"
        assert rows["3"]["jcProgram"] == "Acute Stroke Ready Hospital"

    def test_top_program_drops_sites_without_a_stroke_program_only(self, spark):
        from utilities import prep_jcAccreditationDF
        df = self._df(spark).union(spark.createDataFrame([jc_row(hco="7", site="G", program="Hospital")], JC_SCHEMA))
        rows = prep_jcAccreditationDF(df, topProgramOnly=True).collect()
        assert "7" not in {r["jcHcoId"] for r in rows} and "3" in {r["jcHcoId"] for r in rows}

    def test_top_program_collapses_duplicate_rows_to_one(self, spark):
        from utilities import prep_jcAccreditationDF
        rows = [r for r in prep_jcAccreditationDF(self._df(spark), topProgramOnly=True).collect() if r["jcHcoId"] == "4"]
        assert len(rows) == 1

    def test_same_site_name_under_different_hcos_stays_separate(self, spark):
        from utilities import prep_jcAccreditationDF
        rows = [r for r in prep_jcAccreditationDF(self._df(spark), topProgramOnly=True).collect() if r["jcSiteName"] == "E"]
        assert {r["jcHcoId"] for r in rows} == {"5", "6"}

    def test_rehab_excluded_then_top_keeps_the_other_program(self, spark):
        from utilities import prep_jcAccreditationDF
        rows = {r["jcHcoId"]: r for r in prep_jcAccreditationDF(self._df(spark), topProgramOnly=True,
                                                                excludePrograms=["stroke rehabilitation"]).collect()}
        assert set(rows) == {"1", "3", "4", "5", "6"} and rows["1"]["jcProgramRank"] == 1


# ============================================================
# step 2: the address string and the two lookups (caches, no attaching)
# ============================================================

class TestAddressKey:

    SCHEMA = "street string, city string, state string, zip string"

    @pytest.mark.parametrize("parts, expected", [
        (("1 Main St", "Columbus", "Ohio", "43215"), "1 MAIN ST, COLUMBUS, OHIO 43215"),
        ((" 1  Main   St ", " columbus", "oh ", "43215-9999"), "1 MAIN ST, COLUMBUS, OH 43215"),
        ((None, "Columbus", "OH", "43215"), "COLUMBUS, OH 43215"),
        (("1 Main St", None, "OH", "43215"), "1 MAIN ST, OH 43215"),
        (("1 Main St", "Columbus", "OH", None), "1 MAIN ST, COLUMBUS, OH"),
        (("1 Main St", "Columbus", None, None), "1 MAIN ST, COLUMBUS")])
    def test_normalized(self, spark, parts, expected):
        df = spark.createDataFrame([parts], self.SCHEMA)
        assert geocoding.add_address(df, "street", "city", "state", "zip", "a").collect()[0]["a"] == expected

    def test_two_spellings_one_cache_key(self, spark):
        df = spark.createDataFrame([("1815 Hand Ave", "Bay Minette", "Alabama", "36507"),
                                    ("1815 HAND AVE ", "BAY MINETTE", "ALABAMA", "36507-0000")], self.SCHEMA)
        keys = {r["a"] for r in geocoding.add_address(df, "street", "city", "state", "zip", "a").collect()}
        assert len(keys) == 1


class TestGeocodeCacheBehaviour:

    def test_duplicates_and_nones_in_input(self, tmp_path):
        write_key(tmp_path)
        opener = fake_urlopen([geocode_payload()])
        with mock.patch.object(geocoding, "urlopen", opener):
            records = geocoding.geocode_addresses(["A", "A", None, "A"], str(tmp_path))
        assert opener.call_count == 1 and set(records) == {"A"}

    def test_flush_every(self, tmp_path, monkeypatch):
        write_key(tmp_path)
        writes = []
        monkeypatch.setattr(geocoding, "write_geocode_cache", lambda cache, pathToData: writes.append(len(cache)))
        opener = fake_urlopen([geocode_payload()] * 5)
        with mock.patch.object(geocoding, "urlopen", opener):
            geocoding.geocode_addresses(list("ABCDE"), str(tmp_path), flushEvery=2, workers=1)
        assert writes == [2, 4, 5]

    def test_cache_survives_across_runs_and_mixes_hits_and_misses(self, tmp_path):
        write_key(tmp_path)
        with mock.patch.object(geocoding, "urlopen", fake_urlopen([geocode_payload(lat=1.0)])):
            geocoding.geocode_addresses(["A"], str(tmp_path))
        opener = fake_urlopen([geocode_payload(lat=2.0)])
        with mock.patch.object(geocoding, "urlopen", opener):
            records = geocoding.geocode_addresses(["A", "B"], str(tmp_path))
        assert opener.call_count == 1 and records["A"]["lat"] == 1.0 and records["B"]["lat"] == 2.0

    def test_misses_are_sorted_and_deduplicated(self, tmp_path):
        geocoding.write_geocode_cache({"B": {}}, str(tmp_path))
        assert geocoding.get_geocode_cache_misses(["C", "A", "B", "A", None], str(tmp_path)) == ["A", "C"]

    def test_cache_file_is_readable_json_keyed_by_address(self, tmp_path):
        write_key(tmp_path)
        with mock.patch.object(geocoding, "urlopen", fake_urlopen([geocode_payload(lat=1.5)])):
            geocoding.geocode_addresses(["1 MAIN ST, X, OH 43215"], str(tmp_path))
        cache = json.loads((tmp_path / "GEOCODING" / "googleGeocodeCache.json").read_text())
        assert cache["1 MAIN ST, X, OH 43215"]["lat"] == 1.5 and set(cache["1 MAIN ST, X, OH 43215"]) == set(geocoding.geocodeFields)


class TestPlacesCacheBehaviour:

    def test_bias_passed_per_query(self, tmp_path):
        write_key(tmp_path)
        seen = []
        def fake_find_place(query, apiKey, biasLat=None, biasLng=None):
            seen.append((query, biasLat, biasLng))
            return geocoding.parse_place_response(place_payload())
        with mock.patch.object(geocoding, "find_place", fake_find_place):
            geocoding.find_places({"A, OH": (40.0, -83.0), "B, OH": None}, str(tmp_path), workers=1)
        assert sorted(seen) == [("A, OH", 40.0, -83.0), ("B, OH", None, None)]

    def test_places_and_geocode_caches_are_separate_files(self, tmp_path):
        write_key(tmp_path)
        with mock.patch.object(geocoding, "urlopen", fake_urlopen([geocode_payload()])):
            geocoding.geocode_addresses(["A"], str(tmp_path))
        with mock.patch.object(geocoding, "urlopen", fake_urlopen([place_payload()])):
            geocoding.find_places({"A": None}, str(tmp_path))
        assert set(geocoding.get_geocode_cache(str(tmp_path))) == {"A"} and set(geocoding.get_places_cache(str(tmp_path))) == {"A"}
        assert set(geocoding.get_places_cache(str(tmp_path))["A"]) == set(geocoding.placeFields)

    def test_zero_results_record_shape(self):
        record = geocoding.parse_place_response({"places": []})
        assert record["status"] == "ZERO_RESULTS" and record["types"] is None and record["name"] is None

    def test_types_joined_and_business_status_kept(self):
        record = geocoding.parse_place_response(place_payload(types=("hospital", "health", "point_of_interest")))
        assert record["types"] == "hospital,health,point_of_interest" and record["businessStatus"] == "OPERATIONAL"


# ============================================================
# step 3b: attaching the lookups and choosing the site point
# ============================================================

class TestPrepWithLookups:

    def _run(self, spark, tmp_path, monkeypatch, rows, places):
        from utilities import prep_jcAccreditationDF
        monkeypatch.setattr(geocoding, "geocode_addresses",
                            lambda addresses, pathToData, maxCalls=None, keyFilename=None:
                            {a: geocoding.parse_geocode_response(geocode_payload(lat=40.0, lng=-83.0)) for a in addresses})
        seen = {}
        def fake_find_places(queries, pathToData, maxCalls=None, keyFilename=None):
            seen.update(queries)
            return {q: geocoding.parse_place_response(places.get(q.split(",")[0], {"places": []})) for q in queries}
        monkeypatch.setattr(geocoding, "find_places", fake_find_places)
        result = prep_jcAccreditationDF(spark.createDataFrame(rows, JC_SCHEMA), pathToData=str(tmp_path), placesLookup=True)
        return {r["jcHcoId"]: r for r in result.collect()}, seen

    @pytest.mark.parametrize("types, source", [(("hospital",), "place"), (("medical_center",), "place"),
                                               (("medical_clinic", "health"), "place"), (("health",), "place"),
                                               (("university",), "address"), (("point_of_interest",), "address"),
                                               (("nail_salon",), "address")])
    def test_site_point_by_place_type(self, spark, tmp_path, monkeypatch, types, source):
        rows, _ = self._run(spark, tmp_path, monkeypatch, [jc_row(site="A")], {"A": place_payload(lat=41.0, lng=-83.0, types=types)})
        assert rows["1"]["jcSiteLocationSource"] == source
        assert rows["1"]["jcSiteLat"] == (41.0 if source == "place" else 40.0)
        assert rows["1"]["jcPlaceIsHospital"] == (1 if "hospital" in types else 0)

    def test_no_place_found_falls_back_to_address(self, spark, tmp_path, monkeypatch):
        rows, _ = self._run(spark, tmp_path, monkeypatch, [jc_row(site="Unknown")], {})
        r = rows["1"]
        assert r["jcPlaceStatus"] == "ZERO_RESULTS" and r["jcPlaceIsHospital"] is None and r["jcPlaceDistanceKm"] is None
        assert r["jcSiteLocationSource"] == "address" and r["jcSiteLat"] == 40.0 and r["jcSiteLng"] == -83.0

    def test_place_distance_from_the_organization(self, spark, tmp_path, monkeypatch):
        rows, _ = self._run(spark, tmp_path, monkeypatch, [jc_row(site="A")], {"A": place_payload(lat=40.0 + 2 * KM, lng=-83.0)})
        assert abs(rows["1"]["jcPlaceDistanceKm"] - 2.0) < 0.01

    def test_bias_is_the_address_point(self, spark, tmp_path, monkeypatch):
        _, seen = self._run(spark, tmp_path, monkeypatch, [jc_row(site="A"), jc_row(hco="2", site="B")], {})
        assert seen == {"A, OH": (40.0, -83.0), "B, OH": (40.0, -83.0)}

    def test_address_columns_untouched_by_the_place(self, spark, tmp_path, monkeypatch):
        rows, _ = self._run(spark, tmp_path, monkeypatch, [jc_row(site="A")], {"A": place_payload(lat=41.0, lng=-84.0)})
        assert rows["1"]["jcLat"] == 40.0 and rows["1"]["jcLng"] == -83.0 and rows["1"]["jcPlaceLat"] == 41.0

    def test_no_places_lookup_adds_no_place_columns(self, spark, tmp_path, monkeypatch):
        from utilities import prep_jcAccreditationDF
        monkeypatch.setattr(geocoding, "geocode_addresses",
                            lambda addresses, pathToData, maxCalls=None, keyFilename=None:
                            {a: geocoding.parse_geocode_response(geocode_payload()) for a in addresses})
        monkeypatch.setattr(geocoding, "find_places", lambda *a, **k: pytest.fail("places must not be searched"))
        result = prep_jcAccreditationDF(spark.createDataFrame([jc_row()], JC_SCHEMA), pathToData=str(tmp_path))
        assert "jcLat" in result.columns and "jcPlaceQuery" in result.columns
        assert not any(c.startswith("jcPlaceL") or c.startswith("jcPlaceS") or c.startswith("jcSiteL") for c in result.columns)

    def test_parquet_written(self, spark, tmp_path, monkeypatch):
        from utilities import prep_jcAccreditationDF
        monkeypatch.setattr(geocoding, "geocode_addresses",
                            lambda addresses, pathToData, maxCalls=None, keyFilename=None:
                            {a: geocoding.parse_geocode_response(geocode_payload()) for a in addresses})
        monkeypatch.setattr(geocoding, "find_places",
                            lambda queries, pathToData, maxCalls=None, keyFilename=None:
                            {q: geocoding.parse_place_response(place_payload()) for q in queries})
        out = str(tmp_path / "jc.parquet")
        prep_jcAccreditationDF(spark.createDataFrame([jc_row(), jc_row(hco="2")], JC_SCHEMA), pathToData=str(tmp_path),
                               filename=out, placesLookup=True)
        written = spark.read.parquet(out)
        assert written.count() == 2 and "jcSiteLat" in written.columns


# ============================================================
# step 4a: the nearest hospital
# ============================================================

class TestNearest:

    def _pos(self, spark, rows):
        return spark.createDataFrame(rows, POS_SCHEMA)

    def _sites(self, spark, rows):
        return spark.createDataFrame(rows, SITE_SCHEMA)

    def test_closest_wins(self, spark):
        from utilities import add_pos_nearest_info
        pos = self._pos(spark, [pos_row("1", "FAR", lat=40.0 + 3 * KM), pos_row("2", "NEAR", lat=40.0 + 1 * KM), pos_row("3", "MID", lat=40.0 + 2 * KM)])
        r = add_pos_nearest_info(self._sites(spark, [site_row("1", "S", 40.0, -83.0)]), pos, latCol="jcSiteLat", lngCol="jcSiteLng").collect()[0]
        assert r["posNearestCcn"] == "2" and abs(r["posNearestDistanceKm"] - 1) < 0.01
        assert r["posNearestSecondCcn"] == "3" and abs(r["posNearestSecondDistanceKm"] - 2) < 0.01

    def test_tie_prefers_active_then_lower_ccn(self, spark):
        from utilities import add_pos_nearest_info
        pos = self._pos(spark, [pos_row("9", "CLOSED", active=0), pos_row("5", "ACTIVE B"), pos_row("3", "ACTIVE A")])
        r = add_pos_nearest_info(self._sites(spark, [site_row("1", "S", 40.0, -83.0)]), pos).collect()[0]
        assert r["posNearestCcn"] == "3" and r["posNearestSecondCcn"] == "5"

    def test_only_hospitals_with_coordinates_compete(self, spark):
        from utilities import add_pos_nearest_info
        pos = self._pos(spark, [pos_row("1", "SNF", hospital=0), pos_row("2", "NO COORDS", lat=None, lng=None), pos_row("3", "H", lat=40.0 + 1 * KM)])
        r = add_pos_nearest_info(self._sites(spark, [site_row("1", "S", 40.0, -83.0)]), pos).collect()[0]
        assert r["posNearestCcn"] == "3" and r["posNearestSecondCcn"] is None

    def test_other_state_ignored(self, spark):
        from utilities import add_pos_nearest_info
        pos = self._pos(spark, [pos_row("1", "PA", state="PA"), pos_row("2", "OH", lat=40.0 + 5 * KM)])
        r = add_pos_nearest_info(self._sites(spark, [site_row("1", "S", 40.0, -83.0)]), pos).collect()[0]
        assert r["posNearestCcn"] == "2"

    def test_site_without_coordinates_or_state_hospitals_keeps_nulls(self, spark):
        from utilities import add_pos_nearest_info
        pos = self._pos(spark, [pos_row("1", "OH")])
        sites = self._sites(spark, [site_row("1", "NO COORDS", None, None), site_row("2", "WV", 40.0, -83.0, state="WV")])
        rows = {r["jcHcoId"]: r for r in add_pos_nearest_info(sites, pos).collect()}
        assert len(rows) == 2 and rows["1"]["posNearestCcn"] is None and rows["2"]["posNearestCcn"] is None

    def test_prefix_and_point_columns(self, spark):
        from utilities import add_pos_nearest_info
        pos = self._pos(spark, [pos_row("1", "AT ADDRESS"), pos_row("2", "AT SITE", lat=41.0)])
        sites = self._sites(spark, [site_row("1", "S", 40.0, -83.0, siteLat=41.0)])
        r = add_pos_nearest_info(add_pos_nearest_info(sites, pos, latCol="jcSiteLat", lngCol="jcSiteLng"),
                                 pos, latCol="jcLat", lngCol="jcLng", prefix="posParentNearest").collect()[0]
        assert r["posNearestCcn"] == "2" and r["posParentNearestCcn"] == "1"

    def test_geocode_type_of_the_hospital_carried(self, spark):
        from utilities import add_pos_nearest_info
        pos = self._pos(spark, [pos_row("1", "H", geocodeType="APPROXIMATE")])
        r = add_pos_nearest_info(self._sites(spark, [site_row("1", "S", 40.0, -83.0)]), pos).collect()[0]
        assert r["posNearestGeocodeLocationType"] == "APPROXIMATE" and r["posNearestActive"] == 1


# ============================================================
# step 4b: the CCN assignment, pass by pass
# ============================================================

class TestAssignment:

    def _pos(self, spark, rows):
        return spark.createDataFrame(rows, POS_SCHEMA)

    def _sites(self, spark, rows):
        return spark.createDataFrame(rows, SITE_SCHEMA)

    def _one(self, spark, pos, site, **kwargs):
        from utilities import add_pos_ccn_info
        return add_pos_ccn_info(self._sites(spark, [site]), self._pos(spark, pos), **kwargs).collect()[0]

    @pytest.mark.parametrize("km, method", [(0.0, "nearestSite"), (0.49, "nearestSite"), (0.51, "none")])
    def test_site_threshold(self, spark, km, method):
        r = self._one(spark, [pos_row("1", "H", lat=40.0 + km * KM)], site_row("1", "S", 45.0, -83.0, siteLat=40.0, search="Zzz"))
        assert r["posMatchMethod"] == method

    @pytest.mark.parametrize("km, method", [(0.3, "none"), (0.7, "nearestSite")])
    def test_threshold_argument(self, spark, km, method):
        r = self._one(spark, [pos_row("1", "H", lat=40.0 + 0.5 * KM)], site_row("1", "S", 45.0, -83.0, siteLat=40.0, search="Zzz"), maxDistanceKm=km)
        assert r["posMatchMethod"] == method

    def test_parent_when_site_point_has_nothing(self, spark):
        r = self._one(spark, [pos_row("1", "PARENT", lat=40.0)], site_row("1", "CAMPUS", 40.0, -83.0, siteLat=41.0, search="Zzz"))
        assert r["posMatchMethod"] == "nearestParent" and r["posCcn"] == "1" and r["posCcnPass"] == "parent" and r["posCcnDistanceKm"] == 0

    def test_address_sourced_site_is_parent_even_when_found(self, spark):
        r = self._one(spark, [pos_row("1", "H", lat=40.0)], site_row("1", "S", 40.0, -83.0, source="address"))
        assert r["posMatchMethod"] == "nearestParent" and r["posCcn"] == "1"

    def test_closed_hospital_ignored_when_filtered_out(self, spark):
        pos = [pos_row("1", "CLOSED AT SITE", lat=41.0, active=0), pos_row("2", "PARENT", lat=40.0)]
        posDF = self._pos(spark, pos).filter(F.col("posActive") == 1)
        from utilities import add_pos_ccn_info
        r = add_pos_ccn_info(self._sites(spark, [site_row("1", "S", 40.0, -83.0, siteLat=41.0, search="Zzz")]), posDF).collect()[0]
        assert r["posCcn"] == "2" and r["posMatchMethod"] == "nearestParent"

    @pytest.mark.parametrize("secondKm, ambiguous", [(0.05, 1), (0.09, 1), (0.15, 0)])
    def test_ambiguity_flag(self, spark, secondKm, ambiguous):
        pos = [pos_row("1", "NEAREST", lat=40.0), pos_row("2", "SECOND", lat=40.0 + secondKm * KM)]
        r = self._one(spark, pos, site_row("1", "S", 40.0, -83.0))
        assert r["posCcn"] == "1" and r["posMatchAmbiguous"] == ambiguous

    def test_ambiguity_null_when_unmatched(self, spark):
        r = self._one(spark, [pos_row("1", "H", lat=45.0)], site_row("1", "S", 40.0, -83.0, search="Zzz"))
        assert r["posMatchMethod"] == "none" and r["posMatchAmbiguous"] is None and r["posCcn"] is None

    def test_named_pass_recovers_identical_name_within_reach(self, spark):
        r = self._one(spark, [pos_row("1", "MARIA PARHAM MEDICAL CENTER", lat=40.0 + 2 * KM)],
                      site_row("1", "S", 40.0, -83.0, search="Maria Parham Health"))
        assert r["posMatchMethod"] == "nearestNamed" and r["posCcn"] == "1" and r["posCcnPass"] == "named"
        assert r["posCcnNameScore"] == 1.0 and abs(r["posCcnDistanceKm"] - 2) < 0.01 and r["posMatchAmbiguous"] == 0

    def test_named_pass_respects_reach(self, spark):
        r = self._one(spark, [pos_row("1", "MARIA PARHAM MEDICAL CENTER", lat=40.0 + 2 * KM)],
                      site_row("1", "S", 40.0, -83.0, search="Maria Parham Health"), relaxedDistanceKm=1.0)
        assert r["posMatchMethod"] == "none"

    @pytest.mark.parametrize("search, method", [("Maria Parham Health", "nearestNamed"), ("Sanford Medical Center Fargo", "none"),
                                                ("Hospital", "none"), ("Essentia Health Duluth", "none")])
    def test_named_pass_requires_full_name_score(self, spark, search, method):
        r = self._one(spark, [pos_row("1", "MARIA PARHAM MEDICAL CENTER", lat=40.0 + 2 * KM), pos_row("2", "ESSENTIA HEALTH FARGO", lat=40.0 + 3 * KM)],
                      site_row("1", "S", 40.0, -83.0, search=search))
        assert r["posMatchMethod"] == method

    def test_named_pass_best_score_beats_nearer_weaker_name(self, spark):
        pos = [pos_row("1", "MARIA PARHAM MEDICAL CENTER", lat=40.0 + 4 * KM), pos_row("2", "PARHAM SURGERY CENTER", lat=40.0 + 1 * KM)]
        r = self._one(spark, pos, site_row("1", "S", 40.0, -83.0, search="Maria Parham Health"))
        assert r["posCcn"] == "1"

    def test_named_pass_ties_by_distance(self, spark):
        pos = [pos_row("1", "MARIA PARHAM MEDICAL CENTER", lat=40.0 + 4 * KM), pos_row("2", "MARIA PARHAM HOSPITAL", lat=40.0 + 1 * KM)]
        r = self._one(spark, pos, site_row("1", "S", 40.0, -83.0, search="Maria Parham Health"))
        assert r["posCcn"] == "2"

    def test_named_pass_lower_threshold(self, spark):
        r = self._one(spark, [pos_row("1", "ESSENTIA HEALTH FARGO", lat=40.0 + 2 * KM)],
                      site_row("1", "S", 40.0, -83.0, search="Sanford Medical Center Fargo"), nameScoreMin=0.5)
        assert r["posMatchMethod"] == "nearestNamed" and r["posCcnNameScore"] == 0.5

    def test_named_pass_does_not_touch_matched_sites(self, spark):
        pos = [pos_row("1", "AT SITE", lat=40.0), pos_row("2", "MARIA PARHAM MEDICAL CENTER", lat=40.0 + 2 * KM)]
        r = self._one(spark, pos, site_row("1", "S", 40.0, -83.0, search="Maria Parham Health"))
        assert r["posCcn"] == "1" and r["posMatchMethod"] == "nearestSite" and r["posCcnNameScore"] is None

    def test_without_site_columns_everything_is_parent(self, spark):
        from utilities import add_pos_ccn_info
        sites = self._sites(spark, [site_row("1", "S", 40.0, -83.0)]).drop("jcSiteLat", "jcSiteLng", "jcSiteLocationSource")
        r = add_pos_ccn_info(sites, self._pos(spark, [pos_row("1", "H")])).collect()[0]
        assert r["posMatchMethod"] == "nearestParent" and r["jcSiteLocationSource"] == "address"

    def test_several_sites_share_a_ccn_and_rows_are_kept(self, spark):
        from utilities import add_pos_ccn_info
        sites = self._sites(spark, [site_row("1", "MAIN", 40.0, -83.0), site_row("1", "CAMPUS", 40.0, -83.0, siteLat=41.0, search="Zzz"),
                                    site_row("2", "LOST", 45.0, -83.0, search="Zzz")])
        rows = add_pos_ccn_info(sites, self._pos(spark, [pos_row("1", "H")])).collect()
        assert len(rows) == 3 and {r["posCcn"] for r in rows} == {"1", None}
        assert {r["posMatchMethod"] for r in rows} == {"nearestSite", "nearestParent", "none"}


class TestOverrides:

    def _run(self, spark, overrides):
        from utilities import add_pos_ccn_info
        sites = spark.createDataFrame([site_row("1", "A", 40.0, -83.0), site_row("2", "B", 40.0, -83.0)], SITE_SCHEMA)
        pos = spark.createDataFrame([pos_row("1", "H"), pos_row("7", "OTHER", lat=45.0)], POS_SCHEMA)
        ov = spark.createDataFrame(overrides, "jcHcoId string, jcSiteName string, keepCcn string")
        return {r["jcHcoId"]: r for r in add_pos_ccn_info(sites, pos, overridesDF=ov).collect()}

    def test_override_replaces_and_clears_the_rule_columns(self, spark):
        rows = self._run(spark, [("1", "A", "7")])
        assert rows["1"]["posMatchMethod"] == "override" and rows["1"]["posCcn"] == "7" and rows["1"]["posCcnFacName"] == "OTHER"
        assert rows["1"]["posCcnDistanceKm"] is None and rows["1"]["posCcnPass"] is None and rows["1"]["posMatchAmbiguous"] is None
        assert rows["2"]["posMatchMethod"] == "nearestSite"

    def test_blank_override_means_no_ccn(self, spark):
        rows = self._run(spark, [("1", "A", "")])
        assert rows["1"]["posMatchMethod"] == "none" and rows["1"]["posCcn"] is None

    def test_override_for_absent_site_ignored_and_unknown_ccn_kept(self, spark):
        rows = self._run(spark, [("9", "Z", "1"), ("2", "B", "999999")])
        assert len(rows) == 2 and rows["1"]["posMatchMethod"] == "nearestSite"
        assert rows["2"]["posCcn"] == "999999" and rows["2"]["posCcnFacName"] is None and rows["2"]["posMatchMethod"] == "override"

    def test_duplicate_override_rows_do_not_duplicate_sites(self, spark):
        rows = self._run(spark, [("1", "A", "7"), ("1", "A", "7")])
        assert len(rows) == 2


# ============================================================
# step 4c: the per CCN table and the claims join
# ============================================================

class TestPerCcnTable:

    def _matched(self, spark, rows):
        schema = "jcHcoId string, jcSiteName string, jcProgram string, jcProgramRank int, posCcn string, posMatchMethod string, posCcnDistanceKm double"
        return spark.createDataFrame(rows, schema)

    def test_best_program_and_its_site(self, spark):
        from utilities import get_ccn_jc_info
        df = self._matched(spark, [("1", "B", "Primary Stroke Center", 3, "X", "nearestSite", 0.0),
                                   ("1", "A", "Advanced Comprehensive Stroke Center", 1, "X", "nearestParent", 0.2),
                                   ("2", "C", "Acute Stroke Ready Hospital", 4, "Y", "nearestNamed", 2.0),
                                   ("3", "D", "Primary Stroke Center", 3, None, "none", None)])
        rows = {r["posCcn"]: r for r in get_ccn_jc_info(df).collect()}
        assert set(rows) == {"X", "Y"}
        x = rows["X"]
        assert x["jcSites"] == 2 and x["jcBestProgram"] == "Advanced Comprehensive Stroke Center" and x["jcBestProgramSite"] == "A"
        assert x["jcBestProgramMatchMethod"] == "nearestParent" and x["jcMatchMethods"] == ["nearestParent", "nearestSite"]
        assert abs(x["jcMaxCcnDistanceKm"] - 0.2) < 1e-9 and sorted(x["jcSiteNames"]) == ["A", "B"]

    def test_equal_rank_ties_by_program_then_site_name(self, spark):
        from utilities import get_ccn_jc_info
        df = self._matched(spark, [("1", "Zed", "Primary Stroke Center", 3, "X", "nearestSite", 0.0),
                                   ("2", "Alpha", "Primary Stroke Center", 3, "X", "nearestSite", 0.0)])
        assert get_ccn_jc_info(df).collect()[0]["jcBestProgramSite"] == "Alpha"

    @pytest.mark.parametrize("method, sites, confidence", [("nearestSite", 1, 4), ("nearestSite", 3, 3), ("nearestParent", 1, 2),
                                                           ("nearestParent", 2, 2), ("override", 1, 2), ("nearestNamed", 1, 1)])
    def test_confidence_scale(self, spark, method, sites, confidence):
        from utilities import get_ccn_jc_info
        rows = [(str(i), f"S{i}", "Primary Stroke Center", 3, "X", "nearestSite" if i > 0 else method, 0.0) for i in range(sites)]
        rows[0] = ("0", "A", "Comprehensive Stroke Center", 1, "X", method, 0.0)
        assert get_ccn_jc_info(self._matched(spark, rows)).collect()[0]["jcCertificationConfidence"] == confidence

    def test_confidence_follows_the_best_programs_site_not_the_others(self, spark):
        from utilities import get_ccn_jc_info
        df = self._matched(spark, [("1", "MAIN", "Primary Stroke Center", 3, "X", "nearestSite", 0.0),
                                   ("1", "CAMPUS", "Comprehensive Stroke Center", 1, "X", "nearestParent", 0.0)])
        r = get_ccn_jc_info(df).collect()[0]
        assert r["jcBestProgramMatchMethod"] == "nearestParent" and r["jcCertificationConfidence"] == 2


class TestClaimsJoin:

    def test_every_claim_year_gets_the_same_certification(self, spark):
        from cms.base import add_provider_stroke_certification_info
        base = spark.createDataFrame([("X", y) for y in (2015, 2018, 2022)] + [("Z", 2019)], "PROVIDER string, THRU_DT_YEAR int")
        ccn = spark.createDataFrame([("X", 1, "Primary Stroke Center", 4)], "posCcn string, jcSites int, jcBestProgram string, jcCertificationConfidence int")
        rows = add_provider_stroke_certification_info(base, ccn).collect()
        assert len(rows) == 4
        assert all(r["providerStrokeCertification"] == "Primary Stroke Center" for r in rows if r["PROVIDER"] == "X")
        assert [r["providerStrokeCertification"] for r in rows if r["PROVIDER"] == "Z"] == [None]

    def test_no_row_multiplication_and_only_lowercase_additions(self, spark):
        from cms.base import add_provider_stroke_certification_info
        base = spark.createDataFrame([("X", 2019), ("X", 2019)], "PROVIDER string, THRU_DT_YEAR int")
        ccn = spark.createDataFrame([("X", 2, "Primary Stroke Center", 3)], "posCcn string, jcSites int, jcBestProgram string, jcCertificationConfidence int")
        result = add_provider_stroke_certification_info(base, ccn)
        assert result.count() == 2
        assert set(result.columns) - {"PROVIDER", "THRU_DT_YEAR"} == {"providerStrokeCertification", "providerStrokeCertificationConfidence", "providerStrokeCertificationSites"}


# ============================================================
# the name score used by the last pass
# ============================================================

class TestNameTokensAndScore:

    def _tokens(self, spark, name):
        from utilities import get_nameTokens
        return spark.createDataFrame([(name,)], "n string").select(get_nameTokens(F.col("n")).alias("t")).collect()[0]["t"]

    @pytest.mark.parametrize("name, tokens", [
        ("ST MARY MEDICAL CENTER", ["mary"]), ("St. Mary's Medical Center", ["mary", "s"]),
        ("Maria Parham Health", ["maria", "parham"]), ("General Acute Care Hospital", []),
        ("Hospital Hospital Hospital", []), ("Mercy Catholic Fitzgerald Hospital", ["catholic", "fitzgerald"]),
        ("UCSF Medical Center at Mission Bay", ["ucsf", "at", "mission", "bay"]), ("", []), (None, [])])
    def test_tokens(self, spark, name, tokens):
        assert self._tokens(spark, name) == tokens

    @pytest.mark.parametrize("a, b, score", [
        ("Maria Parham Health", "MARIA PARHAM MEDICAL CENTER", 1.0),
        ("MARIA PARHAM MEDICAL CENTER", "Maria Parham Health", 1.0),
        ("Angel Medical Center", "ANGEL MEDICAL CENTER", 1.0),
        ("UConn John Dempsey Hospital", "JOHN DEMPSEY HOSPITAL", 1.0),
        ("Corpus Christi Medical Center - Doctors", "CHRISTUS SPOHN HOSPITAL CORPUS CHRISTI", 2 / 3),
        ("Mount Sinai Brooklyn", "SOUTH BROOKLYN HEALTH", 0.5),
        ("Hospital", "General Hospital", 0.0), ("Alpha", "Beta", 0.0), (None, "Beta", 0.0)])
    def test_score(self, spark, a, b, score):
        from utilities import get_nameScore
        got = spark.createDataFrame([(a, b)], "a string, b string").select(get_nameScore(F.col("a"), F.col("b")).alias("s")).collect()[0]["s"]
        assert abs(got - score) < 1e-9

    def test_score_is_symmetric(self, spark):
        from utilities import get_nameScore
        df = spark.createDataFrame([("Mercy Catholic Fitzgerald Hospital", "CATHOLIC HEALTH SOUTHEAST HOSPITAL")], "a string, b string")
        r = df.select(get_nameScore(F.col("a"), F.col("b")).alias("ab"), get_nameScore(F.col("b"), F.col("a")).alias("ba")).collect()[0]
        assert r["ab"] == r["ba"] == 0.5


# ============================================================
# the POS side the match relies on
# ============================================================

class TestPosCandidates:

    @pytest.mark.parametrize("ccn, kind", [("360001", "acute"), ("360879", "acute"), ("360880", "other"), ("361300", "cah"), ("361399", "cah"),
                                           ("362000", "ltch"), ("362299", "ltch"), ("363024", "other"), ("363025", "rehabilitation"),
                                           ("363300", "childrens"), ("364000", "psychiatric"), ("364499", "psychiatric"),
                                           ("369800", "transplant"), ("369899", "transplant"), ("369900", "other"),
                                           ("29002E", "emergency"), ("34012F", "federal")])
    def test_hospital_type_by_ccn_range(self, spark, ccn, kind):
        from utilities import add_posHospitalType
        df = spark.createDataFrame([(ccn, 1)], "PRVDR_NUM string, posHospital int")
        assert add_posHospitalType(df).collect()[0]["posHospitalType"] == kind

    def test_non_hospital_rows_have_no_type(self, spark):
        from utilities import add_posHospital, add_posHospitalType
        df = spark.createDataFrame([("360001", "01"), ("365001", "04"), ("361500", "16")], "PRVDR_NUM string, PRVDR_CTGRY_CD string")
        rows = {r["PRVDR_NUM"]: r for r in add_posHospitalType(add_posHospital(df)).collect()}
        assert rows["360001"]["posHospital"] == 1 and rows["360001"]["posHospitalType"] == "acute"
        assert rows["365001"]["posHospital"] == 0 and rows["365001"]["posHospitalType"] is None
        assert rows["361500"]["posHospital"] == 0 and rows["361500"]["posHospitalType"] is None

    def test_candidate_filter_used_by_the_match_script(self, spark):
        pos = spark.createDataFrame([("360001", 1, 1, "acute"), ("361301", 1, 1, "cah"), ("360002", 1, 0, "acute"),
                                     ("362001", 1, 1, "ltch"), ("369801", 1, 1, "transplant"), ("29001E", 1, 1, "emergency")],
                                    "PRVDR_NUM string, posHospital int, posActive int, posHospitalType string")
        kept = {r["PRVDR_NUM"] for r in pos.filter(F.col("posHospitalType").isin(["acute", "cah"]) & (F.col("posActive") == 1)).collect()}
        assert kept == {"360001", "361301"}
