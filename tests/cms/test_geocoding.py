import io
import json
import pytest
from unittest import mock

import geocoding
import pyspark.sql.functions as F

# ============================================================
# Pure Python tests (no SparkSession needed)
# ============================================================

def ok_payload(lat=39.96, lng=-82.99, locationType="ROOFTOP", partial=None):
    result = {"geometry": {"location": {"lat": lat, "lng": lng}, "location_type": locationType},
              "formatted_address": "123 Main St, Columbus, OH 43215, USA"}
    if partial is not None:
        result["partial_match"] = partial
    return {"status": "OK", "results": [result]}

def fake_urlopen(payloads):
    # Each call pops the next payload; the returned object works as a context manager like urlopen's.
    payloads = list(payloads)
    def opener(url, timeout=None):
        payload = payloads.pop(0)
        response = mock.MagicMock()
        response.__enter__.return_value = io.StringIO(json.dumps(payload))
        return response
    return mock.MagicMock(side_effect=opener)

@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr(geocoding.time, "sleep", lambda s: None)

def write_key(tmp_path, key="secret-key"):
    (tmp_path / "GEOCODING").mkdir(exist_ok=True)
    (tmp_path / "GEOCODING" / "key.csv").write_text(key + "\n")


class TestParseGeocodeResponse:

    def test_ok(self):
        record = geocoding.parse_geocode_response(ok_payload())
        assert record["lat"] == 39.96 and record["lng"] == -82.99
        assert record["locationType"] == "ROOFTOP"
        assert record["partialMatch"] == 0
        assert record["status"] == "OK"
        assert set(record) == set(geocoding.geocodeFields)

    def test_partial_match(self):
        record = geocoding.parse_geocode_response(ok_payload(partial=True))
        assert record["partialMatch"] == 1

    def test_zero_results(self):
        record = geocoding.parse_geocode_response({"status": "ZERO_RESULTS", "results": []})
        assert record["status"] == "ZERO_RESULTS"
        assert record["lat"] is None and record["lng"] is None

    def test_retry_statuses(self):
        for status in ["OVER_QUERY_LIMIT", "OVER_DAILY_LIMIT", "UNKNOWN_ERROR"]:
            with pytest.raises(geocoding.GeocodeRetryError):
                geocoding.parse_geocode_response({"status": status})

    def test_request_denied(self):
        with pytest.raises(RuntimeError, match="REQUEST_DENIED"):
            geocoding.parse_geocode_response({"status": "REQUEST_DENIED", "error_message": "bad key"})

    def test_invalid_request(self):
        with pytest.raises(ValueError, match="INVALID_REQUEST"):
            geocoding.parse_geocode_response({"status": "INVALID_REQUEST"})


class TestGeocodeAddress:

    def test_retry_then_ok(self, no_sleep):
        opener = fake_urlopen([{"status": "OVER_QUERY_LIMIT"}, ok_payload()])
        with mock.patch.object(geocoding, "urlopen", opener):
            record = geocoding.geocode_address("123 MAIN ST, COLUMBUS, OH 43215", "secret-key")
        assert record["status"] == "OK"
        assert opener.call_count == 2

    def test_retries_exhausted(self, no_sleep):
        opener = fake_urlopen([{"status": "OVER_QUERY_LIMIT"}] * 3)
        with mock.patch.object(geocoding, "urlopen", opener):
            with pytest.raises(RuntimeError) as e:
                geocoding.geocode_address("123 MAIN ST", "secret-key", maxRetries=2)
        assert opener.call_count == 3
        assert "secret-key" not in str(e.value)

    def test_key_in_url(self, no_sleep):
        opener = fake_urlopen([ok_payload()])
        with mock.patch.object(geocoding, "urlopen", opener):
            geocoding.geocode_address("123 MAIN ST", "secret-key")
        url = opener.call_args[0][0]
        assert "key=secret-key" in url and "components=country%3AUS" in url


class TestGetApiKey:

    def test_reads_and_strips(self, tmp_path):
        write_key(tmp_path, "  abc  ")
        assert geocoding.get_api_key(str(tmp_path)) == "abc"

    def test_missing_file(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            geocoding.get_api_key(str(tmp_path))

    def test_empty_file(self, tmp_path):
        write_key(tmp_path, "")
        with pytest.raises(ValueError):
            geocoding.get_api_key(str(tmp_path))


class TestGeocodeAddresses:

    def test_all_cached_makes_no_calls_and_needs_no_key(self, tmp_path):
        record = geocoding.parse_geocode_response(ok_payload())
        geocoding.write_geocode_cache({"A": record}, str(tmp_path))
        opener = fake_urlopen([])
        with mock.patch.object(geocoding, "urlopen", opener):
            records = geocoding.geocode_addresses(["A", "A", None], str(tmp_path))
        assert records == {"A": record}
        assert opener.call_count == 0

    def test_miss_is_called_once_and_cached(self, tmp_path):
        write_key(tmp_path)
        opener = fake_urlopen([ok_payload()])
        with mock.patch.object(geocoding, "urlopen", opener):
            records = geocoding.geocode_addresses(["A"], str(tmp_path))
        assert opener.call_count == 1
        assert records["A"]["lat"] == 39.96
        cacheText = (tmp_path / "GEOCODING" / "googleGeocodeCache.json").read_text()
        assert "A" in json.loads(cacheText)
        assert "secret-key" not in cacheText
        with mock.patch.object(geocoding, "urlopen", opener):
            geocoding.geocode_addresses(["A"], str(tmp_path))
        assert opener.call_count == 1

    def test_zero_results_is_cached(self, tmp_path):
        write_key(tmp_path)
        opener = fake_urlopen([{"status": "ZERO_RESULTS", "results": []}])
        with mock.patch.object(geocoding, "urlopen", opener):
            geocoding.geocode_addresses(["A"], str(tmp_path))
            geocoding.geocode_addresses(["A"], str(tmp_path))
        assert opener.call_count == 1

    def test_max_calls_guard(self, tmp_path):
        write_key(tmp_path)
        opener = fake_urlopen([])
        with mock.patch.object(geocoding, "urlopen", opener):
            with pytest.raises(RuntimeError, match="maxCalls"):
                geocoding.geocode_addresses(["A", "B"], str(tmp_path), maxCalls=1)
        assert opener.call_count == 0

    def test_parallel_run_geocodes_every_miss_once(self, tmp_path):
        write_key(tmp_path)
        addresses = [f"ADDR {i}" for i in range(50)]
        opener = fake_urlopen([ok_payload(lat=float(i)) for i in range(50)])
        with mock.patch.object(geocoding, "urlopen", opener):
            records = geocoding.geocode_addresses(addresses, str(tmp_path), flushEvery=10, workers=4)
        assert opener.call_count == 50
        assert set(records) == set(addresses)
        assert set(geocoding.get_geocode_cache(str(tmp_path))) == set(addresses)

    def test_interrupted_run_keeps_records(self, tmp_path, no_sleep):
        write_key(tmp_path)
        opener = fake_urlopen([ok_payload(), {"status": "REQUEST_DENIED"}])
        with mock.patch.object(geocoding, "urlopen", opener):
            with pytest.raises(RuntimeError):
                geocoding.geocode_addresses(["A", "B"], str(tmp_path), flushEvery=100, workers=1)
        assert list(geocoding.get_geocode_cache(str(tmp_path))) == ["A"]
        assert geocoding.get_geocode_cache_misses(["A", "B"], str(tmp_path)) == ["B"]


# ============================================================
# Spark tests
# ============================================================

class TestAddAddress:

    COLS = ["street", "city", "state", "zip"]

    def test_normalizes(self, spark):
        df = spark.createDataFrame([("  123  main st ", "columbus ", " oh", "43215-1234")], self.COLS)
        row = geocoding.add_address(df, "street", "city", "state", "zip", "address").collect()[0]
        assert row["address"] == "123 MAIN ST, COLUMBUS, OH 43215"

    def test_null_street_falls_back_to_city_state_zip(self, spark):
        df = spark.createDataFrame([(None, "COLUMBUS", "OH", "43215")], "street string, city string, state string, zip string")
        row = geocoding.add_address(df, "street", "city", "state", "zip", "address").collect()[0]
        assert row["address"] == "COLUMBUS, OH 43215"


class TestAddGeocodeInfo:

    def test_columns_joined_back(self, spark, tmp_path, monkeypatch):
        records = {"A": geocoding.parse_geocode_response(ok_payload()),
                   "B": geocoding.parse_geocode_response({"status": "ZERO_RESULTS", "results": []})}
        monkeypatch.setattr(geocoding, "geocode_addresses", lambda addresses, pathToData, maxCalls=None, keyFilename=None: records)
        df = spark.createDataFrame([("1", "A"), ("2", "A"), ("3", "B")], ["id", "addr"])
        result = geocoding.add_geocode_info(df, "addr", str(tmp_path), "pos")
        rows = {r["id"]: r for r in result.collect()}
        assert len(rows) == 3
        assert rows["1"]["posLat"] == 39.96 and rows["2"]["posLng"] == -82.99
        assert rows["1"]["posGeocodeLocationType"] == "ROOFTOP"
        assert rows["3"]["posLat"] is None and rows["3"]["posGeocodeStatus"] == "ZERO_RESULTS"
        added = set(result.columns) - set(df.columns)
        assert added == {"posLat", "posLng", "posGeocodeLocationType", "posGeocodeFormattedAddress",
                         "posGeocodePartialMatch", "posGeocodeStatus"}
        assert all(c[0].islower() for c in added)
        assert dict(result.dtypes)["posLat"] == "double"


class TestPrepPosDF:

    POS_COLS = ["PRVDR_NUM", "FAC_NAME", "PRVDR_CTGRY_CD", "PRVDR_CTGRY_SBTYP_CD", "FIPS_STATE_CD", "FIPS_CNTY_CD",
                "CBSA_URBN_RRL_IND", "PGM_TRMNTN_CD", "ST_ADR", "CITY_NAME", "STATE_CD", "ZIP_CD"]

    def _df(self, spark):
        return spark.createDataFrame(
            [("360001", "OHIO HOSPITAL", "01", "01", "39", "049", "U", "00", "123 MAIN ST", "COLUMBUS", "OH", "43215"),
             ("360002", "CLOSED HOSPITAL", "01", "01", "39", "049", "U", "01", "789 OAK ST", "COLUMBUS", "OH", "43215"),
             ("365001", "OHIO SNF", "04", "01", "39", "049", "U", "00", "456 ELM ST", "COLUMBUS", "OH", "43215")],
            self.POS_COLS)

    def test_only_hospitals_geocoded_and_parquet_written(self, spark, tmp_path, monkeypatch):
        from utilities import prep_posDF
        seen = []
        def fake_geocode_addresses(addresses, pathToData, maxCalls=None, keyFilename=None):
            seen.extend(addresses)
            return {a: geocoding.parse_geocode_response(ok_payload()) for a in addresses}
        monkeypatch.setattr(geocoding, "geocode_addresses", fake_geocode_addresses)
        filename = str(tmp_path / "pos.parquet")
        result = prep_posDF(self._df(spark), pathToData=str(tmp_path), filename=filename)
        assert sorted(seen) == ["123 MAIN ST, COLUMBUS, OH 43215", "789 OAK ST, COLUMBUS, OH 43215"]
        rows = {r["PRVDR_NUM"]: r for r in result.collect()}
        assert len(rows) == 3
        assert rows["360001"]["posLat"] == 39.96 and rows["360001"]["posAddress"] == "123 MAIN ST, COLUMBUS, OH 43215"
        assert rows["360001"]["posActive"] == 1 and rows["360002"]["posActive"] == 0
        assert rows["360002"]["posLat"] == 39.96
        assert rows["365001"]["posLat"] is None and rows["365001"]["posAddress"] == "456 ELM ST, COLUMBUS, OH 43215"
        assert rows["365001"]["posZip"] == "43215"
        assert spark.read.parquet(filename).count() == 3

    def test_activeOnly_skips_terminated_hospitals(self, spark, tmp_path, monkeypatch):
        from utilities import prep_posDF
        seen = []
        def fake_geocode_addresses(addresses, pathToData, maxCalls=None, keyFilename=None):
            seen.extend(addresses)
            return {a: geocoding.parse_geocode_response(ok_payload()) for a in addresses}
        monkeypatch.setattr(geocoding, "geocode_addresses", fake_geocode_addresses)
        result = prep_posDF(self._df(spark), pathToData=str(tmp_path), activeOnly=True)
        assert seen == ["123 MAIN ST, COLUMBUS, OH 43215"]
        rows = {r["PRVDR_NUM"]: r for r in result.collect()}
        assert len(rows) == 3
        assert rows["360001"]["posLat"] == 39.96 and rows["360002"]["posLat"] is None

    def test_no_pathToData_skips_geocoding(self, spark, monkeypatch):
        from utilities import prep_posDF
        monkeypatch.setattr(geocoding, "geocode_addresses", lambda *a, **k: pytest.fail("should not geocode"))
        result = prep_posDF(self._df(spark))
        assert "posAddress" in result.columns and "posLat" not in result.columns
        assert {"posHospital", "posCah", "posShortTerm"} <= set(result.columns) and "hospital" not in result.columns


class TestPrepJcAccreditationDF:

    JC_COLS = ["HCO ID", "Organization Name", "Organization Doing Business As (DBA) Name", "State", "City",
               "Street Address", "Postal Code", "Site Name", "Site Doing Business As (DBA) Name", "Program",
               "Effective Date", "Status"]
    JC_SCHEMA = ", ".join(f"`{c}` string" for c in JC_COLS)

    def _df(self, spark):
        return spark.createDataFrame(
            [("1", "Ohio Health", None, "Ohio", "Columbus", "123 Main St", "43215-1234", "Ohio Hospital", None,
              "Hospital", "01/01/2020", "Accredited"),
             ("1", "Ohio Health", None, "Ohio", "Columbus", "123 Main St", "43215-1234", "Ohio Hospital", None,
              "Laboratory", "01/01/2020", "Accredited"),
             ("2", "Elm Org", None, "oh", "Columbus", "456 Elm St", "43215", None, "Elm DBA",
              "Hospital", "01/01/2020", "Accredited"),
             ("3", "NA Org", None, "OH", "Columbus", "789 Oak St", "43215", "Legal Entity LLC", "N/A",
              "Hospital", "01/01/2020", "Accredited"),
             ("4", "Generic Org", None, "OH", "Columbus", "789 Oak St", "43215", "Saint John Hospital", "General Acute Care Hospital",
              "Hospital", "01/01/2020", "Accredited")],
            self.JC_SCHEMA)

    def test_renames_geocodes_once_per_address_and_writes_parquet(self, spark, tmp_path, monkeypatch):
        from utilities import prep_jcAccreditationDF
        seen = []
        def fake_geocode_addresses(addresses, pathToData, maxCalls=None, keyFilename=None):
            seen.extend(addresses)
            return {a: geocoding.parse_geocode_response(ok_payload()) for a in addresses}
        monkeypatch.setattr(geocoding, "geocode_addresses", fake_geocode_addresses)
        filename = str(tmp_path / "jc.parquet")
        result = prep_jcAccreditationDF(self._df(spark), pathToData=str(tmp_path), filename=filename)
        assert sorted(seen) == ["123 MAIN ST, COLUMBUS, OHIO 43215", "456 ELM ST, COLUMBUS, OH 43215", "789 OAK ST, COLUMBUS, OH 43215"]
        rows = result.collect()
        assert len(rows) == 5
        assert all(c[0].islower() for c in result.columns)
        byId = {(r["jcHcoId"], r["jcProgram"]): r for r in rows}
        assert byId[("3", "Hospital")]["jcSearchName"] == "Legal Entity LLC"
        assert byId[("4", "Hospital")]["jcSearchName"] == "Saint John Hospital"
        assert byId[("1", "Hospital")]["jcState"] == "OH" and byId[("1", "Hospital")]["jcZip"] == "43215"
        assert byId[("2", "Hospital")]["jcState"] == "OH"
        assert byId[("1", "Hospital")]["jcSearchName"] == "Ohio Hospital" and byId[("1", "Hospital")]["jcPlaceQuery"] == "Ohio Hospital, OH"
        assert byId[("2", "Hospital")]["jcSearchName"] == "Elm DBA" and byId[("2", "Hospital")]["jcPlaceQuery"] == "Elm DBA, OH"
        assert byId[("1", "Hospital")]["jcSiteName"] == "Ohio Hospital"
        assert byId[("2", "Hospital")]["jcSiteName"] == "Elm DBA"
        assert byId[("2", "Hospital")]["jcLat"] == 39.96
        assert spark.read.parquet(filename).count() == 5

    def test_no_pathToData_skips_geocoding(self, spark, monkeypatch):
        from utilities import prep_jcAccreditationDF
        monkeypatch.setattr(geocoding, "geocode_addresses", lambda *a, **k: pytest.fail("should not geocode"))
        result = prep_jcAccreditationDF(self._df(spark))
        assert "jcAddress" in result.columns and "jcLat" not in result.columns


class TestKeyFilename:

    def test_get_api_key_override(self, tmp_path):
        keyFile = tmp_path / "elsewhere.csv"
        keyFile.write_text("xyz\n")
        assert geocoding.get_api_key(str(tmp_path), keyFilename=str(keyFile)) == "xyz"

    def test_geocode_addresses_uses_override(self, tmp_path):
        keyFile = tmp_path / "elsewhere.csv"
        keyFile.write_text("xyz\n")
        opener = fake_urlopen([ok_payload()])
        with mock.patch.object(geocoding, "urlopen", opener):
            geocoding.geocode_addresses(["A"], str(tmp_path), keyFilename=str(keyFile))
        assert "key=xyz" in opener.call_args[0][0]


class TestJcProgramRank:

    JC_SCHEMA = TestPrepJcAccreditationDF.JC_SCHEMA

    def _row(self, hco, site, street, program, date="01/01/2020"):
        return (hco, "Org " + hco, None, "OH", "Columbus", street, "43215", site, None, program, date, "Accredited")

    def _df(self, spark):
        return spark.createDataFrame(
            [self._row("1", "A", "1 Main St", "Advanced Primary Stroke Center", "01/01/2018"),
             self._row("1", "A", "1 Main St", "Advanced Comprehensive Stroke Center", "01/01/2021"),
             self._row("2", "B", "2 Main St", "Hospital"),
             self._row("2", "B", "2 Main St", "Ambulatory Care"),
             self._row("3", "C", "3 Main St", "Acute Stroke Ready Hospital"),
             self._row("3", "C", "3 Main St", "Hospital"),
             self._row("4", "D", "4 Main St", "thrombectomy-capable stroke center"),
             self._row("4", "D", "4 Main St", "Primary Care Medical Home"),
             self._row("5", "E", "5 Main St", "Stroke Rehabilitation"),
             self._row("6", "F", "6 Main St", "Comprehensive Cardiac Center"),
             self._row("7", "G", "7 Main St", "Stroke  Rehabilitation"),
             self._row("7", "G", "7 Main St", "Primary Stroke Center")],
            self.JC_SCHEMA)

    def test_rank_by_keyword(self, spark):
        from utilities import prep_jcAccreditationDF
        rows = prep_jcAccreditationDF(self._df(spark)).collect()
        ranks = {r["jcProgram"]: r["jcProgramRank"] for r in rows}
        assert ranks["Advanced Comprehensive Stroke Center"] == 1
        assert ranks["thrombectomy-capable stroke center"] == 2
        assert ranks["Advanced Primary Stroke Center"] == 3
        assert ranks["Acute Stroke Ready Hospital"] == 4
        assert ranks["Stroke Rehabilitation"] == 5 and ranks["Stroke  Rehabilitation"] == 5
        assert ranks["Hospital"] is None and ranks["Primary Care Medical Home"] is None
        assert ranks["Comprehensive Cardiac Center"] is None
        assert len(rows) == 12

    def test_topProgramOnly_keeps_one_row_per_site(self, spark, tmp_path, monkeypatch):
        from utilities import prep_jcAccreditationDF
        seen = []
        def fake_geocode_addresses(addresses, pathToData, maxCalls=None, keyFilename=None):
            seen.extend(addresses)
            return {a: geocoding.parse_geocode_response(ok_payload()) for a in addresses}
        monkeypatch.setattr(geocoding, "geocode_addresses", fake_geocode_addresses)
        result = prep_jcAccreditationDF(self._df(spark), pathToData=str(tmp_path), topProgramOnly=True)
        rows = {r["jcHcoId"]: r for r in result.collect()}
        assert len(rows) == 7
        assert rows["1"]["jcProgram"] == "Advanced Comprehensive Stroke Center" and rows["1"]["jcEffectiveDate"] == "01/01/2021"
        assert rows["2"]["jcProgram"] == "Ambulatory Care" and rows["2"]["jcProgramRank"] is None
        assert rows["3"]["jcProgram"] == "Acute Stroke Ready Hospital"
        assert rows["4"]["jcProgram"] == "thrombectomy-capable stroke center"
        assert rows["5"]["jcProgramRank"] == 5 and rows["6"]["jcProgramRank"] is None
        assert rows["7"]["jcProgram"] == "Primary Stroke Center"
        assert len(seen) == 7 and all(rows[h]["jcLat"] == 39.96 for h in rows)

    def test_excludePrograms_drops_rows_before_collapse(self, spark):
        from utilities import prep_jcAccreditationDF
        result = prep_jcAccreditationDF(self._df(spark), topProgramOnly=True, excludePrograms=["Stroke Rehabilitation"])
        rows = {r["jcHcoId"]: r for r in result.collect()}
        assert set(rows) == {"1", "2", "3", "4", "6", "7"}
        assert rows["7"]["jcProgram"] == "Primary Stroke Center"
        allRows = prep_jcAccreditationDF(self._df(spark), excludePrograms=["stroke rehabilitation"]).collect()
        assert len(allRows) == 10 and not any("Rehabilitation" in r["jcProgram"] for r in allRows)


class TestGeodesicDistance:

    def test_columbus_to_cleveland(self, spark):
        import pyspark.sql.functions as F
        df = spark.createDataFrame([(39.9612, -82.9988, 41.4993, -81.6944)], "lat1 double, lng1 double, lat2 double, lng2 double")
        row = df.select(geocoding.get_geodesicDistanceKm(F.col("lat1"), F.col("lng1"), F.col("lat2"), F.col("lng2")).alias("d"),
                        geocoding.get_geodesicDistanceKm(F.col("lat2"), F.col("lng2"), F.col("lat1"), F.col("lng1")).alias("r"),
                        geocoding.get_geodesicDistanceKm(F.col("lat1"), F.col("lng1"), F.col("lat1"), F.col("lng1")).alias("z")).collect()[0]
        assert abs(row["d"] - 203) / 203 < 0.01
        assert row["d"] == row["r"]
        assert row["z"] == 0


class TestAddPosNearestInfo:

    POS_SCHEMA = "PRVDR_NUM string, FAC_NAME string, STATE_CD string, posHospital int, posActive int, posLat double, posLng double, posGeocodeLocationType string"
    JC_SCHEMA = "jcHcoId string, jcSiteName string, jcAddress string, jcState string, jcLat double, jcLng double"

    def _pos(self, spark):
        return spark.createDataFrame(
            [("360001", "NEAR ACTIVE", "OH", 1, 1, 40.0000, -83.0000, "ROOFTOP"),
             ("360002", "NEAR CLOSED", "OH", 1, 0, 40.0000, -83.0000, "ROOFTOP"),
             ("360003", "FAR", "OH", 1, 1, 40.0100, -83.0000, "APPROXIMATE"),
             ("360004", "SNF NOT HOSPITAL", "OH", 0, 1, 40.0001, -83.0000, "ROOFTOP"),
             ("390001", "PA NEXT DOOR", "PA", 1, 1, 40.0002, -83.0000, "ROOFTOP"),
             ("360005", "NO COORDS", "OH", 1, 1, None, None, None)],
            self.POS_SCHEMA)

    def _jc(self, spark):
        return spark.createDataFrame(
            [("1", "A", "ADDR A", "OH", 40.0000, -83.0000),
             ("1", "A", "ADDR A", "OH", 40.0000, -83.0000),
             ("2", "B", "ADDR B", "OH", 40.0090, -83.0000),
             ("3", "C", "ADDR C", "OH", None, None),
             ("4", "D", "ADDR D", "WV", 40.0000, -83.0000)],
            self.JC_SCHEMA)

    def test_nearest(self, spark):
        from utilities import add_pos_nearest_info
        result = add_pos_nearest_info(self._jc(spark), self._pos(spark))
        rows = result.collect()
        assert len(rows) == 5
        byId = {r["jcHcoId"]: r for r in rows}
        assert byId["1"]["posNearestCcn"] == "360001" and byId["1"]["posNearestDistanceKm"] == 0
        assert byId["1"]["posNearestActive"] == 1 and byId["1"]["posNearestGeocodeLocationType"] == "ROOFTOP"
        assert byId["1"]["posNearestSecondCcn"] == "360002" and byId["1"]["posNearestSecondDistanceKm"] == 0
        assert byId["1"]["posNearestSecondActive"] == 0
        assert byId["2"]["posNearestCcn"] == "360003" and abs(byId["2"]["posNearestDistanceKm"] - 0.111) < 0.01
        assert byId["2"]["posNearestSecondCcn"] == "360001" and abs(byId["2"]["posNearestSecondDistanceKm"] - 1.0) < 0.01
        assert byId["3"]["posNearestCcn"] is None and byId["3"]["posNearestDistanceKm"] is None
        assert byId["4"]["posNearestCcn"] is None and byId["4"]["posNearestSecondCcn"] is None
        added = set(result.columns) - set(self._jc(spark).columns)
        assert added == {"posNearestCcn", "posNearestFacName", "posNearestDistanceKm", "posNearestActive", "posNearestGeocodeLocationType",
                         "posNearestSecondCcn", "posNearestSecondFacName", "posNearestSecondDistanceKm", "posNearestSecondActive"}
        assert all(c[0].islower() for c in added)

    def test_prefix_and_coordinates(self, spark):
        from utilities import add_pos_nearest_info
        jc = self._jc(spark).withColumn("otherLat", F.lit(40.0100)).withColumn("otherLng", F.lit(-83.0))
        result = add_pos_nearest_info(jc, self._pos(spark), latCol="otherLat", lngCol="otherLng", prefix="posParentNearest")
        byId = {r["jcHcoId"]: r for r in result.collect()}
        assert byId["1"]["posParentNearestCcn"] == "360003" and byId["1"]["posParentNearestDistanceKm"] == 0
        assert "posNearestCcn" not in result.columns


class TestAddPosCcnInfo:

    POS_SCHEMA = "PRVDR_NUM string, FAC_NAME string, STATE_CD string, posHospital int, posActive int, posLat double, posLng double, posGeocodeLocationType string"
    JC_SCHEMA = ("jcHcoId string, jcSiteName string, jcAddress string, jcState string, jcLat double, jcLng double, "
                 "jcSiteLat double, jcSiteLng double, jcSiteLocationSource string, jcProgram string, jcProgramRank int")

    def _pos(self, spark):
        return spark.createDataFrame(
            [("360001", "PARENT", "OH", 1, 1, 40.0, -83.0, "ROOFTOP"),
             ("360002", "OWN CCN CAMPUS", "OH", 1, 1, 41.0, -83.0, "ROOFTOP"),
             ("360003", "TIE ACTIVE", "OH", 1, 1, 42.0, -83.0, "ROOFTOP"),
             ("360004", "TIE CLOSED", "OH", 1, 0, 42.0, -83.0, "ROOFTOP"),
             ("360005", "FAR", "OH", 1, 1, 43.0, -83.0, "ROOFTOP"),
             ("360006", "MARIA PARHAM MEDICAL CENTER", "OH", 1, 1, 46.0, -83.0, "APPROXIMATE"),
             ("360007", "SOME OTHER PLACE", "OH", 1, 1, 46.01, -83.0, "ROOFTOP"),
             ("360008", "CATHOLIC HEALTH SOUTHEAST HOSPITAL", "OH", 1, 1, 47.0, -83.0, "ROOFTOP")],
            self.POS_SCHEMA)

    def _jc(self, spark):
        parent = (40.0, -83.0)
        return spark.createDataFrame(
            [("1", "MAIN", "ADDR", "OH", *parent, *parent, "place", "Comprehensive Stroke Center", 1),
             ("1", "OWN CCN", "ADDR", "OH", *parent, 41.0, -83.0, "place", "Primary Stroke Center", 3),
             ("1", "PROVIDER BASED", "ADDR", "OH", *parent, 44.0, -83.0, "place", "Acute Stroke Ready Hospital", 4),
             ("2", "TIE", "ADDR T", "OH", 42.0, -83.0, 42.0, -83.0, "place", "Primary Stroke Center", 3),
             ("3", "LOST", "ADDR L", "OH", 44.0, -83.0, 44.0, -83.0, "address", "Primary Stroke Center", 3),
             ("4", "NEAR FAR", "ADDR F", "OH", 43.002, -83.0, 43.002, -83.0, "place", "Primary Stroke Center", 3),
             ("5", "FALLBACK NONE", "ADDR N", "OH", 44.0, -83.0, 45.0, -83.0, "place", "Primary Stroke Center", 3),
             ("6", "NAMED", "ADDR X", "OH", 46.02, -83.0, 46.02, -83.0, "place", "Primary Stroke Center", 3),
             ("7", "OTHER NAME", "ADDR Y", "OH", 46.02, -83.0, 46.02, -83.0, "place", "Primary Stroke Center", 3),
             ("8", "HALF NAME", "ADDR Z", "OH", 47.02, -83.0, 47.02, -83.0, "place", "Primary Stroke Center", 3)],
            self.JC_SCHEMA).withColumn("jcSearchName", F.when(F.col("jcSiteName") == "NAMED", "Maria Parham Health")
                                                        .when(F.col("jcSiteName") == "OTHER NAME", "Somewhere Else Hospital")
                                                        .when(F.col("jcSiteName") == "HALF NAME", "Mercy Catholic Fitzgerald Hospital")
                                                        .otherwise(F.col("jcSiteName")))

    def test_rule(self, spark):
        from utilities import add_pos_ccn_info
        result = add_pos_ccn_info(self._jc(spark), self._pos(spark))
        rows = {r["jcSiteName"]: r for r in result.collect()}
        assert len(rows) == 10
        assert rows["MAIN"]["posMatchMethod"] == "nearestSite" and rows["MAIN"]["posCcn"] == "360001" and rows["MAIN"]["posCcnPass"] == "site"
        assert rows["OWN CCN"]["posMatchMethod"] == "nearestSite" and rows["OWN CCN"]["posCcn"] == "360002"
        assert rows["PROVIDER BASED"]["posMatchMethod"] == "nearestParent" and rows["PROVIDER BASED"]["posCcn"] == "360001"
        assert rows["PROVIDER BASED"]["posCcnPass"] == "parent" and rows["PROVIDER BASED"]["posCcnDistanceKm"] == 0
        assert rows["TIE"]["posCcn"] == "360003" and rows["TIE"]["posMatchAmbiguous"] == 1 and rows["MAIN"]["posMatchAmbiguous"] == 0
        assert rows["LOST"]["posMatchMethod"] == "none" and rows["LOST"]["posCcn"] is None and rows["LOST"]["posMatchAmbiguous"] is None
        assert rows["NEAR FAR"]["posCcn"] == "360005" and 0.2 < rows["NEAR FAR"]["posCcnDistanceKm"] < 0.25
        assert rows["FALLBACK NONE"]["posMatchMethod"] == "none"
        assert rows["NAMED"]["posMatchMethod"] == "nearestNamed" and rows["NAMED"]["posCcn"] == "360006" and rows["NAMED"]["posCcnPass"] == "named"
        assert rows["NAMED"]["posCcnNameScore"] == 1.0 and 2.1 < rows["NAMED"]["posCcnDistanceKm"] < 2.3 and rows["NAMED"]["posMatchAmbiguous"] == 0
        assert rows["OTHER NAME"]["posMatchMethod"] == "none" and rows["OTHER NAME"]["posCcnNameScore"] is None
        assert rows["HALF NAME"]["posMatchMethod"] == "none" and rows["MAIN"]["posCcnNameScore"] is None
        assert rows["MAIN"]["posParentNearestCcn"] == "360001" and rows["OWN CCN"]["posParentNearestCcn"] == "360001"
        added = set(result.columns) - set(self._jc(spark).columns)
        assert {"posCcn", "posCcnFacName", "posCcnDistanceKm", "posCcnActive", "posCcnPass", "posMatchMethod", "posMatchAmbiguous"} <= added
        assert all(c[0].islower() for c in added)

    def test_without_site_columns_uses_address(self, spark):
        from utilities import add_pos_ccn_info
        jc = self._jc(spark).drop("jcSiteLat", "jcSiteLng", "jcSiteLocationSource")
        rows = {r["jcSiteName"]: r for r in add_pos_ccn_info(jc, self._pos(spark)).collect()}
        assert rows["OWN CCN"]["posCcn"] == "360001" and rows["PROVIDER BASED"]["posMatchMethod"] == "nearestParent"
        assert rows["MAIN"]["posMatchMethod"] == "nearestParent" and rows["MAIN"]["posCcnPass"] == "parent"

    def test_overrides(self, spark):
        from utilities import add_pos_ccn_info
        overrides = spark.createDataFrame([("1", "MAIN", "360005"), ("2", "TIE", ""), ("9", "ABSENT", "360001")],
                                          "jcHcoId string, jcSiteName string, keepCcn string")
        result = add_pos_ccn_info(self._jc(spark), self._pos(spark), overridesDF=overrides)
        rows = {r["jcSiteName"]: r for r in result.collect()}
        assert len(rows) == 10
        assert rows["MAIN"]["posMatchMethod"] == "override" and rows["MAIN"]["posCcn"] == "360005" and rows["MAIN"]["posCcnFacName"] == "FAR"
        assert rows["MAIN"]["posCcnDistanceKm"] is None and rows["MAIN"]["posCcnPass"] is None
        assert rows["TIE"]["posMatchMethod"] == "none" and rows["TIE"]["posCcn"] is None
        assert rows["OWN CCN"]["posMatchMethod"] == "nearestSite"

    def test_name_score_threshold(self, spark):
        from utilities import add_pos_ccn_info
        rows = {r["jcSiteName"]: r for r in add_pos_ccn_info(self._jc(spark), self._pos(spark), nameScoreMin=0.5).collect()}
        assert rows["HALF NAME"]["posMatchMethod"] == "nearestNamed" and rows["HALF NAME"]["posCcn"] == "360008"
        assert rows["HALF NAME"]["posCcnNameScore"] == 0.5 and rows["OTHER NAME"]["posMatchMethod"] == "none"

    def test_get_ccn_jc_info(self, spark):
        from utilities import add_pos_ccn_info, get_ccn_jc_info
        result = get_ccn_jc_info(add_pos_ccn_info(self._jc(spark), self._pos(spark)))
        rows = {r["posCcn"]: r for r in result.collect()}
        assert set(rows) == {"360001", "360002", "360003", "360005", "360006"}
        assert rows["360001"]["jcSites"] == 2 and rows["360001"]["jcBestProgramRank"] == 1
        assert rows["360001"]["jcBestProgram"] == "Comprehensive Stroke Center" and sorted(rows["360001"]["jcSiteNames"]) == ["MAIN", "PROVIDER BASED"]
        assert rows["360001"]["jcBestProgramSite"] == "MAIN" and rows["360001"]["jcBestProgramMatchMethod"] == "nearestSite"
        assert rows["360001"]["jcMatchMethods"] == ["nearestParent", "nearestSite"]
        assert rows["360006"]["jcBestProgramMatchMethod"] == "nearestNamed"
        assert {k: r["jcCertificationConfidence"] for k, r in rows.items()} == {"360001": 3, "360002": 4, "360003": 4, "360005": 4, "360006": 1}


class TestAddPlaceInfo:

    def test_columns_from_cache(self, spark, tmp_path, monkeypatch):
        seen = dict()
        def fake_find_places(queries, pathToData, maxCalls=None, keyFilename=None):
            seen.update(queries)
            return {q: geocoding.parse_place_response(place_payload() if q == "A, NY" else {}) for q in queries}
        monkeypatch.setattr(geocoding, "find_places", fake_find_places)
        df = spark.createDataFrame([("1", "A, NY", 40.7, -74.0), ("2", "A, NY", 40.8, -74.1), ("3", "B, NY", None, None)],
                                   "id string, q string, lat double, lng double")
        result = geocoding.add_place_info(df, "q", str(tmp_path), "jc", biasLatCol="lat", biasLngCol="lng", maxCalls=0)
        rows = {r["id"]: r for r in result.collect()}
        assert seen == {"A, NY": (40.7, -74.0), "B, NY": None}
        assert rows["1"]["jcPlaceLat"] == 40.72 and rows["2"]["jcPlaceName"] == "NYP Allen Hospital" and rows["1"]["jcPlaceTypes"] == "hospital,health"
        assert rows["3"]["jcPlaceStatus"] == "ZERO_RESULTS" and rows["3"]["jcPlaceLat"] is None
        assert set(result.columns) - set(df.columns) == {"jcPlaceLat", "jcPlaceLng", "jcPlaceName", "jcPlaceAddress", "jcPlaceTypes", "jcPlaceStatus"}


class TestPrepJcAccreditationPlaces:

    def test_site_location(self, spark, tmp_path, monkeypatch):
        from utilities import prep_jcAccreditationDF
        cols = TestPrepJcAccreditationDF.JC_SCHEMA
        df = spark.createDataFrame(
            [("1", "Org", None, "New York", "New York", "525 E 68th St", "10065", "NYP", "Allen Hospital", "Primary Stroke Center", "2020-01-01", "Certification"),
             ("2", "Org", None, "New York", "New York", "525 E 68th St", "10065", "Clinic Site", None, "Primary Stroke Center", "2020-01-01", "Certification"),
             ("3", "Org", None, "New York", "New York", "525 E 68th St", "10065", "Unknown Site", None, "Primary Stroke Center", "2020-01-01", "Certification"),
             ("4", "Org", None, "New York", "New York", "525 E 68th St", "10065", "Campus Site", None, "Primary Stroke Center", "2020-01-01", "Certification")],
            cols)
        monkeypatch.setattr(geocoding, "geocode_addresses",
                            lambda addresses, pathToData, maxCalls=None, keyFilename=None: {a: geocoding.parse_geocode_response(ok_payload(lat=40.7647, lng=-73.9540)) for a in addresses})
        def fake_find_places(queries, pathToData, maxCalls=None, keyFilename=None):
            out = dict()
            for q in queries:
                if q.startswith("Allen Hospital"):
                    out[q] = geocoding.parse_place_response(place_payload(lat=40.8700, lng=-73.9200))
                elif q.startswith("Clinic Site"):
                    out[q] = geocoding.parse_place_response(place_payload(lat=40.9, lng=-73.9, types=("medical_clinic",)))
                elif q.startswith("Campus Site"):
                    out[q] = geocoding.parse_place_response(place_payload(lat=40.95, lng=-73.9, types=("university", "point_of_interest")))
                else:
                    out[q] = geocoding.parse_place_response({})
            return out
        monkeypatch.setattr(geocoding, "find_places", fake_find_places)
        result = prep_jcAccreditationDF(df, pathToData=str(tmp_path), placesLookup=True)
        rows = {r["jcHcoId"]: r for r in result.collect()}
        assert rows["1"]["jcPlaceQuery"] == "Allen Hospital, NY" and rows["1"]["jcPlaceIsHospital"] == 1
        assert rows["1"]["jcSiteLocationSource"] == "place" and rows["1"]["jcSiteLat"] == 40.87 and 11 < rows["1"]["jcPlaceDistanceKm"] < 13
        assert rows["2"]["jcPlaceIsHospital"] == 0 and rows["2"]["jcSiteLocationSource"] == "place" and rows["2"]["jcSiteLat"] == 40.9
        assert rows["4"]["jcPlaceIsHospital"] == 0 and rows["4"]["jcSiteLocationSource"] == "address" and rows["4"]["jcSiteLat"] == 40.7647
        assert rows["3"]["jcPlaceStatus"] == "ZERO_RESULTS" and rows["3"]["jcPlaceIsHospital"] is None
        assert rows["3"]["jcSiteLocationSource"] == "address" and rows["3"]["jcPlaceDistanceKm"] is None
        assert rows["1"]["jcLat"] == 40.7647


class TestAddPosHospitalType:

    def test_ranges(self, spark):
        from utilities import add_posHospitalType
        cases = [("360001", 1, "acute"), ("360879", 1, "acute"), ("361313", 1, "cah"), ("362032", 1, "ltch"),
                 ("363031", 1, "rehabilitation"), ("363309", 1, "childrens"), ("364003", 1, "psychiatric"),
                 ("399808", 1, "transplant"), ("29002E", 1, "emergency"), ("34012F", 1, "federal"),
                 ("360900", 1, "other"), ("365001", 0, None)]
        df = spark.createDataFrame([(c, h) for c, h, _ in cases], "PRVDR_NUM string, posHospital int")
        got = {r["PRVDR_NUM"]: r["posHospitalType"] for r in add_posHospitalType(df).collect()}
        assert got == {c: t for c, _, t in cases}


def place_payload(lat=40.72, lng=-73.92, name="NYP Allen Hospital", types=("hospital", "health")):
    return {"places": [{"id": "ChIJx", "displayName": {"text": name, "languageCode": "en"},
                        "formattedAddress": "5141 Broadway, New York, NY 10034, USA",
                        "location": {"latitude": lat, "longitude": lng}, "types": list(types),
                        "businessStatus": "OPERATIONAL"}]}


class TestParsePlaceResponse:

    def test_found(self):
        record = geocoding.parse_place_response(place_payload())
        assert record["status"] == "OK" and record["lat"] == 40.72 and record["lng"] == -73.92
        assert record["name"] == "NYP Allen Hospital" and record["types"] == "hospital,health"
        assert record["placeId"] == "ChIJx" and record["businessStatus"] == "OPERATIONAL"
        assert set(record) == set(geocoding.placeFields)

    def test_empty(self):
        record = geocoding.parse_place_response({})
        assert record["status"] == "ZERO_RESULTS" and record["lat"] is None


class TestFindPlace:

    def test_request_shape(self, no_sleep):
        opener = fake_urlopen([place_payload()])
        with mock.patch.object(geocoding, "urlopen", opener):
            record = geocoding.find_place("NYP Allen Hospital, NY", "secret-key", biasLat=40.7, biasLng=-74.0)
        request = opener.call_args[0][0]
        body = json.loads(request.data)
        assert request.get_method() == "POST" and request.full_url == geocoding.googlePlacesUrl
        assert body["textQuery"] == "NYP Allen Hospital, NY" and body["regionCode"] == "US"
        assert body["locationBias"]["circle"]["center"] == {"latitude": 40.7, "longitude": -74.0}
        assert body["locationBias"]["circle"]["radius"] == 50000
        assert request.get_header("X-goog-api-key") == "secret-key"
        assert request.get_header("X-goog-fieldmask") == geocoding.googlePlacesFieldMask
        assert record["status"] == "OK"

    def test_no_bias(self, no_sleep):
        opener = fake_urlopen([place_payload()])
        with mock.patch.object(geocoding, "urlopen", opener):
            geocoding.find_place("X, NY", "secret-key")
        assert "locationBias" not in json.loads(opener.call_args[0][0].data)

    def test_retry_on_503_then_ok(self, no_sleep):
        from urllib.error import HTTPError
        calls = []
        def opener(request, timeout=None):
            calls.append(request)
            if len(calls) == 1:
                raise HTTPError(request.full_url, 503, "unavailable", {}, io.BytesIO(b"busy"))
            response = mock.MagicMock()
            response.__enter__.return_value = io.StringIO(json.dumps(place_payload()))
            return response
        with mock.patch.object(geocoding, "urlopen", opener):
            record = geocoding.find_place("X, NY", "secret-key")
        assert len(calls) == 2 and record["status"] == "OK"

    def test_403_raises_without_key(self, no_sleep):
        from urllib.error import HTTPError
        def opener(request, timeout=None):
            raise HTTPError(request.full_url, 403, "forbidden", {}, io.BytesIO(b"API not enabled"))
        with mock.patch.object(geocoding, "urlopen", opener):
            with pytest.raises(RuntimeError) as e:
                geocoding.find_place("X, NY", "secret-key")
        assert "403" in str(e.value) and "API not enabled" in str(e.value) and "secret-key" not in str(e.value)


class TestFindPlaces:

    def test_all_cached_makes_no_calls(self, tmp_path):
        record = geocoding.parse_place_response(place_payload())
        geocoding.write_places_cache({"A, NY": record}, str(tmp_path))
        opener = fake_urlopen([])
        with mock.patch.object(geocoding, "urlopen", opener):
            records = geocoding.find_places({"A, NY": (40.7, -74.0)}, str(tmp_path))
        assert records == {"A, NY": record} and opener.call_count == 0

    def test_miss_called_once_and_cached(self, tmp_path):
        write_key(tmp_path)
        opener = fake_urlopen([place_payload()])
        with mock.patch.object(geocoding, "urlopen", opener):
            records = geocoding.find_places({"A, NY": None}, str(tmp_path))
            geocoding.find_places({"A, NY": None}, str(tmp_path))
        assert opener.call_count == 1 and records["A, NY"]["name"] == "NYP Allen Hospital"
        cacheText = (tmp_path / "GEOCODING" / "googlePlacesCache.json").read_text()
        assert "A, NY" in json.loads(cacheText) and "secret-key" not in cacheText

    def test_zero_results_cached(self, tmp_path):
        write_key(tmp_path)
        opener = fake_urlopen([{}])
        with mock.patch.object(geocoding, "urlopen", opener):
            geocoding.find_places({"A, NY": None}, str(tmp_path))
            geocoding.find_places({"A, NY": None}, str(tmp_path))
        assert opener.call_count == 1

    def test_max_calls_guard(self, tmp_path):
        write_key(tmp_path)
        opener = fake_urlopen([])
        with mock.patch.object(geocoding, "urlopen", opener):
            with pytest.raises(RuntimeError, match="maxCalls"):
                geocoding.find_places({"A, NY": None, "B, NY": None}, str(tmp_path), maxCalls=1)
        assert opener.call_count == 0


class TestNameScore:

    def test_scores(self, spark):
        from utilities import get_nameScore, get_nameTokens
        df = spark.createDataFrame(
            [("Maria Parham Health", "MARIA PARHAM MEDICAL CENTER", 1.0),
             ("St. Mary Medical Center", "ST MARY MEDICAL CENTER", 1.0),
             ("Hospital", "GENERAL ACUTE CARE HOSPITAL", 0.0),
             ("Mercy Catholic Fitzgerald Hospital", "MERCY FITZGERALD HOSPITAL", 1.0),
             ("Mercy Catholic Fitzgerald Hospital", "CATHOLIC HEALTH SOUTHEAST HOSPITAL", 0.5),
             ("Sanford Medical Center Fargo", "ESSENTIA HEALTH FARGO", 0.5),
             ("Corpus Christi Medical Center - Doctors", "CHRISTUS SPOHN HOSPITAL CORPUS CHRISTI", 0.6666666666666666),
             (None, "ANY", 0.0)],
            "a string, b string, expected double")
        rows = df.select("a", get_nameScore(F.col("a"), F.col("b")).alias("s"), "expected", get_nameTokens(F.col("a")).alias("t")).collect()
        for r in rows:
            assert abs(r["s"] - r["expected"]) < 1e-9, (r["a"], r["s"])
        assert [r["t"] for r in rows if r["a"] == "St. Mary Medical Center"][0] == ["mary"]
