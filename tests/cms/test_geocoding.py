import io
import json
import pytest
from unittest import mock

import geocoding

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

    def test_interrupted_run_keeps_records(self, tmp_path, no_sleep):
        write_key(tmp_path)
        opener = fake_urlopen([ok_payload(), {"status": "REQUEST_DENIED"}])
        with mock.patch.object(geocoding, "urlopen", opener):
            with pytest.raises(RuntimeError):
                geocoding.geocode_addresses(["A", "B"], str(tmp_path), flushEvery=100)
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
        monkeypatch.setattr(geocoding, "geocode_addresses", lambda addresses, pathToData, maxCalls=None: records)
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
                "CBSA_URBN_RRL_IND", "ST_ADR", "CITY_NAME", "STATE_CD", "ZIP_CD"]

    def _df(self, spark):
        return spark.createDataFrame(
            [("360001", "OHIO HOSPITAL", "01", "01", "39", "049", "U", "123 MAIN ST", "COLUMBUS", "OH", "43215"),
             ("365001", "OHIO SNF", "04", "01", "39", "049", "U", "456 ELM ST", "COLUMBUS", "OH", "43215")],
            self.POS_COLS)

    def test_only_hospitals_geocoded_and_parquet_written(self, spark, tmp_path, monkeypatch):
        from utilities import prep_posDF
        seen = []
        def fake_geocode_addresses(addresses, pathToData, maxCalls=None):
            seen.extend(addresses)
            return {a: geocoding.parse_geocode_response(ok_payload()) for a in addresses}
        monkeypatch.setattr(geocoding, "geocode_addresses", fake_geocode_addresses)
        filename = str(tmp_path / "pos.parquet")
        result = prep_posDF(self._df(spark), pathToData=str(tmp_path), filename=filename)
        assert seen == ["123 MAIN ST, COLUMBUS, OH 43215"]
        rows = {r["PRVDR_NUM"]: r for r in result.collect()}
        assert len(rows) == 2
        assert rows["360001"]["posLat"] == 39.96 and rows["360001"]["posAddress"] == "123 MAIN ST, COLUMBUS, OH 43215"
        assert rows["365001"]["posLat"] is None and rows["365001"]["posAddress"] == "456 ELM ST, COLUMBUS, OH 43215"
        assert rows["365001"]["posZip"] == "43215"
        assert spark.read.parquet(filename).count() == 2

    def test_no_pathToData_skips_geocoding(self, spark, monkeypatch):
        from utilities import prep_posDF
        monkeypatch.setattr(geocoding, "geocode_addresses", lambda *a, **k: pytest.fail("should not geocode"))
        result = prep_posDF(self._df(spark))
        assert "posAddress" in result.columns and "posLat" not in result.columns
