import json
import os
import time
from datetime import date
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

'''Coordinates for street addresses from the Google Geocoding API
(https://developers.google.com/maps/documentation/geocoding), with every response cached on disk so that
an address is paid for once. The cache and the API key live under pathToData/GEOCODING, outside the repo:
the key must never be committed and the cache is data, not code.'''

googleGeocodeUrl = "https://maps.googleapis.com/maps/api/geocode/json"
retryStatuses = ["OVER_QUERY_LIMIT", "OVER_DAILY_LIMIT", "UNKNOWN_ERROR"]
geocodeFields = ["lat", "lng", "locationType", "formattedAddress", "partialMatch", "status", "geocodedOn"]

class GeocodeRetryError(Exception):
    '''The API asked us to try again later (quota or a transient server side error).'''

def get_geocode_cache_filename(pathToData):
    return f"{pathToData}/GEOCODING/googleGeocodeCache.json"

def get_api_key_filename(pathToData):
    return f"{pathToData}/GEOCODING/key.csv"

def get_api_key(pathToData):
    '''The key is a single line in a file outside the repo (chmod 600) rather than an argument or an
    environment variable, so that it never ends up in a notebook cell, a shell history or a commit.'''
    filename = get_api_key_filename(pathToData)
    if not os.path.isfile(filename):
        raise FileNotFoundError(f"put the Google Geocoding API key, one line, in {filename}")
    with open(filename) as f:
        apiKey = f.readline().strip()
    if apiKey == "":
        raise ValueError(f"{filename} is empty")
    return apiKey

def get_geocode_cache(pathToData):
    filename = get_geocode_cache_filename(pathToData)
    if not os.path.isfile(filename):
        return dict()
    with open(filename) as f:
        return json.load(f)

def write_geocode_cache(cache, pathToData):
    '''Writes to a temporary file and renames it, so that a kernel killed mid-write leaves the previous
    cache intact rather than a truncated json.'''
    filename = get_geocode_cache_filename(pathToData)
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename + ".tmp", "w") as f:
        json.dump(cache, f, indent=1, sort_keys=True)
    os.replace(filename + ".tmp", filename)

def get_geocode_cache_misses(addresses, pathToData):
    '''The addresses that are not in the cache, ie the paid calls a geocode_addresses run would make.'''
    cache = get_geocode_cache(pathToData)
    return sorted(set(a for a in addresses if a is not None and a not in cache))

def parse_geocode_response(payload):
    '''Turns the API json into a cache record. ZERO_RESULTS is a record too (null coordinates) so that
    an address the API cannot resolve is not asked again on every run. location_type says how precise the
    point is (ROOFTOP, RANGE_INTERPOLATED, GEOMETRIC_CENTER, APPROXIMATE) and partial_match that the API
    could not match the whole address and settled for something similar; both are kept so that the
    quality of a coordinate can be judged downstream.'''
    status = payload.get("status")
    if status in retryStatuses:
        raise GeocodeRetryError(f"{status}: {payload.get('error_message', '')}")
    if status == "REQUEST_DENIED":
        raise RuntimeError(f"REQUEST_DENIED: {payload.get('error_message', '')}")
    if status == "INVALID_REQUEST":
        raise ValueError(f"INVALID_REQUEST: {payload.get('error_message', '')}")
    if status not in ["OK", "ZERO_RESULTS"]:
        raise RuntimeError(f"unexpected geocoding status {status}")
    record = dict(lat=None, lng=None, locationType=None, formattedAddress=None, partialMatch=None,
                  status=status, geocodedOn=date.today().isoformat())
    if status == "OK":
        result = payload["results"][0]
        record["lat"] = result["geometry"]["location"]["lat"]
        record["lng"] = result["geometry"]["location"]["lng"]
        record["locationType"] = result["geometry"].get("location_type")
        record["formattedAddress"] = result.get("formatted_address")
        record["partialMatch"] = int(result.get("partial_match", False))
    return record

def geocode_address(address, apiKey, maxRetries=5):
    '''One API call with exponential backoff on quota and transient errors. Exceptions never carry the
    request url because it contains the key.'''
    url = googleGeocodeUrl + "?" + urlencode(dict(address=address, components="country:US", key=apiKey))
    for attempt in range(maxRetries + 1):
        try:
            with urlopen(url, timeout=30) as response:
                return parse_geocode_response(json.load(response))
        except GeocodeRetryError as e:
            lastError = str(e)
        except HTTPError as e:
            if e.code < 500:
                raise RuntimeError(f"geocoding {address!r} failed with http {e.code}") from None
            lastError = f"http {e.code}"
        except URLError as e:
            lastError = str(e.reason)
        if attempt < maxRetries:
            time.sleep(2 ** attempt)
    raise RuntimeError(f"geocoding {address!r} failed after {maxRetries} retries: {lastError}")

def geocode_addresses(addresses, pathToData, flushEvery=100, maxCalls=None):
    '''Returns {address: record} for every address, calling the API only for the ones not in the cache.
    The cache is flushed every flushEvery new records and when the loop ends for any reason, so an
    interrupted run keeps what it paid for and the next run resumes where it stopped. maxCalls is a cost
    guard: when more addresses are missing than that, nothing is called and the count is reported instead.
    When nothing is missing the key file is not even read.'''
    cache = get_geocode_cache(pathToData)
    misses = get_geocode_cache_misses(addresses, pathToData)
    if len(misses) == 0:
        return {a: cache[a] for a in addresses if a is not None}
    if maxCalls is not None and len(misses) > maxCalls:
        raise RuntimeError(f"{len(misses)} addresses are not cached, more than maxCalls={maxCalls}")
    apiKey = get_api_key(pathToData)
    newRecords = 0
    try:
        for address in misses:
            cache[address] = geocode_address(address, apiKey)
            newRecords += 1
            if newRecords % flushEvery == 0:
                write_geocode_cache(cache, pathToData)
    finally:
        if newRecords > 0:
            write_geocode_cache(cache, pathToData)
    return {a: cache[a] for a in addresses if a is not None}

def add_address(DF, streetCol, cityCol, stateCol, zipCol, addressCol):
    '''One line address "STREET, CITY, ST 12345" from the parts, trimmed, upper cased and with runs of
    white space collapsed, so that the same place written slightly differently in two sources yields the
    same string and therefore the same cache entry. A null street falls back to "CITY, ST 12345", which
    the API resolves to an APPROXIMATE point.'''
    def clean(col):
        return F.regexp_replace(F.upper(F.trim(F.col(col))), r"\s{2,}", " ")
    zip5 = F.substring(F.trim(F.col(zipCol)), 1, 5)
    DF = DF.withColumn(addressCol,
                       F.concat_ws(", ", clean(streetCol), clean(cityCol), F.concat_ws(" ", clean(stateCol), zip5)))
    return DF

def get_geocode_schema(prefix):
    return StructType([StructField("address", StringType()),
                       StructField(f"{prefix}Lat", DoubleType()),
                       StructField(f"{prefix}Lng", DoubleType()),
                       StructField(f"{prefix}GeocodeLocationType", StringType()),
                       StructField(f"{prefix}GeocodeFormattedAddress", StringType()),
                       StructField(f"{prefix}GeocodePartialMatch", IntegerType()),
                       StructField(f"{prefix}GeocodeStatus", StringType())])

def add_geocode_info(DF, addressCol, pathToData, prefix, maxCalls=None):
    '''Adds {prefix}Lat, {prefix}Lng, {prefix}GeocodeLocationType, {prefix}GeocodeFormattedAddress,
    {prefix}GeocodePartialMatch and {prefix}GeocodeStatus for the address in addressCol. The distinct
    addresses are collected to the driver and geocoded there (see geocode_addresses), so filter DF to the
    rows that need coordinates before calling this: every distinct address that is not cached is a paid
    call. The resulting small table is broadcast joined back.'''
    addresses = [row[0] for row in DF.select(addressCol).distinct().collect()]
    records = geocode_addresses(addresses, pathToData, maxCalls=maxCalls)
    rows = [(a, r["lat"], r["lng"], r["locationType"], r["formattedAddress"], r["partialMatch"], r["status"])
            for a, r in records.items()]
    geocodeDF = DF.sparkSession.createDataFrame(rows, schema=get_geocode_schema(prefix))
    DF = DF.join(F.broadcast(geocodeDF), on=[F.col(addressCol) == F.col("address")], how="left_outer").drop("address")
    return DF
