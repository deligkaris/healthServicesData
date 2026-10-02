import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

'''Coordinates for street addresses from the Google Geocoding API
(https://developers.google.com/maps/documentation/geocoding) and for place names from the Places API (New) text search,
with every response cached on disk so that an address or a name is paid for once. The caches and the API key live
under pathToData/GEOCODING, outside the repo: the key must never be committed and the caches are data, not code.'''

googleGeocodeUrl = "https://maps.googleapis.com/maps/api/geocode/json"
googlePlacesUrl = "https://places.googleapis.com/v1/places:searchText"
googlePlacesFieldMask = "places.id,places.displayName,places.formattedAddress,places.location,places.types,places.businessStatus"
retryStatuses = ["OVER_QUERY_LIMIT", "OVER_DAILY_LIMIT", "UNKNOWN_ERROR"]
geocodeWorkers = 4
geocodeFields = ["lat", "lng", "locationType", "formattedAddress", "partialMatch", "status", "geocodedOn"]
placeFields = ["lat", "lng", "name", "address", "types", "placeId", "businessStatus", "status", "searchedOn"]

class GeocodeRetryError(Exception):
    '''The API asked us to try again later (quota or a transient server side error).'''

def get_geocode_cache_filename(pathToData):
    return f"{pathToData}/GEOCODING/googleGeocodeCache.json"

def get_api_key_filename(pathToData):
    return f"{pathToData}/GEOCODING/key.csv"

def get_api_key(pathToData, keyFilename=None):
    '''The key is a single line in a file outside the repo (chmod 600) rather than an argument or an
    environment variable, so that it never ends up in a notebook cell, a shell history or a commit. The file is
    pathToData/GEOCODING/key.csv unless keyFilename says otherwise.'''
    filename = keyFilename if keyFilename is not None else get_api_key_filename(pathToData)
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

def geocode_addresses(addresses, pathToData, flushEvery=500, maxCalls=None, keyFilename=None, workers=None):
    '''Returns {address: record} for every address, calling the API only for the ones not in the cache.
    The calls run on a small thread pool (workers, default geocodeWorkers): at 4 threads and ~0.2 s per call
    that is ~20 requests/s, well under the API's 50 requests/s limit, and ~10 min for 12k addresses instead
    of ~1 h. The cache is flushed every flushEvery new records and when the loop ends for any reason, so an
    interrupted run keeps what it paid for and the next run resumes where it stopped; on an error the calls
    not started yet are cancelled. maxCalls is a cost guard: when more addresses are missing than that,
    nothing is called and the count is reported instead. When nothing is missing the key file (see
    get_api_key) is not even read.'''
    cache = get_geocode_cache(pathToData)
    misses = get_geocode_cache_misses(addresses, pathToData)
    if len(misses) == 0:
        return {a: cache[a] for a in addresses if a is not None}
    if maxCalls is not None and len(misses) > maxCalls:
        raise RuntimeError(f"{len(misses)} addresses are not cached, more than maxCalls={maxCalls}")
    apiKey = get_api_key(pathToData, keyFilename)
    newRecords = 0
    pool = ThreadPoolExecutor(max_workers=workers if workers is not None else geocodeWorkers)
    futures = {pool.submit(geocode_address, address, apiKey): address for address in misses}
    try:
        for future in as_completed(futures):
            cache[futures[future]] = future.result()
            newRecords += 1
            if newRecords % flushEvery == 0:
                write_geocode_cache(cache, pathToData)
                print(f"geocoded {newRecords}/{len(misses)}")
    finally:
        pool.shutdown(wait=True, cancel_futures=True)
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

def add_geocode_info(DF, addressCol, pathToData, prefix, maxCalls=None, keyFilename=None):
    '''Adds {prefix}Lat, {prefix}Lng, {prefix}GeocodeLocationType, {prefix}GeocodeFormattedAddress,
    {prefix}GeocodePartialMatch and {prefix}GeocodeStatus for the address in addressCol. The distinct
    addresses are collected to the driver and geocoded there (see geocode_addresses), so filter DF to the
    rows that need coordinates before calling this: every distinct address that is not cached is a paid
    call. The resulting small table is broadcast joined back.'''
    addresses = [row[0] for row in DF.select(addressCol).distinct().collect()]
    records = geocode_addresses(addresses, pathToData, maxCalls=maxCalls, keyFilename=keyFilename)
    rows = [(a, r["lat"], r["lng"], r["locationType"], r["formattedAddress"], r["partialMatch"], r["status"])
            for a, r in records.items()]
    geocodeDF = DF.sparkSession.createDataFrame(rows, schema=get_geocode_schema(prefix))
    DF = DF.join(F.broadcast(geocodeDF), on=[F.col(addressCol) == F.col("address")], how="left_outer").drop("address")
    return DF

def get_geodesicDistanceKm(lat1, lng1, lat2, lng2):
    '''Great circle distance in km between two points given as column expressions of degrees (haversine on a
    sphere of mean radius 6371.0088 km, accurate to ~0.3% which is far below geocoding error). Also what
    dyadGeodesicDistanceKm between two hospitals is meant to be computed with.'''
    dLat = F.radians(lat2) - F.radians(lat1)
    dLng = F.radians(lng2) - F.radians(lng1)
    a = F.sin(dLat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dLng / 2) ** 2
    return 2 * 6371.0088 * F.asin(F.sqrt(a))

def get_places_cache_filename(pathToData):
    return f"{pathToData}/GEOCODING/googlePlacesCache.json"

def get_places_cache(pathToData):
    filename = get_places_cache_filename(pathToData)
    if not os.path.isfile(filename):
        return dict()
    with open(filename) as f:
        return json.load(f)

def write_places_cache(cache, pathToData):
    filename = get_places_cache_filename(pathToData)
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename + ".tmp", "w") as f:
        json.dump(cache, f, indent=1, sort_keys=True)
    os.replace(filename + ".tmp", filename)

def get_places_cache_misses(queries, pathToData):
    '''The queries that are not in the places cache, ie the paid calls a find_places run would make.'''
    cache = get_places_cache(pathToData)
    return sorted(set(q for q in queries if q is not None and q not in cache))

def parse_place_response(payload):
    '''Turns the Places API (New) text search json into a cache record holding the first result: its coordinates,
    display name, formatted address, types (a hospital should carry "hospital") and business status. No result is a
    ZERO_RESULTS record so that a name the API cannot resolve is not asked again on every run.'''
    record = dict(lat=None, lng=None, name=None, address=None, types=None, placeId=None, businessStatus=None,
                  status="ZERO_RESULTS", searchedOn=date.today().isoformat())
    places = payload.get("places", [])
    if len(places) > 0:
        place = places[0]
        record["lat"] = place["location"]["latitude"]
        record["lng"] = place["location"]["longitude"]
        record["name"] = place.get("displayName", {}).get("text")
        record["address"] = place.get("formattedAddress")
        record["types"] = ",".join(place.get("types", []))
        record["placeId"] = place.get("id")
        record["businessStatus"] = place.get("businessStatus")
        record["status"] = "OK"
    return record

def find_place(query, apiKey, biasLat=None, biasLng=None, biasRadiusM=50000, maxRetries=5):
    '''One Places API (New) text search (https://developers.google.com/maps/documentation/places/web-service/text-search)
    for a name such as "North Baldwin Infirmary, AL", returning the first place. With biasLat/biasLng the search prefers
    results within biasRadiusM of that point (the parent organization's address; the API allows at most 50 km) without excluding others, which keeps
    a generic name like "Memorial Hospital" near where the site is expected. The field mask limits the response to what
    the cache record holds (and what is billed). Exponential backoff on quota and transient errors; 400 and 403 (bad
    request, API not enabled or key not allowed) raise with the API's message; exceptions never carry the key.'''
    body = dict(textQuery=query, regionCode="US")
    if biasLat is not None and biasLng is not None:
        body["locationBias"] = dict(circle=dict(center=dict(latitude=biasLat, longitude=biasLng), radius=biasRadiusM))
    headers = {"Content-Type": "application/json", "X-Goog-Api-Key": apiKey, "X-Goog-FieldMask": googlePlacesFieldMask}
    for attempt in range(maxRetries + 1):
        request = Request(googlePlacesUrl, data=json.dumps(body).encode(), headers=headers, method="POST")
        try:
            with urlopen(request, timeout=30) as response:
                return parse_place_response(json.load(response))
        except HTTPError as e:
            message = e.read().decode(errors="replace")[:500]
            if e.code in (429, 500, 502, 503, 504):
                lastError = f"http {e.code}: {message}"
            else:
                raise RuntimeError(f"places search {query!r} failed with http {e.code}: {message}") from None
        except URLError as e:
            lastError = str(e.reason)
        if attempt < maxRetries:
            time.sleep(2 ** attempt)
    raise RuntimeError(f"places search {query!r} failed after {maxRetries} retries: {lastError}")

def find_places(queries, pathToData, flushEvery=500, maxCalls=None, keyFilename=None, workers=None):
    '''Returns {query: record} for every query, calling the Places API only for the ones not in the cache; queries is
    {query: (biasLat, biasLng)} (or None for no bias). Same cache, flush, cost guard, key and thread pool behaviour as
    geocode_addresses, in GEOCODING/googlePlacesCache.json.'''
    cache = get_places_cache(pathToData)
    misses = get_places_cache_misses(queries, pathToData)
    if len(misses) == 0:
        return {q: cache[q] for q in queries if q is not None}
    if maxCalls is not None and len(misses) > maxCalls:
        raise RuntimeError(f"{len(misses)} queries are not cached, more than maxCalls={maxCalls}")
    apiKey = get_api_key(pathToData, keyFilename)
    newRecords = 0
    pool = ThreadPoolExecutor(max_workers=workers if workers is not None else geocodeWorkers)
    futures = dict()
    for query in misses:
        bias = queries.get(query) or (None, None)
        futures[pool.submit(find_place, query, apiKey, bias[0], bias[1])] = query
    try:
        for future in as_completed(futures):
            cache[futures[future]] = future.result()
            newRecords += 1
            if newRecords % flushEvery == 0:
                write_places_cache(cache, pathToData)
                print(f"searched {newRecords}/{len(misses)}")
    finally:
        pool.shutdown(wait=True, cancel_futures=True)
        if newRecords > 0:
            write_places_cache(cache, pathToData)
    return {q: cache[q] for q in queries if q is not None}

def get_place_schema(prefix):
    return StructType([StructField("query", StringType()),
                       StructField(f"{prefix}PlaceLat", DoubleType()),
                       StructField(f"{prefix}PlaceLng", DoubleType()),
                       StructField(f"{prefix}PlaceName", StringType()),
                       StructField(f"{prefix}PlaceAddress", StringType()),
                       StructField(f"{prefix}PlaceTypes", StringType()),
                       StructField(f"{prefix}PlaceStatus", StringType())])

def add_place_info(DF, queryCol, pathToData, prefix, biasLatCol=None, biasLngCol=None, maxCalls=None, keyFilename=None):
    '''Adds {prefix}PlaceLat, {prefix}PlaceLng, {prefix}PlaceName, {prefix}PlaceAddress, {prefix}PlaceTypes (comma
    separated) and {prefix}PlaceStatus for the place name in queryCol, from a Places text search biased to the point in
    biasLatCol/biasLngCol when given (the first point seen for a query is used). The distinct queries are collected to
    the driver and looked up there (see find_places), so every distinct query that is not cached is a paid call;
    maxCalls=0 guarantees none. The resulting small table is broadcast joined back.'''
    cols = [queryCol] + ([biasLatCol, biasLngCol] if biasLatCol is not None else [])
    queries = dict()
    for row in DF.select(*cols).collect():
        if row[0] is None:
            continue
        bias = (row[1], row[2]) if biasLatCol is not None and row[1] is not None else None
        if row[0] not in queries or queries[row[0]] is None:
            queries[row[0]] = bias
    records = find_places(queries, pathToData, maxCalls=maxCalls, keyFilename=keyFilename)
    rows = [(q, r["lat"], r["lng"], r["name"], r["address"], r["types"], r["status"]) for q, r in records.items()]
    placeDF = DF.sparkSession.createDataFrame(rows, schema=get_place_schema(prefix))
    DF = DF.join(F.broadcast(placeDF), on=[F.col(queryCol) == F.col("query")], how="left_outer").drop("query")
    return DF
