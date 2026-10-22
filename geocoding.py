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
driving distances and times between two points from the Routes API, and the 2020 census block of a point from the
Census Geocoder, with every response cached on disk so that an address, a name, a route or a point is asked once. The
caches and the API key live under pathToData/GEOCODING, outside the repo: the key must never be committed and the caches
are data, not code.'''

googleGeocodeUrl = "https://maps.googleapis.com/maps/api/geocode/json"
googlePlacesUrl = "https://places.googleapis.com/v1/places:searchText"
googlePlacesFieldMask = "places.id,places.displayName,places.formattedAddress,places.location,places.types,places.businessStatus"
#the Routes API replaced the Directions API, which Google made Legacy (no new projects) on 2025-03-01
googleRoutesUrl = "https://routes.googleapis.com/directions/v2:computeRoutes"
googleRoutesFieldMask = "routes.distanceMeters,routes.duration"
censusGeographiesUrl = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
#the 2020 census blocks (layer 10) rather than Current_Current, whose blocks and response keys change over time
censusGeographiesParams = dict(benchmark="Public_AR_Census2020", vintage="Census2020_Census2020", layers="10", format="json")
retryStatuses = ["OVER_QUERY_LIMIT", "OVER_DAILY_LIMIT", "UNKNOWN_ERROR"]
#one thread, and a pause before every request to a Google API (Geocoding, Places, Routes), retries included, so Google
#gets at most one request a second
geocodeWorkers = 1
googleSecondsBetweenRequests = 1
geocodeFields = ["lat", "lng", "locationType", "formattedAddress", "partialMatch", "status", "geocodedOn"]
placeFields = ["lat", "lng", "name", "address", "types", "placeId", "businessStatus", "status", "searchedOn"]
routeFields = ["distanceMeters", "durationSeconds", "status", "routedOn"]
censusFields = ["block", "status", "searchedOn"]

class GeocodeRetryError(Exception):
    '''The API asked us to try again later (quota or a transient server side error).'''

def is_blank(text):
    '''True for a null or all white space address or query, which is never sent to an API.'''
    #a blank address still makes a paid call, answered with a made up point or INVALID_REQUEST; concat_ws produces one
    #when every part of an address is null
    return text is None or text.strip() == ""

def get_geocode_cache_filename(pathToData):
    return f"{pathToData}/GEOCODING/googleGeocodeCache.json"

def get_api_key_filename(pathToData):
    return f"{pathToData}/GEOCODING/key.csv"

def get_api_key(pathToData, keyFilename=None):
    '''The Google API key, one line in pathToData/GEOCODING/key.csv unless keyFilename says otherwise.'''
    #a file outside the repo (chmod 600) rather than an argument or environment variable, so the key never ends up in a
    #notebook cell, a shell history or a commit
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
    filename = get_geocode_cache_filename(pathToData)
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    #write then rename, so a kernel killed mid-write leaves the previous cache intact rather than a truncated json
    with open(filename + ".tmp", "w") as f:
        json.dump(cache, f, indent=1, sort_keys=True)
    os.replace(filename + ".tmp", filename)

def get_geocode_cache_misses(addresses, pathToData):
    '''The addresses that are not in the cache, ie the paid calls a geocode_addresses run would make.'''
    cache = get_geocode_cache(pathToData)
    return sorted(set(a for a in addresses if not is_blank(a) and a not in cache))

def parse_geocode_response(payload):
    '''The Geocoding API json as a cache record (geocodeFields).'''
    status = payload.get("status")
    if status in retryStatuses:
        raise GeocodeRetryError(f"{status}: {payload.get('error_message', '')}")
    if status == "REQUEST_DENIED":
        raise RuntimeError(f"REQUEST_DENIED: {payload.get('error_message', '')}")
    if status == "INVALID_REQUEST":
        raise ValueError(f"INVALID_REQUEST: {payload.get('error_message', '')}")
    if status not in ["OK", "ZERO_RESULTS"]:
        raise RuntimeError(f"unexpected geocoding status {status}")
    #ZERO_RESULTS is cached too (null coordinates) so an address the API cannot resolve is not asked again on every run
    record = dict(lat=None, lng=None, locationType=None, formattedAddress=None, partialMatch=None,
                  status=status, geocodedOn=date.today().isoformat())
    if status == "OK":
        result = payload["results"][0]
        record["lat"] = result["geometry"]["location"]["lat"]
        record["lng"] = result["geometry"]["location"]["lng"]
        #how precise the point is (ROOFTOP, RANGE_INTERPOLATED, GEOMETRIC_CENTER, APPROXIMATE) and whether the API could
        #not match the whole address and settled for something similar; kept to judge the coordinate downstream
        record["locationType"] = result["geometry"].get("location_type")
        record["formattedAddress"] = result.get("formatted_address")
        record["partialMatch"] = int(result.get("partial_match", False))
    return record

def geocode_address(address, apiKey, maxRetries=5):
    '''One Geocoding API call for an address, as a cache record.'''
    #exponential backoff on quota and transient errors; exceptions never carry the url because it contains the key
    url = googleGeocodeUrl + "?" + urlencode(dict(address=address, components="country:US", key=apiKey))
    for attempt in range(maxRetries + 1):
        time.sleep(googleSecondsBetweenRequests)
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
    '''{address: record} for every address, calling the Geocoding API only for the ones not in the cache.'''
    cache = get_geocode_cache(pathToData)
    misses = get_geocode_cache_misses(addresses, pathToData)
    if len(misses) == 0: #the key file is not even read
        return {a: cache[a] for a in addresses if not is_blank(a)}
    #cost guard: when more addresses are missing than maxCalls nothing is called and the count is reported instead
    if maxCalls is not None and len(misses) > maxCalls:
        raise RuntimeError(f"{len(misses)} addresses are not cached, more than maxCalls={maxCalls}")
    apiKey = get_api_key(pathToData, keyFilename)
    newRecords = 0
    #at ~1 request/s, 1k addresses take ~20 min
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
        #flushed however the loop ends, so an interrupted run keeps what it paid for and the next run resumes there;
        #on an error the calls not started yet are cancelled
        pool.shutdown(wait=True, cancel_futures=True)
        if newRecords > 0:
            write_geocode_cache(cache, pathToData)
    return {a: cache[a] for a in addresses if not is_blank(a)}

def add_address(DF, streetCol, cityCol, stateCol, zipCol, addressCol):
    '''addressCol: one line address "STREET, CITY, ST 12345" from the parts, the geocoding cache key.'''
    #trimmed, upper cased, runs of white space collapsed, so the same place written slightly differently in two sources
    #yields the same string and the same cache entry; a null street gives "CITY, ST 12345", an APPROXIMATE point
    def clean(col):
        return F.regexp_replace(F.upper(F.trim(F.col(col))), r"\s{2,}", " ")
    zip5 = F.substring(F.trim(F.col(zipCol)), 1, 5)
    stateZip = F.concat_ws(" ", clean(stateCol), zip5)
    #concat_ws skips nulls but not the empty string a state and zip both null produce, hence the when
    DF = DF.withColumn(addressCol,
                       F.concat_ws(", ", clean(streetCol), clean(cityCol), F.when(stateZip != "", stateZip)))
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
    {prefix}GeocodePartialMatch and {prefix}GeocodeStatus for the address in addressCol.'''
    #the distinct addresses are collected to the driver and geocoded there, so filter DF to the rows that need
    #coordinates first: every distinct address that is not cached is a paid call
    addresses = [row[0] for row in DF.select(addressCol).distinct().collect()]
    records = geocode_addresses(addresses, pathToData, maxCalls=maxCalls, keyFilename=keyFilename)
    rows = [(a, r["lat"], r["lng"], r["locationType"], r["formattedAddress"], r["partialMatch"], r["status"])
            for a, r in records.items()]
    geocodeDF = DF.sparkSession.createDataFrame(rows, schema=get_geocode_schema(prefix)) #small, broadcast joined back
    DF = DF.join(F.broadcast(geocodeDF), on=[F.col(addressCol) == F.col("address")], how="left_outer").drop("address")
    return DF

def get_geodesicDistanceKm(lat1, lng1, lat2, lng2):
    '''Great circle distance in km between two points given as column expressions of degrees.'''
    #haversine on a sphere of mean radius 6371.0088 km, accurate to ~0.3%, far below geocoding error
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
    #write then rename, as write_geocode_cache
    with open(filename + ".tmp", "w") as f:
        json.dump(cache, f, indent=1, sort_keys=True)
    os.replace(filename + ".tmp", filename)

def get_places_cache_misses(queries, pathToData):
    '''The queries that are not in the places cache, ie the paid calls a find_places run would make.'''
    cache = get_places_cache(pathToData)
    return sorted(set(q for q in queries if not is_blank(q) and q not in cache))

def parse_place_response(payload):
    '''The Places API text search json as a cache record (placeFields) holding the first result.'''
    #no result is a ZERO_RESULTS record so a name the API cannot resolve is not asked again on every run
    record = dict(lat=None, lng=None, name=None, address=None, types=None, placeId=None, businessStatus=None,
                  status="ZERO_RESULTS", searchedOn=date.today().isoformat())
    places = payload.get("places", [])
    if len(places) > 0:
        place = places[0]
        record["lat"] = place["location"]["latitude"]
        record["lng"] = place["location"]["longitude"]
        record["name"] = place.get("displayName", {}).get("text")
        record["address"] = place.get("formattedAddress")
        record["types"] = ",".join(place.get("types", [])) #a hospital should carry "hospital"
        record["placeId"] = place.get("id")
        record["businessStatus"] = place.get("businessStatus")
        record["status"] = "OK"
    return record

def find_place(query, apiKey, biasLat=None, biasLng=None, biasRadiusM=50000, maxRetries=5):
    '''One Places API (New) text search for a name such as "North Baldwin Infirmary, AL", the first place as a cache
    record (https://developers.google.com/maps/documentation/places/web-service/text-search).'''
    body = dict(textQuery=query, regionCode="US")
    #the bias makes the search prefer, without requiring, results within biasRadiusM of the point (the organization's
    #address; the API allows at most 50 km), which keeps a generic name like "Memorial Hospital" near where the site is expected
    if biasLat is not None and biasLng is not None:
        body["locationBias"] = dict(circle=dict(center=dict(latitude=biasLat, longitude=biasLng), radius=biasRadiusM))
    #the field mask limits the response to what the cache record holds, and what is billed
    headers = {"Content-Type": "application/json", "X-Goog-Api-Key": apiKey, "X-Goog-FieldMask": googlePlacesFieldMask}
    for attempt in range(maxRetries + 1):
        request = Request(googlePlacesUrl, data=json.dumps(body).encode(), headers=headers, method="POST")
        time.sleep(googleSecondsBetweenRequests)
        try:
            with urlopen(request, timeout=30) as response:
                return parse_place_response(json.load(response))
        except HTTPError as e:
            message = e.read().decode(errors="replace")[:500]
            if e.code in (429, 500, 502, 503, 504): #quota and transient errors are retried with exponential backoff
                lastError = f"http {e.code}: {message}"
            else: #400 bad request, 403 API not enabled or key not allowed; the message never carries the key
                raise RuntimeError(f"places search {query!r} failed with http {e.code}: {message}") from None
        except URLError as e:
            lastError = str(e.reason)
        if attempt < maxRetries:
            time.sleep(2 ** attempt)
    raise RuntimeError(f"places search {query!r} failed after {maxRetries} retries: {lastError}")

def find_places(queries, pathToData, flushEvery=500, maxCalls=None, keyFilename=None, workers=None):
    '''{query: record} for every query of {query: (biasLat, biasLng) or None}, calling the Places API only for the ones
    not in the cache; the cache, flush, cost guard, key and thread pool behave as in geocode_addresses.'''
    cache = get_places_cache(pathToData)
    misses = get_places_cache_misses(queries, pathToData)
    if len(misses) == 0:
        return {q: cache[q] for q in queries if not is_blank(q)}
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
    return {q: cache[q] for q in queries if not is_blank(q)}

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
    separated) and {prefix}PlaceStatus for the place name in queryCol, searched with a bias to biasLatCol/biasLngCol.'''
    #the distinct queries are collected to the driver and looked up there, every uncached one is a paid call (maxCalls=0
    #guarantees none); the first bias point seen for a query is used
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
    placeDF = DF.sparkSession.createDataFrame(rows, schema=get_place_schema(prefix)) #small, broadcast joined back
    DF = DF.join(F.broadcast(placeDF), on=[F.col(queryCol) == F.col("query")], how="left_outer").drop("query")
    return DF

def read_json_cache(filename):
    if not os.path.isfile(filename):
        return dict()
    with open(filename) as f:
        return json.load(f)

def write_json_cache(cache, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    #write then rename, as write_geocode_cache
    with open(filename + ".tmp", "w") as f:
        json.dump(cache, f, indent=1, sort_keys=True)
    os.replace(filename + ".tmp", filename)

def get_cached_records(argsByKey, cacheFilename, get_call, maxCalls=None, flushEvery=500, workers=None):
    '''{key: record} for every key of argsByKey ({key: the arguments of one call}), calling get_call()(*arguments) only
    for the keys not in the json cache at cacheFilename; the cache, flush, cost guard and thread pool behave as in
    geocode_addresses.'''
    cache = read_json_cache(cacheFilename)
    misses = sorted(k for k in argsByKey if k not in cache)
    if len(misses) == 0:
        return {k: cache[k] for k in argsByKey}
    if maxCalls is not None and len(misses) > maxCalls:
        raise RuntimeError(f"{len(misses)} keys are not cached, more than maxCalls={maxCalls}")
    #get_call runs only when something is missing, so a fully cached run does not even read the API key
    call = get_call()
    newRecords = 0
    pool = ThreadPoolExecutor(max_workers=workers if workers is not None else geocodeWorkers)
    futures = {pool.submit(call, *argsByKey[k]): k for k in misses}
    try:
        for future in as_completed(futures):
            cache[futures[future]] = future.result()
            newRecords += 1
            if newRecords % flushEvery == 0:
                write_json_cache(cache, cacheFilename)
                print(f"{os.path.basename(cacheFilename)}: {newRecords}/{len(misses)}")
    finally:
        pool.shutdown(wait=True, cancel_futures=True)
        if newRecords > 0:
            write_json_cache(cache, cacheFilename)
    return {k: cache[k] for k in argsByKey}

def get_routes_cache_filename(pathToData):
    return f"{pathToData}/GEOCODING/googleRoutesCache.json"

def get_route_key(fromLat, fromLng, toLat, toLng):
    '''The routes cache key, "lat,lng;lat,lng" from origin to destination.'''
    #6 decimals is ~0.1 m, finer than any geocode, and keeps the key independent of how a float prints
    return f"{fromLat:.6f},{fromLng:.6f};{toLat:.6f},{toLng:.6f}"

def parse_route_response(payload):
    '''The Routes API json as a cache record (routeFields) holding the first route.'''
    #no route (eg an island hospital with no road to the other) comes back as {} and is cached as ZERO_RESULTS
    record = dict(distanceMeters=None, durationSeconds=None, status="ZERO_RESULTS", routedOn=date.today().isoformat())
    routes = payload.get("routes", [])
    if len(routes) > 0:
        #proto3 json leaves out zero values, which two NPIs at one address produce; duration is a string like "165s"
        record["distanceMeters"] = routes[0].get("distanceMeters", 0)
        record["durationSeconds"] = round(float(routes[0].get("duration", "0s").rstrip("s")))
        record["status"] = "OK"
    return record

def route_dyad(fromLat, fromLng, toLat, toLng, apiKey, maxRetries=5):
    '''One Routes API call for driving from one point to another, as a cache record
    (https://developers.google.com/maps/documentation/routes/compute_route_directions).'''
    def waypoint(lat, lng):
        return dict(location=dict(latLng=dict(latitude=lat, longitude=lng)))
    #TRAFFIC_UNAWARE: no departure time and no live traffic, the cheapest SKU and the same answer on every run
    body = dict(origin=waypoint(fromLat, fromLng), destination=waypoint(toLat, toLng),
                travelMode="DRIVE", routingPreference="TRAFFIC_UNAWARE")
    headers = {"Content-Type": "application/json", "X-Goog-Api-Key": apiKey, "X-Goog-FieldMask": googleRoutesFieldMask}
    for attempt in range(maxRetries + 1):
        request = Request(googleRoutesUrl, data=json.dumps(body).encode(), headers=headers, method="POST")
        time.sleep(googleSecondsBetweenRequests)
        try:
            with urlopen(request, timeout=30) as response:
                return parse_route_response(json.load(response))
        except HTTPError as e:
            message = e.read().decode(errors="replace")[:500]
            if e.code in (429, 500, 502, 503, 504): #quota and transient errors are retried with exponential backoff
                lastError = f"http {e.code}: {message}"
            else: #403 is most often the Routes API not enabled for the key's project; the message never carries the key
                raise RuntimeError(f"route {get_route_key(fromLat, fromLng, toLat, toLng)} failed with http {e.code}: {message}") from None
        except OSError as e: #URLError, a timeout or a dropped connection
            lastError = str(e)
        if attempt < maxRetries:
            time.sleep(2 ** attempt)
    raise RuntimeError(f"route {get_route_key(fromLat, fromLng, toLat, toLng)} failed after {maxRetries} retries: {lastError}")

def get_routes(pairs, pathToData, maxCalls=None, keyFilename=None, workers=None):
    '''{get_route_key: record} for every (fromLat, fromLng, toLat, toLng) of pairs, calling the Routes API only for the
    pairs not in pathToData/GEOCODING/googleRoutesCache.json.'''
    argsByKey = {get_route_key(*pair): pair for pair in pairs}
    def get_call():
        apiKey = get_api_key(pathToData, keyFilename)
        return lambda fromLat, fromLng, toLat, toLng: route_dyad(fromLat, fromLng, toLat, toLng, apiKey)
    return get_cached_records(argsByKey, get_routes_cache_filename(pathToData), get_call, maxCalls=maxCalls, workers=workers)

def add_driving_info(DF, fromLatCol, fromLngCol, toLatCol, toLngCol, pathToData, prefix, maxCalls=None, keyFilename=None):
    '''Adds {prefix}DrivingDistanceKm, {prefix}DrivingTimeMin and {prefix}DrivingStatus (OK or ZERO_RESULTS) for driving
    from the point at fromLatCol/fromLngCol to the one at toLatCol/toLngCol, from the Google Routes API.'''
    #the distinct point pairs are collected to the driver and routed there, every uncached pair is a paid call (maxCalls=0
    #guarantees none, and its error says how many there would be); a row with a null coordinate gets null columns
    coordinateCols = [fromLatCol, fromLngCol, toLatCol, toLngCol]
    #tuple first: "in" on a Row looks at its field names, not its values
    pairs = [pair for pair in map(tuple, DF.select(*coordinateCols).distinct().collect()) if None not in pair]
    records = get_routes(pairs, pathToData, maxCalls=maxCalls, keyFilename=keyFilename)
    rows = []
    for pair in pairs:
        record = records[get_route_key(*pair)]
        rows.append(pair + (record["distanceMeters"], record["durationSeconds"], record["status"]))
    routeCoordinateCols = ["routeFromLat", "routeFromLng", "routeToLat", "routeToLng"]
    schema = StructType([StructField(c, DoubleType()) for c in routeCoordinateCols] +
                        [StructField("routeDistanceMeters", IntegerType()), StructField("routeDurationSeconds", IntegerType()),
                         StructField("routeStatus", StringType())])
    routeDF = DF.sparkSession.createDataFrame(rows, schema=schema) #small, broadcast joined back
    #the coordinates went to the driver and back as python floats, which round trip a double exactly
    DF = (DF.join(F.broadcast(routeDF), on=[F.col(c) == F.col(r) for c, r in zip(coordinateCols, routeCoordinateCols)],
                  how="left_outer")
            .withColumn(f"{prefix}DrivingDistanceKm", F.col("routeDistanceMeters") / 1000)
            .withColumn(f"{prefix}DrivingTimeMin", F.col("routeDurationSeconds") / 60)
            .withColumn(f"{prefix}DrivingStatus", F.col("routeStatus"))
            .drop(*routeCoordinateCols, "routeDistanceMeters", "routeDurationSeconds", "routeStatus"))
    return DF

def get_census_cache_filename(pathToData):
    return f"{pathToData}/GEOCODING/censusBlockCache.json"

def get_point_key(lat, lng):
    '''The census cache key, "lat,lng" at 6 decimals as in get_route_key.'''
    return f"{lat:.6f},{lng:.6f}"

def parse_census_response(payload):
    '''The Census Geocoder json as a cache record (censusFields) holding the 15 digit GEOID of the 2020 census block.'''
    #a point in no block (eg offshore) comes back without one and is cached as ZERO_RESULTS
    record = dict(block=None, status="ZERO_RESULTS", searchedOn=date.today().isoformat())
    blocks = payload.get("result", {}).get("geographies", {}).get("Census Blocks", [])
    if len(blocks) > 0:
        record["block"] = blocks[0]["GEOID"]
        record["status"] = "OK"
    return record

def get_census_block(lat, lng, maxRetries=5):
    '''One Census Geocoder call for the 2020 census block of a point, as a cache record
    (https://geocoding.geo.census.gov/geocoder/Geocoding_Services_API.html).'''
    #free and keyless; x is the longitude and y the latitude
    url = censusGeographiesUrl + "?" + urlencode(dict(x=lng, y=lat, **censusGeographiesParams))
    for attempt in range(maxRetries + 1):
        try:
            with urlopen(url, timeout=60) as response:
                return parse_census_response(json.load(response))
        except HTTPError as e:
            if e.code < 500:
                raise RuntimeError(f"census block of {get_point_key(lat, lng)} failed with http {e.code}") from None
            lastError = f"http {e.code}"
        except OSError as e: #URLError, a timeout or a dropped connection
            lastError = str(e)
        if attempt < maxRetries:
            time.sleep(2 ** attempt)
    raise RuntimeError(f"census block of {get_point_key(lat, lng)} failed after {maxRetries} retries: {lastError}")

def get_census_blocks(points, pathToData, maxCalls=None, workers=None):
    '''{get_point_key: record} for every (lat, lng) of points, calling the Census Geocoder only for the points not in
    pathToData/GEOCODING/censusBlockCache.json.'''
    argsByKey = {get_point_key(*point): point for point in points}
    return get_cached_records(argsByKey, get_census_cache_filename(pathToData), lambda: get_census_block,
                              maxCalls=maxCalls, workers=workers)

def add_census_geography_info(DF, latCol, lngCol, pathToData, prefix, maxCalls=None):
    '''Adds {prefix}CensusBlockGroup (12 digit GEOID, the ADI key) and {prefix}CensusTract (11 digits) of the 2020 census
    block the point at latCol/lngCol is in, from the Census Geocoder.'''
    #the distinct points are collected to the driver and looked up there, ~0.3 s a point; a null coordinate gets nulls
    points = [point for point in map(tuple, DF.select(latCol, lngCol).distinct().collect()) if None not in point]
    records = get_census_blocks(points, pathToData, maxCalls=maxCalls)
    rows = [point + (records[get_point_key(*point)]["block"],) for point in points]
    schema = StructType([StructField("censusLat", DoubleType()), StructField("censusLng", DoubleType()),
                         StructField("censusBlock", StringType())])
    blockDF = DF.sparkSession.createDataFrame(rows, schema=schema) #small, broadcast joined back
    #the block GEOID is state (2), county (3), tract (6), block (4) digits, and the block group is the block's first digit
    DF = (DF.join(F.broadcast(blockDF), on=[F.col(latCol) == F.col("censusLat"), F.col(lngCol) == F.col("censusLng")],
                  how="left_outer")
            .withColumn(f"{prefix}CensusBlockGroup", F.substring("censusBlock", 1, 12))
            .withColumn(f"{prefix}CensusTract", F.substring("censusBlock", 1, 11))
            .drop("censusLat", "censusLng", "censusBlock"))
    return DF
