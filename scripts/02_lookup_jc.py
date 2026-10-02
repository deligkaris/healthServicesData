'''Looks up the sites of the joint commission accredited organizations export with Google, twice, and fills the caches
only: the site's address with the Geocoding API (GEOCODING/googleGeocodeCache.json, keyed by address) and the site's
public name with the Places API text search (GEOCODING/googlePlacesCache.json, keyed by "name, ST"). Nothing is attached
to any dataframe and no parquet is written; prep_jcAccreditationDF attaches the address results from the cache later
with no paid call, and the places cache is there for the same purpose.

Usage: python scripts/02_lookup_jc.py [--jc jc.csv] [--data /path/to/DATA] [--key key.csv] [--no-addresses] [--no-places]
                                   [--dry-run] [--max-calls N]

    --jc        the export, one row per site and program (see prep_jcAccreditationDF for its columns)
    --data      pathToData of get_data: the key is read from DATA/GEOCODING/key.csv (--key to change) and both caches
                live in DATA/GEOCODING
The export's address is the accredited organization's, so for an organization with several sites every site carries
the parent's address; the name search (site DBA name when there is one, else the site name, plus the state, biased to
within 50 km of the parent's address) is what locates the site itself. The places search needs the Places API (New)
enabled on the key's project and costs about $32 per 1,000 queries beyond the free allowance.
The defaults are the laptop layout: the export, key and caches in ~/Downloads/GEOCODING (so --data is ~/Downloads).
'''
import argparse
import math
import os
import sys

pathToRepo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, pathToRepo)
import geocoding

parser = argparse.ArgumentParser()
parser.add_argument("--jc", default=os.path.expanduser("~/Downloads/GEOCODING/jc.csv"), help="joint commission export csv")
parser.add_argument("--data", default=os.path.expanduser("~/Downloads"), help="the DATA directory of get_data, with GEOCODING/key.csv in it")
parser.add_argument("--key", default=None, help="file with the Google API key, one line (default DATA/GEOCODING/key.csv)")
parser.add_argument("--no-addresses", action="store_true", help="skip the address geocoding")
parser.add_argument("--no-places", action="store_true", help="skip the name search")
parser.add_argument("--dry-run", action="store_true", help="only report how many addresses and names would be looked up")
parser.add_argument("--max-calls", type=int, default=2000, help="abort a lookup if more than this many are not cached")
args = parser.parse_args()
pathToData = os.path.abspath(args.data)
keyFilename = args.key if args.key is not None else geocoding.get_api_key_filename(pathToData)

for f in [args.jc, keyFilename]:
    if not os.path.isfile(f):
        sys.exit(f"missing {f}")

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import utilities

spark = SparkSession.builder.master("local[*]").appName("lookup_jc").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

rawJcDF = spark.read.csv(args.jc, header=True)
jcDF = utilities.prep_jcAccreditationDF(rawJcDF, topProgramOnly=True, excludePrograms=["stroke rehabilitation"])
sites = jcDF.select("jcAddress", "jcPlaceQuery").collect()
spark.stop()

addresses = sorted(set(row["jcAddress"] for row in sites if row["jcAddress"]))
queries = sorted(set(row["jcPlaceQuery"] for row in sites if row["jcPlaceQuery"]))
addressMisses = geocoding.get_geocode_cache_misses(addresses, pathToData)
queryMisses = geocoding.get_places_cache_misses(queries, pathToData)
print(f"{len(sites)} sites, {len(addresses)} distinct addresses ({len(addressMisses)} not cached), "
      f"{len(queries)} distinct names ({len(queryMisses)} not cached)")

if args.dry_run:
    sys.exit(0)

addressRecords = dict()
if not args.no_addresses:
    addressRecords = geocoding.geocode_addresses(addresses, pathToData, maxCalls=args.max_calls, keyFilename=keyFilename)
    print(f"addresses: {sum(r['status'] == 'OK' for r in addressRecords.values())} of {len(addressRecords)} found")

if not args.no_places:
    addressRecords = addressRecords or geocoding.geocode_addresses(addresses, pathToData, maxCalls=0, keyFilename=keyFilename)
    bias = dict()
    for row in sites:
        record = addressRecords.get(row["jcAddress"])
        if row["jcPlaceQuery"] and record and record["lat"] is not None:
            bias.setdefault(row["jcPlaceQuery"], (record["lat"], record["lng"]))
    placeRecords = geocoding.find_places({q: bias.get(q) for q in queries}, pathToData, maxCalls=args.max_calls, keyFilename=keyFilename)
    found = [r for r in placeRecords.values() if r["status"] == "OK"]
    hospitals = [r for r in found if "hospital" in (r["types"] or "").split(",")]
    print(f"names: {len(found)} of {len(placeRecords)} found, {len(hospitals)} of them typed hospital")

    def km(lat1, lng1, lat2, lng2):
        dLat, dLng = math.radians(lat2 - lat1), math.radians(lng2 - lng1)
        a = math.sin(dLat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dLng / 2) ** 2
        return 2 * 6371.0088 * math.asin(math.sqrt(a))
    buckets = [0, 0.1, 0.5, 2, 10]
    counts = dict()
    for row in sites:
        place, address = placeRecords.get(row["jcPlaceQuery"]), addressRecords.get(row["jcAddress"])
        if not place or not address or place["lat"] is None or address["lat"] is None:
            continue
        d = km(address["lat"], address["lng"], place["lat"], place["lng"])
        label = next((f"{lo} - {hi}" for lo, hi in zip(buckets, buckets[1:]) if lo <= d < hi), f"{buckets[-1]} -")
        counts[label] = counts.get(label, 0) + 1
    print("sites per bucket of the distance (km) between the name's place and the address:")
    for lo, hi in list(zip(buckets, buckets[1:])) + [(buckets[-1], None)]:
        label = f"{lo} - {hi}" if hi is not None else f"{lo} -"
        print(f"  {label:>10}  {counts.get(label, 0):>5}")
