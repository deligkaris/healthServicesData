'''Builds jcAccreditation.parquet, the processed joint commission accredited organizations export that
scripts/04_match_jc_pos.py reads: one row per site with its top stroke certification, the geocoded address of its
organization and the place Google found for the site's own name (see utilities.prep_jcAccreditationDF).

Usage: python scripts/03_geocode_jc.py [--jc jc.csv] [--data /path/to/DATA] [--key key.csv] [--out jcAccreditation.parquet]
                                    [--no-places] [--dry-run] [--max-calls N]

    --jc        the export, one row per site and program
    --data      pathToData of get_data: the key is read from DATA/GEOCODING/key.csv (--key to change) and the address
                and places caches live in DATA/GEOCODING; run scripts/02_lookup_jc.py first to fill them, this script
                makes no paid call by default (--max-calls 0)
Stroke Rehabilitation rows are dropped and a site with several programs keeps its top one (Comprehensive >
Thrombectomy-Capable > Primary > Acute Stroke Ready). The site location (jcSiteLat/jcSiteLng) is the place found for
the name when it is a hospital, else the organization's address.
The defaults are the laptop layout: the export, key, caches and output in ~/Downloads/GEOCODING (so --data is ~/Downloads).
'''
import argparse
import os
import sys

pathToRepo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, pathToRepo)
import geocoding

parser = argparse.ArgumentParser()
parser.add_argument("--jc", default=os.path.expanduser("~/Downloads/GEOCODING/jc.csv"), help="joint commission export csv")
parser.add_argument("--data", default=os.path.expanduser("~/Downloads"), help="the DATA directory of get_data, with GEOCODING/key.csv in it")
parser.add_argument("--key", default=None, help="file with the Google API key, one line (default DATA/GEOCODING/key.csv)")
parser.add_argument("--out", default=os.path.expanduser("~/Downloads/GEOCODING/jcAccreditation.parquet"), help="parquet to write (DATA/JOINT-COMMISSION/jcAccreditation.parquet on OSC)")
parser.add_argument("--no-places", action="store_true", help="do not attach the place found for the site name")
parser.add_argument("--dry-run", action="store_true", help="only report the row counts and what is not cached")
parser.add_argument("--max-calls", type=int, default=0, help="allow this many paid lookups for addresses or names not cached (default none)")
args = parser.parse_args()
pathToData = os.path.abspath(args.data)
keyFilename = args.key if args.key is not None else geocoding.get_api_key_filename(pathToData)

if not os.path.isfile(args.jc):
    sys.exit(f"missing {args.jc}")

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import utilities

spark = SparkSession.builder.master("local[*]").appName("geocode_jc").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

excludePrograms = ["stroke rehabilitation"]
rawJcDF = spark.read.csv(args.jc, header=True)
allDF = utilities.prep_jcAccreditationDF(rawJcDF)
keptDF = utilities.prep_jcAccreditationDF(rawJcDF, excludePrograms=excludePrograms)
jcDF = utilities.prep_jcAccreditationDF(rawJcDF, topProgramOnly=True, excludePrograms=excludePrograms)
siteKey = ["jcHcoId", "jcSiteName", "jcAddress"]
addresses = [row[0] for row in jcDF.select("jcAddress").distinct().collect()]
queries = [row[0] for row in jcDF.select("jcPlaceQuery").distinct().collect()]
print(f"{allDF.count()} rows in the export, {allDF.count() - keptDF.count()} rows dropped as {excludePrograms}, "
      f"{allDF.select(siteKey).distinct().count() - keptDF.select(siteKey).distinct().count()} sites lost entirely by that")
print(f"{jcDF.count()} sites after keeping the top program per site, "
      f"{len(addresses)} distinct addresses ({len(geocoding.get_geocode_cache_misses(addresses, pathToData))} not cached), "
      f"{len(queries)} distinct names ({len(geocoding.get_places_cache_misses(queries, pathToData))} not cached)")
jcDF.groupBy("jcProgramRank", "jcProgram").count().orderBy("jcProgramRank", F.desc("count")).show(100, truncate=False)

if args.dry_run:
    sys.exit(0)

utilities.prep_jcAccreditationDF(rawJcDF, pathToData=pathToData, filename=args.out, maxCalls=args.max_calls, keyFilename=keyFilename,
                                 topProgramOnly=True, excludePrograms=excludePrograms, placesLookup=not args.no_places)

jcDF = spark.read.parquet(args.out)
print(f"wrote {args.out}: {jcDF.count()} rows")
jcDF.groupBy("jcGeocodeStatus", "jcGeocodeLocationType", "jcGeocodePartialMatch").count().orderBy(F.desc("count")).show(truncate=False)
if not args.no_places:
    jcDF.groupBy("jcPlaceStatus", "jcPlaceIsHospital", "jcSiteLocationSource").count().orderBy(F.desc("count")).show(truncate=False)
    buckets = [0, 0.1, 0.5, 2, 10]
    print("sites per bucket of the distance (km) between the place found for the name and the organization's address:")
    for lo, hi in list(zip(buckets, buckets[1:])) + [(buckets[-1], None)]:
        cond = (F.col("jcPlaceDistanceKm") >= lo) & ((F.col("jcPlaceDistanceKm") < hi) if hi is not None else F.lit(True))
        print(f"  {lo} - {'' if hi is None else hi:<4}  {jcDF.filter(cond).count():>5}")
spark.stop()
