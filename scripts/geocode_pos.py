'''Geocodes the hospitals of the CMS Provider of Services file and writes the pos parquet that
healthServicesData.get_data loads.

Usage: python scripts/geocode_pos.py [--pos POS_OTHER_DEC22.csv] [--data /path/to/DATA] [--key key.csv] [--out pos.parquet]
                                     [--active-only] [--dry-run] [--max-calls N]

    --pos       the raw POS "other" file from data.cms.gov
    --data      pathToData of get_data: the key is read from DATA/GEOCODING/key.csv (--key to change) and the address
                cache lives in DATA/GEOCODING/googleGeocodeCache.json (one record per address, reused on every later
                run, only new addresses cost a call); the parquet is written to DATA/PROVIDER-OF-SERVICES/pos.parquet
                (--out to change), which is where get_data loads it from
The defaults are the arguments the Dec 2022 pos.parquet was built with on the laptop: the raw file, key and cache in
~/Downloads/GEOCODING (so --data is ~/Downloads), the parquet written next to them, every hospital geocoded.
The POS file keeps every provider that ever had a CCN, closed ones included; --active-only geocodes only the hospitals
with PGM_TRMNTN_CD 00 (the parquet still keeps every row, terminated hospitals just get null coordinates).
The repo's geocoding and utilities modules are imported from the parent directory of this script.
'''
import argparse
import os
import sys

pathToRepo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, pathToRepo)
import geocoding

parser = argparse.ArgumentParser()
parser.add_argument("--pos", default=os.path.expanduser("~/Downloads/GEOCODING/POS_OTHER_DEC22.csv"), help="raw POS other csv")
parser.add_argument("--data", default=os.path.expanduser("~/Downloads"), help="the DATA directory of get_data, with GEOCODING/key.csv in it")
parser.add_argument("--key", default=None, help="file with the Google Geocoding API key, one line (default DATA/GEOCODING/key.csv)")
parser.add_argument("--out", default=os.path.expanduser("~/Downloads/GEOCODING/pos.parquet"), help="parquet to write (DATA/PROVIDER-OF-SERVICES/pos.parquet on OSC)")
parser.add_argument("--active-only", action="store_true", help="geocode only active hospitals (PGM_TRMNTN_CD 00)")
parser.add_argument("--dry-run", action="store_true", help="only report how many addresses would be geocoded")
parser.add_argument("--max-calls", type=int, default=13000, help="abort before calling the API if more addresses than this are not cached")
args = parser.parse_args()
pathToData = os.path.abspath(args.data)
keyFilename = args.key if args.key is not None else geocoding.get_api_key_filename(pathToData)
outFilename = args.out

for f in [args.pos, keyFilename]:
    if not os.path.isfile(f):
        sys.exit(f"missing {f}")

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import utilities

spark = SparkSession.builder.master("local[*]").appName("geocode_pos").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

rawPosDF = spark.read.csv(args.pos, header=True)
posDF = utilities.prep_posDF(rawPosDF)
toGeocode = (F.col("posHospital") == 1) & ((F.col("posActive") == 1) | F.lit(not args.active_only))
addresses = [row[0] for row in posDF.filter(toGeocode).select("posAddress").distinct().collect()]
misses = geocoding.get_geocode_cache_misses(addresses, pathToData)
print(f"{posDF.count()} POS rows, {posDF.filter(F.col('posHospital') == 1).count()} hospitals, "
      f"{posDF.filter((F.col('posHospital') == 1) & (F.col('posActive') == 1)).count()} active hospitals")
print(f"{len(addresses)} distinct addresses to geocode, {len(misses)} not cached (paid calls)")

if args.dry_run:
    sys.exit(0)

utilities.prep_posDF(rawPosDF, pathToData=pathToData, filename=outFilename, maxCalls=args.max_calls, keyFilename=keyFilename,
                     activeOnly=args.active_only)

posDF = spark.read.parquet(outFilename)
print(f"wrote {outFilename}: {posDF.count()} rows")
(posDF.filter(toGeocode)
      .groupBy("posGeocodeStatus", "posGeocodeLocationType", "posGeocodePartialMatch")
      .count().orderBy(F.desc("count")).show(truncate=False))
spark.stop()
