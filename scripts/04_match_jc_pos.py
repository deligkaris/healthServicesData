'''Assigns each joint commission site the CCN of the POS hospital it is (see utilities.add_pos_ccn_info), from the
geocoded locations alone: the nearest active acute or critical access hospital within 0.5 km of the site's own point, else
within 0.5 km of its organization's address (a campus billing under its parent), else, as a last resort and the only use
of names, the hospital within 5 km whose name contains every distinctive word of the site's name (nearestNamed), else none.
No hand review; an optional csv of exceptions (jcHcoId, jcSiteName, keepCcn) can override single sites.

Usage: python scripts/04_match_jc_pos.py [--jc jcAccreditation.parquet] [--pos pos.parquet] [--overrides overrides.csv]
                                      [--max-distance-km 0.5] [--tie-distance-km 0.1]
                                      [--out jcMatched.parquet] [--ccn-out ccnStrokeCertification.parquet] [--unmatched jcUnmatched.csv]

Writes jcMatched.parquet (one row per site with posCcn, posCcnDistanceKm, posMatchMethod, ...), ccnStrokeCertification.parquet
(one row per CCN with its best stroke certification and the number of sites; see utilities.get_ccn_jc_info) and
jcUnmatched.csv (the sites without a CCN and what was found near them, a report to eyeball, not an input), and prints how
the sites were assigned. The defaults are the laptop layout, everything in ~/Downloads/GEOCODING.
'''
import argparse
import csv
import os
import sys

pathToRepo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, pathToRepo)

geocodingDir = os.path.expanduser("~/Downloads/GEOCODING")
parser = argparse.ArgumentParser()
parser.add_argument("--jc", default=os.path.join(geocodingDir, "jcAccreditation.parquet"), help="written by scripts/03_geocode_jc.py")
parser.add_argument("--pos", default=os.path.join(geocodingDir, "pos.parquet"), help="written by scripts/01_geocode_pos.py")
parser.add_argument("--overrides", default=None, help="optional csv of exceptions: jcHcoId, jcSiteName, keepCcn (blank for no CCN)")
parser.add_argument("--max-distance-km", type=float, default=0.5)
parser.add_argument("--tie-distance-km", type=float, default=0.1)
parser.add_argument("--relaxed-distance-km", type=float, default=5.0, help="reach of the last resort name checked lookup for sites still unmatched")
parser.add_argument("--name-score-min", type=float, default=1.0, help="name score the last resort lookup requires (1 = every distinctive word of the shorter name is shared)")
parser.add_argument("--out", default=os.path.join(geocodingDir, "jcMatched.parquet"))
parser.add_argument("--ccn-out", default=os.path.join(geocodingDir, "ccnStrokeCertification.parquet"))
parser.add_argument("--unmatched", default=os.path.join(geocodingDir, "jcUnmatched.csv"))
args = parser.parse_args()

for f in [args.jc, args.pos] + ([args.overrides] if args.overrides else []):
    if not os.path.exists(f):
        sys.exit(f"missing {f}")

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import utilities

spark = SparkSession.builder.master("local[*]").appName("match_jc_pos").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

jcDF = spark.read.parquet(args.jc)
posDF = spark.read.parquet(args.pos).filter(F.col("posHospitalType").isin(["acute", "cah"]) & (F.col("posActive") == 1))
overridesDF = spark.read.csv(args.overrides, header=True) if args.overrides else None

jcDF = utilities.add_pos_ccn_info(jcDF, posDF, maxDistanceKm=args.max_distance_km, tieDistanceKm=args.tie_distance_km,
                                  relaxedDistanceKm=args.relaxed_distance_km, nameScoreMin=args.name_score_min, overridesDF=overridesDF)
jcDF.cache()
print(f"{jcDF.count()} sites, {posDF.count()} candidate hospitals (acute or cah, active)")
jcDF.groupBy("posMatchMethod", "jcSiteLocationSource").count().orderBy("posMatchMethod", "jcSiteLocationSource").show()
print(f"{jcDF.filter(F.col('posMatchAmbiguous') == 1).count()} assigned sites had another hospital within {args.tie_distance_km} km (tie broken toward the open one)")
print("sites matched by the last resort name checked lookup (nearestNamed), the only name based assignments:")
(jcDF.filter(F.col("posMatchMethod") == "nearestNamed")
     .select("jcSearchName", "posCcn", "posCcnFacName", F.round("posCcnDistanceKm", 2).alias("posCcnDistanceKm"), "posCcnNameScore")
     .orderBy("jcSearchName").show(100, truncate=45))
print("assigned sites per distance bucket (km):")
buckets = [0, 0.1, 0.3, 0.5, 5]
for lo, hi in zip(buckets, buckets[1:]):
    n = jcDF.filter((F.col("posCcnDistanceKm") > lo if lo > 0 else F.col("posCcnDistanceKm") >= lo) & (F.col("posCcnDistanceKm") <= hi)).count()
    print(f"  {lo} - {hi}  {n}")

ccnDF = utilities.get_ccn_jc_info(jcDF)
print(f"{ccnDF.count()} CCNs received a site, {ccnDF.filter(F.col('jcSites') > 1).count()} of them more than one:")
ccnDF.filter(F.col("jcSites") > 1).select("posCcn", "jcSites", "jcBestProgram", "jcSiteNames").orderBy(F.desc("jcSites")).show(100, truncate=False)
ccnDF.groupBy("jcBestProgramRank", "jcBestProgram").count().orderBy("jcBestProgramRank").show(truncate=False)
ccnDF.groupBy("jcCertificationConfidence", "jcBestProgramMatchMethod").count().orderBy(F.desc("jcCertificationConfidence")).show(truncate=False)

unmatchedCols = ["jcHcoId", "jcSiteName", "jcSiteDbaName", "jcAddress", "jcProgram", "jcSiteLocationSource",
                 "jcPlaceName", "jcPlaceAddress", "jcPlaceTypes", "jcPlaceDistanceKm",
                 "posNearestCcn", "posNearestFacName", "posNearestDistanceKm",
                 "posParentNearestCcn", "posParentNearestFacName", "posParentNearestDistanceKm"]
unmatched = jcDF.filter(F.col("posCcn").isNull()).select([c for c in unmatchedCols if c in jcDF.columns]).orderBy("jcSiteName").collect()
with open(args.unmatched, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([c for c in unmatchedCols if c in jcDF.columns])
    writer.writerows(unmatched)
print(f"wrote {args.unmatched}: {len(unmatched)} sites without a CCN")

jcDF.drop("jcProgramRank").coalesce(1).write.mode("overwrite").parquet(args.out)
ccnDF.drop("jcBestProgramRank").coalesce(1).write.mode("overwrite").parquet(args.ccn_out)
print(f"wrote {args.out} and {args.ccn_out}")
spark.stop()
