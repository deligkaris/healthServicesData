'''Writes tests/data/benchmarks/geographicVariationState.csv, the state rows of the Original Medicare Geographic Variation
public use file that tests/data/test_benchmarks.py compares the library with: all ages, every year of the release, the
columns below. Public CMS aggregates, no beneficiary data.

Usage: python scripts/05_geographic_variation.py [--dataset ID] [--out PATH]

    --dataset   the data.cms.gov dataset id of "Medicare Geographic Variation - by National, State & County"; the default
                is the 2014-2024 release (data dictionary: 2014-2024 Original Medicare Geographic Variation Data
                Dictionary, methods: Original Medicare Geographic Variation Public Use File Methods Paper, both linked
                from https://data.cms.gov/summary-statistics-on-use-and-payments/medicare-geographic-comparisons/medicare-geographic-variation-by-national-state-county)
    --out       where the csv is written

A new release has a new dataset id (https://data.cms.gov/data.json lists it); rerun with it and review the diff of the csv.
'''
import argparse
import csv
import json
import os
import urllib.request

pathToRepo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
columns = ["YEAR", "BENE_GEO_CD", "BENE_GEO_DESC", "BENES_TOTAL_CNT", "BENES_WTH_PTAPTB_CNT", "BENES_OM_CNT", "BENES_MA_CNT",
           "BENE_AVG_AGE", "BENE_FEML_PCT", "BENE_DUAL_PCT", "IP_CVRD_STAYS_PER_1000_BENES", "ER_VISITS_PER_1000_BENES"]

parser = argparse.ArgumentParser()
parser.add_argument("--dataset", default="6219697b-8f6c-4164-bed4-cd9317c58ebc", help="data.cms.gov dataset id")
parser.add_argument("--out", default=os.path.join(pathToRepo, "tests", "data", "benchmarks", "geographicVariationState.csv"))
args = parser.parse_args()

url = (f"https://data.cms.gov/data-api/v1/dataset/{args.dataset}/data"
       "?filter[BENE_GEO_LVL]=State&filter[BENE_AGE_LVL]=All&size=5000")
request = urllib.request.Request(url, headers={"User-Agent": "healthServicesData"})
with urllib.request.urlopen(request, timeout=120) as response:
    rows = json.load(response)
if not rows or any(c not in rows[0] for c in columns):
    raise SystemExit(f"unexpected response from {url}: {len(rows)} rows, columns {sorted(rows[0]) if rows else None}")
rows.sort(key=lambda r: (r["YEAR"], r["BENE_GEO_CD"]))
os.makedirs(os.path.dirname(args.out), exist_ok=True)
with open(args.out, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(columns)
    writer.writerows([r[c] for c in columns] for r in rows)
print(f"{len(rows)} rows, years {rows[0]['YEAR']}-{rows[-1]['YEAR']}, written to {args.out}")
