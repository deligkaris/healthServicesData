# Health Services Data Management

PySpark functions for CMS and other health services-related data management. 

- All functions assume that column names follow the CMS Field Short Name convention 
- Base functions with name add_x  add 1 column with column name x (so that you do not have to guess the column name)
- Revenue/Line functions with name add_x include an inClaim flag, because sometimes we are interested in determining 
  something based on the line and sometimes we are interested in something claim-wide
  - when inClaim=True, these functions add 1 column with name x and 1 column with name xInClaim
  - when inClaim=False, these functions add 1 column with name x
- Functions with complex names, eg add_pcpHomeVisit, implicitly assume an AND condition, eg pcp and home visit
- Functions with name add_x_info add more than 1 column on the dataframe (you will need to do a printSchema to see what you added)
- Functions with name get_x return x and the argument(s) of the function is/are not modified
- When there is only one dataframe argument, then this is probably done using a withColumn pyspark command or a command 
   that does not require data shuffle (smaller computational load)
- When there are more than one dataframe arguments, then this is probably done using a join pyspark command or a command 
   that requires data shuffle (larger computational load)
- Explanations and references for the methods implemented are included on the code, when available

Examples:

- add_death_date_info(mbsfDF) uses withColumn commands to add columns to the mbsfDF using information from the same dataframe
- add_death_date_info(baseDF,mbsfDF) uses a join to add columns to baseDF using information from the mbsfDF dataframe

## Linking Joint Commission stroke centers to CMS hospitals (scripts/)

The Joint Commission export of stroke-certified sites carries no CCN, so a site is matched to the CMS Provider of
Services (POS) hospital whose geocoded point is nearest the site's; names are not compared between the two sources
except as a last resort. `scripts/01_geocode_pos.py` geocodes the POS hospital addresses with the Google Geocoding API
and builds `pos.parquet`; `scripts/02_lookup_jc.py` geocodes the export's addresses and, because those are the accredited
organization's rather than the site's, locates each site by searching its public name with the Google Places API,
filling two caches under `DATA/GEOCODING` so every paid lookup happens once;
`scripts/03_geocode_jc.py` attaches both results and builds `jcAccreditation.parquet` (one row per site, its top stroke
certification); `scripts/04_match_jc_pos.py` assigns each site the nearest active acute or critical access hospital
within 0.5 km of the point found for its name (`nearestSite`), else within 0.5 km of the organization's address, which
is where CMS lists a campus that bills under its parent (`nearestParent`), else, as a last resort, a hospital within
5 km whose name contains every distinctive word of the site's (`nearestNamed`), and writes `jcMatched.parquet` (one row
per site, `posMatchMethod` records how it was assigned) and `ccnStrokeCertification.parquet`, the per-CCN table that
joins the claims on `PROVIDER`: `jcBestProgram` is the highest certification among the sites assigned to the CCN,
`jcSites` how many sites that was, and `jcCertificationConfidence` folds the two into one scale (4: the certified site is
the CCN's only site and a hospital sits where Google puts its name; 3: same but other sites share the CCN; 2: the site
was placed at its organization's address, a campus credited to the parent; 1: name based). The rule is deterministic
and reproducible from the two source files, the key and the caches.

