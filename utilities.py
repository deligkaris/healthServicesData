import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from schemas import hcrisRptSchema, hcrisNmrcSchema
from geocoding import add_address, add_geocode_info, add_place_info, get_geocode_schema, get_geodesicDistanceKm
from urllib.request import urlopen
import json
import re
from itertools import chain
from functools import reduce

#yearMin and yearMax are limits, the code is designed to operate within these limits
#create a map from year to number of days in all previous years (assume there is a year that is year 0)
#this helps with finding the day number an event has occured (Jan 1 of year 0 is day 1 eg)
leapYears = [2012, 2016, 2020, 2024, 2028]
yearMin = min(leapYears)-2
yearMax = max(leapYears)+2
years = [y for y in range(yearMin,yearMax)]  
nonLeapYears = list( set(years)^set(leapYears) )
daysInYearsPriorDict = dict()
monthsInYearsPriorDict = dict()
for year in range(min(years),max(years)):
    nLeapYears = sum( [year>x for x in leapYears] )
    nNonLeapYears = sum( [year>x for x in nonLeapYears])
    days = nNonLeapYears*365 + nLeapYears*366
    daysInYearsPriorDict[year] = days
    months = nNonLeapYears*12 + nLeapYears*12
    monthsInYearsPriorDict[year] = months
#absolute day number of October 1, 2015 (when ICD10 took effect), in the same frame as THRU_DT_DAY (see
#cms.utilities.add_through_date_info) so it tracks any change to leapYears/yearMin. 274 = day-of-year of Oct 1
#in non-leap 2015 (Jan-Sep = 273 days). Used to null comorbidities whose 360-day lookback reaches into the ICD9 era.
icd10Day = daysInYearsPriorDict[2015] + 274

#usps state abbreviations by state name, for sources that spell the state out (eg the joint commission export)
usStateAbbreviations = {"alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA", "colorado": "CO",
                        "connecticut": "CT", "delaware": "DE", "district of columbia": "DC", "florida": "FL", "georgia": "GA",
                        "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
                        "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD", "massachusetts": "MA",
                        "michigan": "MI", "minnesota": "MN", "mississippi": "MS", "missouri": "MO", "montana": "MT",
                        "nebraska": "NE", "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
                        "new york": "NY", "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
                        "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC", "south dakota": "SD",
                        "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
                        "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY", "puerto rico": "PR", "guam": "GU",
                        "virgin islands": "VI", "u.s. virgin islands": "VI", "american samoa": "AS",
                        "northern mariana islands": "MP"}

#definition of which states (fips codes) belong to which region
usRegionFipsCodes = {"west":  ["04", "08", "16", "35", "30", "49", "32", "56", "02", "06", "15", "41", "53"],
                     "south": ["10", "11", "12", "13", "24", "37", "45", "51", "54", "01", "21", "28", "47", "05", "22", "40", "48"],
                     "midwest": ["18", "17", "26", "39", "55", "19", "20", "27", "29", "31", "38", "46"],
                     "northeast": ["09", "23", "25", "33", "44", "50", "34", "36", "42"]}

#joint commission stroke certifications in rank order, the generic words that make a site DBA name or a facility name
#non distinctive, and the google place types that count as a care site (see prep_jcAccreditationDF, get_nameTokens)
jcStrokeProgramRanking = ["comprehensive", "thrombectomy", "primary", "acute stroke ready", "stroke rehabilitation"]
jcGenericNameWords = ["hospital", "hospitals", "medical", "center", "centre", "general", "acute", "care", "the", "inc", "llc",
                      "lp", "ltd", "health", "healthcare", "system", "services", "regional", "community", "of", "and", "a", "an"]
jcPlaceSiteTypes = ["hospital", "medical_center", "medical_clinic", "health"]
nameGenericWords = jcGenericNameWords + ["campus", "hosp", "ctr", "med", "memorial", "st", "saint", "university", "county",
                                        "baptist", "methodist", "mercy", "providence"]

def get_daysInYearsPrior():
    return F.create_map([F.lit(x) for x in chain(*daysInYearsPriorDict.items())])

def get_monthsInYearsPrior():
    return F.create_map([F.lit(x) for x in chain(*monthsInYearsPriorDict.items())])

def add_column_prior(df, column, who, when, gapFill=None):
    '''Adds the column's value from the prior year, keyed by `who` and ordered by `when`.
    Generic across grains: `who` is the partition identity and `when` is the year column.
    `who` may be a single column name (a provider, for the stays grain) or a list of column
    names forming a composite key (the dyad pair fromORGNPINM+toORGNPINM, for the transfers
    dyad grain) -- the key must match the grain of `column` or the prior value will be the
    wrong row's. For some `who` the prior year would be 2 years prior because spark uses
    whatever row is lagging/behind the current one, so in that case the prior quantity is set
    to null (or to `gapFill`, see below). Domain modules wrap this with their own defaults
    (see cms.stays.add_column_prior and cms.transfers.add_column_prior).

    `gapFill` controls what a >1-year gap contributes. There are two ways the immediately
    prior year (when-1) can be absent from the data, and they are NOT the same:
      * `when` is the group's first observed year (no preceding row at all). We cannot tell
        "existed but had no activity" from "did not exist yet / outside our data window", so
        column+"Prior" is always null here regardless of gapFill.
      * `when` follows a >1-year gap (the group bills both before and after the gap, so it
        provably existed during it). For a COUNT/VOLUME column (e.g. providerSepticShockAnnualVolume)
        the missing year genuinely had a value of 0, so pass gapFill=0 to record that. For a
        PROPORTION or index column (e.g. nodeHhi, dyadProportionTransfers*) the missing year
        is undefined (0/0), so leave gapFill=None and the gap stays null.

    A null in column+"Prior" means the prior-year value is UNOBSERVED, not zero. Do not
    coalesce it to 0 downstream -- that would fabricate a real prior value where none exists.
    Use gapFill=0 here (at the source) instead of coalescing downstream so first-year nulls
    are preserved while genuine gap-year zeros are recorded.'''
    whoCols = who if isinstance(who, list) else [who]
    eachWho = Window.partitionBy(whoCols).orderBy(when)
    eachWhoWhen = Window.partitionBy(whoCols + [when])
    df = (df
          .withColumn("prior", F.lag(when,1).over(eachWho))
          .withColumn(column+"Prior", F.lag(column,1).over(eachWho))
          #exactly-1-year-back keeps the lagged value; a >1-year gap (when-prior>1) means the
          #intervening year(s) had no rows -> gapFill (0 for counts, null for proportions);
          #everything else -- same-year predecessor (when-prior==0) and the group's first row
          #(prior is null) -- must stay null so the max() broadcast below ignores it.
          .withColumn(column+"Prior", F.when( F.col(when)-F.col("prior")==1, F.col(column+"Prior"))
                                       .when( F.col(when)-F.col("prior")>1, F.lit(gapFill))
                                       .otherwise(F.lit(None)))
          #lag fires only on the row-1 of each (who,when) group; broadcast the prior value to every row in the group
          .withColumn(column+"Prior", F.max(F.col(column+"Prior")).over(eachWhoWhen))
          .drop("prior")) #scratch column used only to validate the lag is exactly 1 year
    return df

def get_filenames(pathToData, pathToAHAData, yearInitial, yearFinal):

    filenames = dict()

    #data exist only until 2020 (including), I copied the 2020 file to be used for 2021 and 2022
    #if data for 2021 and 2022 become available then I will need to also modify prep_sdohDF below
    #source: AHRQ social determinants of health database https://www.ahrq.gov/sdoh/data-analytics/sdoh-data.html
    filenames["sdoh"] = [pathToData + f'/SDOH/sdoh_year{year}.csv' for year in range(2016,2023)]

    #AHRQ compendium of US health systems: https://www.ahrq.gov/chsp/data-resources/compendium.html
    #data exist only for years 2016, 2018, 2020, 2021, 2022 I copied the 2016 data to 2017 and the 2018 data to 2019
    filenames["chspHosp"] = [pathToData + f'/CHSP/chsp-hospital-linkage-year{year}.csv' for year in range(2016,2023)]

    #case mix index from https://www.nber.org/research/data/centers-medicare-medicaid-services-cms-casemix-file-hospital-ipps
    filenames["cmi"] = [pathToData + f'/CASE-MIX-INDEX/casemix{year}.csv' for year in range(2016,2025)]

    filenames["aha"] = [pathToAHAData + f"/AHAAS Raw Data/FY{iYear} ASDB/COMMA/ASPUB" + f"{iYear}"[-2:] + ".CSV" for iYear in range(yearInitial,yearFinal+1)]

    # NPI numbers and other provider information obtained from CMS: https://download.cms.gov/nppes/NPI_Files.html
    filenames["npi"] = [pathToData + '/npidata_pfile_20050523-20220807.csv']

    # got this file from https://data.nber.org/data/cbsa-msa-fips-ssa-county-crosswalk.html
    # found it using some insight from https://resdac.org/cms-data/variables/county-code-claim-ssa
    #https://www.nber.org/research/data/census-core-based-statistical-area-cbsa-federal-information-processing-series-fips-county-crosswalk
    filenames["cbsa"] = [pathToData + '/cbsatocountycrosswalk.csv']

    # county level data were obtained from US Census Bureau
    # https://www.census.gov/cgi-bin/geo/shapefiles/index.php
    filenames["shpCounty"] = [pathToData + "/tl2021county/tl_2021_us_county.shp"]
    
    #county-level data from Plotly
    filenames["geojsonCounty"] = ['https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json']

    # data from USDA ERS
    # https://www.ers.usda.gov/data-products/atlas-of-rural-and-small-town-america/download-the-data/
    # https://www.ers.usda.gov/data-products/rural-urban-continuum-codes.aspx
    filenames["ersPeople"] = [pathToData + "/USDA-ERS/People.csv"]
    filenames["ersJobs"] = [pathToData + "/USDA-ERS/Jobs.csv"]
    filenames["ersIncome"] = [pathToData + "/USDA-ERS/Income.csv"]
    filenames["ersRucc"] = [pathToData + "/USDA-ERS/ruralurbancodes2013.csv"]

    # US Census data
    # This is how I got the data: https://www.youtube.com/watch?v=I6r-y_GQLfo
    # https://www.census.gov/data/developers/data-sets/acs-1year.html   #no I used the 5 year estimate see below
    # Exact query: 
    # https://api.census.gov/data/2021/acs/acs5/profile?get=NAME,DP03_0062E,DP03_0009PE,DP02_0068PE,DP05_0001E&for=county:*&in=state:39
    # I think you are only seeing partial data due to the type of estimate you are viewing. The American Community Survey (ACS) has 
    # 1-Year Estimates and 5-Year Estimates. Each type of estimate has their own table. To be included in a 1-Year Estimate Table, the geography 
    # must have 65,000 or more people. So, to see each county in Ohio, you should view the ACS 5-Year Estimates.
    # for examples: https://api.census.gov/data/2021/acs/acs1/profile/examples.html
    # for the codes: https://api.census.gov/data/2021/acs/acs1/profile/variables.html
    # https://api.census.gov/data/2021/acs/acs1/profile.html
    # the information above is about the first draft of the data for ohio, for the entire country I used:
    # https://api.census.gov/data/2015/acs/acs5?get=NAME,B19013_001E,B01001_001E&for=county:*&in=state:*
    filenames["census"] = [pathToData + f"/CENSUS/ACS5/acs5-year{year}.csv" for year in range(2016,2024)]

    # to calculate population density I need to use the Gazetteer file:
    #  https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.2020.html
    filenames["gazetteer2020"] = [pathToData + "/CENSUS/2020_Gaz_counties_national.csv"] 

    #beds, resident counts and rural/urban status per hospital per year, built once by get_hcrisDF below from the
    #raw HCRIS 2552-10 files that sit next to it in the same folder (the HOSP10FY{year} folders) -- see that
    #docstring for the form worksheet each measure comes from and for how a cost reporting period is mapped
    #to a calendar year.
    #this is a parquet rather than a csv because the raw files it is built from are ~8GB of csv per release,
    #which is why read_and_prep_dataframe reads this one key differently
    #CMS publishes its own year by year summary of these same cost reports, the hospital provider cost report
    #public use file, and that file is NOT enough here: it reports a single Number of Beds and never breaks
    #the beds out by unit, so providerHcrisBedsIcu and providerHcrisBedsCriticalCare exist only in the raw worksheet cells.
    #this parquet replaced the 2018 public use file the code used to read as hospCost2018, which supplied one
    #year of beds, residents and rural/urban status to claims of every year. The measures the two share were
    #checked against each other and agree exactly on every cost report both contain (2015-2023, ~35k reports),
    #so this parquet loses nothing by not being built from the public use file, and gains every other year.
    #Where they differ is vintage: the public use file is rebuilt
    #from a later HCRIS release, so for the most recent years it carries amended refilings of periods this
    #parquet holds under the original report number
    #the public use file is one zip per year at
    #https://data.cms.gov/provider-compliance/cost-reports/hospital-provider-cost-report/data
    #while the detailed files this parquet is built from, one zip per federal fiscal year, are at
    #https://www.cms.gov/data-research/statistics-trends-and-reports/cost-reports/cost-reports-fiscal-year
    filenames["hcris"] = [pathToData + '/HCRIS-COST-REPORTS/COST-REPORTS/hcris.parquet']

    # a CSV file that JB scraped from the CBI website: https://www.communitybenefitinsight.org
    # has hospital identifiers + hospital size + rural/urban + location + a couple other useful variables…
    # mostly, though, it has a bunch of financial details that aren’t terribly relevant to us. 
    # We could link in census data to get a sense of the socioeconomic region for hospitals as well...
    filenames["cbiHospitals"] = [pathToData + '/COMMUNITY-BENEFIT-INSIGHT/cbiHospitals.csv']
    filenames["cbiDetails"] = [pathToData + '/COMMUNITY-BENEFIT-INSIGHT/allHospitalsWithDetails.csv']

    # CAUTION: the NPI to CCN and CCN to NPI correspondence is NOT 1-to-1!! (for 1 CCN you have several NPIs and for 1 NPIs you have several CCNs....) 
    # I think that F.col("othpidty")=="6" will give the CCN number, in the description file below it is listed as Medicare OSCAR...
    # https://www.nber.org/research/data/national-provider-identifier-npi-medicare-ccn-crosswalk
    # https://data.nber.org/npi/desc/othpid/desc.txt for a description of what the variables in this file mean
    filenames["npiMedicareXw"] = [pathToData + "/npi_medicarexw.csv"]

    #https://www.huduser.gov/portal/datasets/usps_crosswalk.html#data
    #county column: 5 digit unique 2000 or 2010 Census county GEOID consisting of state FIPS + county FIPS.
    #zip codes split between counties are listed more than once and their ratios are shown
    filenames["zipToCounty"] = [pathToData + "/HUD/ZIP_COUNTY_122021.csv"]

    #dartmouth atlas zip to hsa to hrr crosswalks: https://data.dartmouthatlas.org/supplemental/
    #data exist only until 2019 (including), I copied the 2019 file (ZipHsaHrr19.csv) to ZipHsaHrr20.csv,...,ZipHsaHrr24.csv
    #to be used for 2020-2024, the copies are identical to the 2019 file so their zip column is still named zipcode19
    #if data after 2019 become available, replace the copies with the real files and extend the range below if needed
    #the 2015, 2016, 2017 files are published as xls, I converted them to csv with the zip codes as 5 character strings
    #(leading zeros kept) and renamed the zip column of the 2017 file from zipcode2017 to zipcode17
    filenames["zipToHrr"] = [pathToData + f'/DARTMOUTH-ATLAS/ZipHsaHrr{year-2000}.csv' for year in range(2015,2025)]

    pathMA = pathToData +'/MEDICARE-ADVANTAGE' 

    # https://resdac.org/articles/public-use-sources-managed-care-enrollment-and-penetration-rates
    # https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/MCRAdvPartDEnrolData/MA-State-County-Penetration
    # Rates are posted for all 12 months of the year, I chose July, because it is outside Medicare and MA enrollment periods and at the middle
    # of the non-enrollment periods
    #because there are several MA penetration rate files, put them in a dictionary
    filenames["maPenetration"] = [pathMA + f"/State_County_Penetration_MA_{iYear}_07/State_County_Penetration_MA_{iYear}_07_withYear.csv" for iYear in range(yearInitial,yearFinal+1)]

    #this set is for Medicare-registered hospitals only, the hospital ID is CCN (I checked) but the documentation does not state that
    #https://data.cms.gov/provider-data/dataset/xubh-q36u
    #in a quick test with inpatient stroke claims, this set was about 98.5% complete, using county names,
    #but for outpatient claims, this set was about 81% complete
    filenames["medicareHospitalInfo"] = [pathToData + "/Hospital_General_Information.csv"]

    #https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/provider-of-services-file-hospital-non-hospital-facilities
    #the parquet is built once by prep_posDF from the raw file PROVIDER-OF-SERVICES/POS_OTHER_DEC22.csv (same folder) and
    #carries the geocoded coordinates of the hospitals, see prep_posDF for how to rebuild it
    filenames["pos"] = [pathToData + "/PROVIDER-OF-SERVICES/pos.parquet"]
   
    #https://www.neighborhoodatlas.medicine.wisc.edu/
    #the 2023 ADI data is also available now (see my DATA folder), but I will need to implement the code for deciding which one to use
    #
    #In order to know with certainty how the national ranking is done and what the numbers mean, I used an address in an area that is 
    #fairly affluent and an address in an area that is fairly deprived.
    #I used the find geographies tab in 
    #https://urldefense.com/v3/__https://geocoding.geo.census.gov/geocoder/__;!!AU3bcTlGKuA!HiZeehzBHPgvoodNZF_XXWf9LNHwCueN_pVzexQ3xkzMcpLNMUsPp8V-PdnaNo2d8R5DmDvqLb4UjQfAiPme9qiE5VRF$ 
    #The link was:
    #https://urldefense.com/v3/__https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress?address=1690*20W*20Lane*20Ave*2C*20Columbus*20OH*2043221&benchmark=4&vintage=4__;JSUlJSUlJQ!!AU3bcTlGKuA!HiZeehzBHPgvoodNZF_XXWf9LNHwCueN_pVzexQ3xkzMcpLNMUsPp8V-PdnaNo2d8R5DmDvqLb4UjQfAiPme9v3QJHC4$ 
    #The block group was: 390490064301
    #The affluent area (1690 W Lane Ave, Columbus, OH 43221) had a national ranking of 26 and a state ranking of 1.
    #The link was:
    #https://urldefense.com/v3/__https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress?address=1975*20Cleveland*20Ave*2C*20Columbus*2C*20OH*2043211&benchmark=4&vintage=4__;JSUlJSUlJQ!!AU3bcTlGKuA!HiZeehzBHPgvoodNZF_XXWf9LNHwCueN_pVzexQ3xkzMcpLNMUsPp8V-PdnaNo2d8R5DmDvqLb4UjQfAiPme9lzqNl5Z$ 
    #The block group was: 390490007304
    #The deprived area had a national rank of 63 and a state rank of 4.
    #
    #Conclusion: Thus the higher the ranking (closest to 0, small numbers) the most affluent the area is or the least deprived.
    filenames["adi"] = [pathToData + "/ATLAS-DISCRIMINATION-INDEX/US_2020_ADI_CensusBlockGroup_v3.2.csv"]

    #have permission from AAMC to use this dataset for a single project only
    filenames["aamcHospitals"] = [pathToData + "/AAMC/teachingHospitalRequest-modifiedHeaders.csv"]

    #https://apps.acgme.org/ads/Public, I submitted a request to the data retrieval system and I got the data in an email
    filenames["acgmeSites"] = [pathToData + "/ACGME/ParticipatingSiteListingAY20212022.csv"]
    filenames["acgmePrograms"] = [pathToData + "/ACGME/ProgramListingAY20212022.csv"]

    #https://onlinelibrary.wiley.com/doi/10.1002/emp2.12673
    filenames["strokeCentersCamargo"] = [pathToData + "/CAMARGO-GROUP/2018_Stroke_CMS_2023apr-modifiedHeader.csv"]

    #joint commission website
    filenames["strokeCentersJC"] = [pathToData + "/JOINT-COMMISSION/StrokeCertificationList.csv"]
    #one row per CCN with its joint commission stroke certification, built by scripts/01-04 from the pos parquet and the
    #accredited organizations export of the joint commission website (the site level jcAccreditation.parquet those
    #scripts also write is an input of the match and an audit trail, not loaded here)
    filenames["ccnStrokeCertification"] = [pathToData + "/JOINT-COMMISSION/ccnStrokeCertification.parquet"]

    #hcup, procedure classes for ICD10
    #https://hcup-us.ahrq.gov/toolssoftware/procedureicd10/procedure_icd10.jsp?
    filenames["procedureClasses"] = [pathToData + "/HCUP/PClassR_v2026-1-modified.csv"]

    return filenames

#includes both dataframes and other non-spark data
def read_data(spark, filenames):

     data = dict()
     for file in list(filenames.keys()):
         if file in ["gazetteer2020","geojsonCounty"]:
             continue
         else:
            data[file] = map(lambda x: read_and_prep_dataframe(x, file, spark), filenames[file])
            data[file] = reduce(lambda x,y: x.unionByName(y,allowMissingColumns=True), data[file])

     data["gazetteer2020"] = spark.read.option("delimiter","\t").option("inferSchema", "true").csv(filenames["gazetteer2020"], header=True)
     with urlopen(filenames["geojsonCounty"][0]) as response:
        data["geojsonCounty"] = json.load(response)

     return data

def read_and_prep_dataframe(filename, file, spark):

    if file in ["hcris", "pos", "ccnStrokeCertification"]:
        return spark.read.parquet(filename)

    df = spark.read.csv(filename, header=True)

    if file=="npi":
        df = prep_npiProvidersDF(df)
    elif file=="maPenetration":
        df = prep_maPenetrationDF(df)
    elif file=="acgmeSites":
        df = prep_acgmeSitesDF(df)
    elif file=="acgmePrograms":
        df = prep_acgmeProgramsDF(df)
    elif file=="aamcHospitals":
        df = prep_aamcHospitalsDF(df)
    elif file=="strokeCentersCamargo":
        df = prep_strokeCentersCamargoDF(df)
    elif file=="strokeCentersJC":
        df = prep_strokeCentersJCDF(df)
    elif file=="zipToCounty":
        df = prep_zipToCountyDF(df)
    elif file=="zipToHrr":
        df = prep_zipToHrrDF(df, filename)
    elif file=="aha":
        df = prep_ahaDF(df, filename)
    elif file=="chspHosp":
        df = prep_chspHospDF(df, filename)
    elif file=="cmi":
        df = prep_cmiDF(df)
    elif file=="ersRucc":
        df = prep_ersRuccDF(df)
    elif file=="sdoh":
        df = prep_sdohDF(df, filename)
    elif file=="census":
        df = prep_censusDF(df, filename)
    elif (file=="adi"):
        df = prep_adiDF(df)
    elif file=="procedureClasses":
        df = prep_procedureClassesDF(df)
    return df   

def get_data(yearInitial, yearFinal, spark, pathToData='/users/PAS2164/deligkaris/DATA', pathToAHAData='/fs/ess/PAS2164/AHA', runTests=False):
    '''pathToData: where I keep all non-CMS data, pathToAHAData: where all AHA data are stored'''
    filenames = get_filenames(pathToData, pathToAHAData, yearInitial, yearFinal)
    data = read_data(spark, filenames)
    if runTests:
        run_data_tests(data)
    return data

def prep_procedureClassesDF(df):
    df = df.withColumn("ICD-10-PCS-CODE", F.regexp_replace(F.col("ICD-10-PCS-CODE"), "'", ""))
    return df

def prep_adiDF(adiDF):
    adiDF = (adiDF
              .withColumn("adiNatRank", F.when( F.col("ADI_NATRANK").isin("GQ-PH", "GQ", "PH", "QDI"), F.lit(None)).otherwise(F.col("ADI_NATRANK")))
              .withColumn("adiStaRank", F.when( F.col("ADI_STATERNK").isin("GQ-PH", "GQ", "PH", "QDI"), F.lit(None)).otherwise(F.col("ADI_STATERNK")))
              #add the categorical variables below because they are useful in analyses sometimes
              .withColumn("adiNatRankGroup", F.when( F.col("adiNatRank")<=25, 4 ).when( F.col("adiNatRank")<=50, 3 )
                                              .when( F.col("adiNatRank")<=75, 2 ).when( F.col("adiNatRank")<=100, 1 ))
              .withColumn("adiStaRankGroup", F.when( F.col("adiStaRank")<=25, 4 ).when( F.col("adiStaRank")<=50, 3 )
                                              .when( F.col("adiStaRank")<=75, 2 ).when( F.col("adiStaRank")<=100, 1 )))
    return adiDF

def prep_ersRuccDF(ersRuccDF):
    ersRuccDF = (ersRuccDF.withColumn('RUCC_2013', F.col("RUCC_2013").cast('int'))
                          .withColumn('ruccGroup', F.when( F.col("RUCC_2013")<=3, 0) #the number of categories is too large for analyses so do a group
                                                    .when( F.col("RUCC_2013")<=6, 1)
                                                    .when( F.col("RUCC_2013")<=9, 2)))
    return ersRuccDF

def prep_sdohDF(sdohDF, filename):
    #because I copied the 2020 sdoh file for 21 and 22, the year column is not accurate (for 21 and 22), so I need to overwrite it here
    year = int(re.compile(r'year\d{4}').search(filename).group()[4:])

    #U.S. Bureau of Economic Analysis, "Table 1.1.9. Implicit Price Deflators for Gross Domestic Product" (accessed Friday, July 25, 2025).
    #table can be found in DATA/BEA
    gdpDeflator = {2014: 96.421,
                   2015: 97.316,
                   2016: 98.241,
                   2017: 100.000,
                   2018: 102.291,
                   2019: 103.979,
                   2020: 105.361,
                   2021: 110.172,
                   2022: 118.026,
                   2023: 122.273,
                   2024: 125.230}
 
    deflator = gdpDeflator[2024]/gdpDeflator[year] #standardize to 2024 incomes

    #because I copied the 2020 data to 2021 and 2022 I will adjust the income values...
    adjustment2021And2022 = {2014: 1, 2015: 1, 2016: 1, 2017: 1, 2018: 1, 2019: 1, 2020: 1, 
                             2021: gdpDeflator[2021]/gdpDeflator[2020],
                             2022: gdpDeflator[2022]/gdpDeflator[2020]}

    adjustment = adjustment2021And2022[year]

    sdohDF = (sdohDF.withColumn("ACS_MEDIAN_HH_INC", F.col("ACS_MEDIAN_HH_INC").cast('int'))
                    .withColumn("medianHhIncomeAdjusted", F.col("ACS_MEDIAN_HH_INC")*deflator*adjustment)
                    #these columns do not exist in the data file of the last year (2020) so for now I am excluding them
                    #.withColumn("AHRF_TOT_NEUROLOGICAL_SURG", F.col("AHRF_TOT_NEUROLOGICAL_SURG").cast('int'))
                    #.withColumn("CDCA_HEART_DTH_RATE_ABOVE35", F.col("CDCA_HEART_DTH_RATE_ABOVE35").cast('float'))
                    #.withColumn("CDCA_PREV_DTH_RATE_BELOW74", F.col("CDCA_PREV_DTH_RATE_BELOW74").cast('float'))
                    #.withColumn("CDCA_STROKE_DTH_RATE_ABOVE35", F.col("CDCA_STROKE_DTH_RATE_ABOVE35").cast('float'))
                    #.withColumn("HIFLD_MEDIAN_DIST_UC", F.col("HIFLD_MEDIAN_DIST_UC").cast('float'))
                    .withColumn("POS_MEDIAN_DIST_ED", F.col("POS_MEDIAN_DIST_ED").cast('float'))
                    .withColumn("POS_MEDIAN_DIST_MEDSURG_ICU", F.col("POS_MEDIAN_DIST_MEDSURG_ICU").cast('float'))
                    .withColumn("POS_MEDIAN_DIST_TRAUMA", F.col("POS_MEDIAN_DIST_TRAUMA").cast('float'))
                    .withColumn("year", F.lit(year).cast('int')))
    return sdohDF

def prep_censusDF(censusDF, filename):
    year = int(re.compile(r'year\d{4}').search(filename).group()[4:])
    censusDF = (censusDF.withColumn("year", F.lit(year).cast('int')) 
                        .withColumn("medianHouseholdIncome", F.col("B19013_001E").cast('int'))
                        .withColumn("totalPopulation", F.col("B01001_001E").cast('int'))
                        .withColumnRenamed("state", "fipsState")
                        .withColumn("fipsCounty", F.concat(F.col("fipsState"), F.col("county")))
                        .drop("county"))
    return censusDF

def prep_chspHospDF(chspHospDF, filename):
    chspYear = int(re.compile(r'year\d{4}').search(filename).group()[4:])
    chspHospDF = (chspHospDF.withColumn("year", F.lit(chspYear)))
    #for reasons unknown, the same CCN, 104079, appears in 2 lines in this file with two different compendium_hospital_id, and all else the same
    #I filter out the line with the ID that is not used at later years
    chspHospDF = chspHospDF.filter(F.col("compendium_hospital_id")!="CHSP00008136")
    #The health_sys_id variable indicates whether a hospital is part of a health system or not.
    #If this variable is Null then the hospital is not part of a health system.
    #eg see page 21 of https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/2021-hospital-linkage-techdoc-rev.pdf
    #There are 4073 non null health sys id values in the chspHospDF and 2652 null values consistent with the Table IV.1 in the PDF.
    #I am referring just to hospitals because the chspHospDF is the hospital linkage file from AHRQ
    chspHospDF = chspHospDF.withColumn("isVI", F.when( F.col("health_sys_id").isNotNull(), F.lit(1)).otherwise(F.lit(0))) 
    return chspHospDF

def prep_cmiDF(cmiDF):
    cmiDF = (cmiDF.withColumn("cases", F.col("cases").cast('int'))
                  .withColumn("casemixindex", F.col("casemixindex").cast('double'))
                  .withColumn("sumcasemix", F.col("sumcasemix").cast('double'))
                  .withColumn("year", F.col("year").cast('int')))
    return cmiDF

def prep_ahaDF(ahaDF, filename):
    #note: some AHA columns are coded as 0=no, 1=yes, some are 2=no, 1=yes.....
    ahaYear = int(re.compile(r'FY\d{4}').search(filename).group()[2:])
    #include a column so that I know which year the data was from, need this when I union the aha data from several years
    ahaDF = (ahaDF.withColumn("year", F.lit(ahaYear))
                  .withColumn("ahaACGME", F.col("MAPP3").cast('int'))        #one or more ACGME programs
                  .withColumn("ahaACGME", F.when( F.col("ahaACGME")==2, 0).otherwise(F.col("ahaACGME")))
                  .withColumn("ahaMedSchoolAff", F.col("MAPP5").cast('int')) #medical school affiliation
                  .withColumn("ahaMedSchoolAff", F.when( F.col("ahaMedSchoolAff")==2, 0).otherwise(F.col("ahaMedSchoolAff")))
                  .withColumn("ahaCOTH", F.col("MAPP8").cast('int'))         #member of COTH
                  .withColumn("ahaCOTH", F.when( F.col("ahaCOTH")==2, 0).otherwise(F.col("ahaCOTH")))
                  .withColumn("ahaCah", F.col("MAPP18").cast('int'))         #critical access hospital
                  .withColumn("ahaCah", F.when( F.col("ahaCah")==2, 0).otherwise(F.col("ahaCah")))
                  .withColumn("ahaTotalMinusNursingBeds", F.col("BDH").cast('int'))           #total facility beds - nursing home beds, ton of missingness, useless
                  .withColumn("ahaTotalHospitalBeds", F.col("HOSPBD").cast('int')) #total hospital beds
                  .withColumn("ahaSize", F.when( F.col("ahaTotalMinusNursingBeds").isNull(), F.lit(None))
                                          .when( F.col("ahaTotalMinusNursingBeds")<100, 0)
                                          .when( (F.col("ahaTotalMinusNursingBeds")>=100)&(F.col("ahaTotalMinusNursingBeds")<400), 1)
                                          .when( F.col("ahaTotalMinusNursingBeds")>=400, 2)
                                          .otherwise(F.lit(None)))
                  .withColumn("FTERES", F.col("FTERES").cast('int'))         #full time equivalent residents and interns
                  .withColumn("LAT", F.col("LAT").cast('double'))
                  .withColumn("LONG", F.col("LONG").cast('double'))
                  .withColumn("ahaResidentToBedRatio", F.col("FTERES")/F.col("ahaTotalMinusNursingBeds"))
                  .withColumn("ahaBedsIcu", F.col("MSICBD").cast('int')) #number of medical/surgical intensive care beds
                  .withColumn("ahaIcuHos", F.col("MSICHOS").cast('int'))
                  #NIS definition of teaching hospitals: https://hcup-us.ahrq.gov/db/vars/hosp_teach/nisnote.jsp
                  #the definition was somewhat unclear so I asked for clarification, see email on 7/25/2024:
                  #A hospital is considered to be a teaching hospital if it met any one of the following three criteria:
                  # Residency training approval by the Accreditation Council for Graduate Medical Education (ACGME)
                  # Membership in the Council of Teaching Hospitals (COTH)
                  # A ratio of full-time equivalent interns and residents to beds of .25 or higher.
                  .withColumn("ahaNisTeachingHospital", 
                              F.when( (F.col("ahaCOTH")==1) | (F.col("ahaACGME")==1) | (F.col("ahaResidentToBedRatio")>=0.25) , 1)
                               .otherwise(0))
                  .withColumn("ahaCbsaType", F.when( F.col("CBSATYPE")=="Metro", 0)
                                              .when( F.col("CBSATYPE")=="Micro", 1)
                                              .when( F.col("CBSATYPE")=="Rural", 2)
                                              .otherwise(F.lit(None)))
                  .withColumn("CNTRL", F.col("CNTRL").cast('int'))
                  #0: public (government federal or non-federal), 1: not for profit, 2: for profit
                  .withColumn("ahaOwner", F.when( F.col("CNTRL").isin([12,13,14,15,16]), 0) #government, non-federal
                                           .when( F.col("CNTRL").isin([21,23]), 1)          #non-government, not-for-profit
                                           .when( F.col("CNTRL").isin([31,32,33]), 2)       #investor-owned, for-profit
                                           .when( F.col("CNTRL").isin([40,41,42,43,44,45,46,47,48]), 3) #government, federal
                                           .otherwise(F.lit(None)))
                  #sometimes there are very few government federal or non-federal claims and it helps to group the tiny category with the other one
                  .withColumn("ahaOwnerGroup", F.when( F.col("ahaOwner").isin(0,3), 0) #government
                                                .otherwise( F.col("ahaOwner") ))
                  #system member, if SYSID is not blank then this is 1
                  #but in practice MHSMEMB is either null, or 1 or 8 or 2 (counts = 6149, 12411, 2, 5 respectively for 2016)
                  #I will set this to 1 only when MHSMEMB is 1 as it is supposed to be 
                  #also, the related SYSID variable is only the last 4 digits of the AHA health care system identifier so 
                  #there is some uncertainty if the same SYSID really means the same health system
                  .withColumn("MHSMEMB", F.col("MHSMEMB").cast('int'))
                  .withColumn("ahaSystemMember", F.when( F.col("MHSMEMB")==1, F.lit(1) ).otherwise(F.lit(0))))

    if ahaYear > 2016:
        ahaDF = (ahaDF.withColumn("STRCHOS", F.col("STRCHOS").cast('int'))
                      .withColumn("STRCSYS", F.col("STRCSYS").cast('int'))
                      .withColumn("STRCVEN", F.col("STRCVEN").cast('int'))
                      .withColumn("EICUHOS", F.col("EICUHOS").cast('int'))
                      .withColumn("EICUSYS", F.col("EICUSYS").cast('int'))
                      .withColumn("EICUVEN", F.col("EICUVEN").cast('int'))
                      #teleICU: hospital, system, or vendor provided; already coded 0=no, 1=yes
                      .withColumn("ahaTeleicuHos", F.col("EICUHOS"))
                      .withColumn("ahaTeleicuSys", F.col("EICUSYS"))
                      .withColumn("ahaTeleicuVen", F.col("EICUVEN")))

    return ahaDF

def get_cbus_metro_ssa_counties():
    # definition of columbus metro area counties according to US Census bureau 
    # source: https://obamawhitehouse.archives.gov/sites/default/files/omb/bulletins/2013/b13-01.pdf (page 29)
    # city of Columbus may have a different definition
    return ["36250", "36210", "36230", "36460", "36500", "36660", "36810", "36650", "36600","36380"]

def prep_zipToCountyDF(zipToCountyDF):
    #note: the method used below to assign a signle fips county code to a zip code should only be used as a last resort when all else has failed...
    eachZip = Window.partitionBy("zip")
    zipToCountyDF = (zipToCountyDF.withColumn("maxBusRatio",
                                              F.max(F.col("bus_ratio")).over(eachZip))
                                  .withColumn("countyOfMaxBusRatio",
                                              F.when( F.col("maxBusRatio")==F.col("bus_ratio"), 1) #more than 1 counties per zip can be that county
                                               .otherwise(0))
                                  .withColumn("numberOfCountiesOfMaxBusRatio",
                                              F.sum( F.col("countyOfMaxBusRatio")).over(eachZip))
                                  .withColumn("maxTotRatio",
                                              F.max(F.col("tot_ratio")).over(eachZip))
                                  .withColumn("countyOfMaxTotRatio",
                                              F.when( F.col("maxTotRatio")==F.col("tot_ratio"), 1) #more than 1 counties per zip can be that county
                                               .otherwise(0))
                                  .withColumn("numberOfCountiesOfMaxTotRatio",
                                              F.sum( F.col("countyOfMaxTotRatio")).over(eachZip))
                                  .withColumn("minFips",
                                              F.min(F.col("county").cast('double')).over(eachZip)) #when all else fails, just choose one deterministically
                                  .withColumn("countyOfMinFips",
                                              F.when( F.col("county")==F.col("minFips"), 1)
                                               .otherwise(0)))

    zipToCountyDF = (zipToCountyDF.withColumn("countyForZip",
                                              F.when( 
                                                  (F.col("numberOfCountiesOfMaxBusRatio")==1) & #number one choice: county with most businesses
                                                  (F.col("countyOfMaxBusRatio")==1), 1)
                                               .when( (F.col("numberOfCountiesOfMaxTotRatio")==1) & #number two choice: county with most bus+res+other
                                                  (F.col("numberOfCountiesOfMaxBusRatio")>1) &
                                                  (F.col("countyOfMaxTotRatio")==1), 1)
                                               .when( (F.col("numberOfCountiesOfMaxBusRatio")>1) &
                                                  (F.col("numberOfCountiesOfMaxTotRatio")>1) &
                                                  (F.col("countyOfMinFips")==1), 1)             #number three choice: county with the min fips 
                                               .otherwise(0)))

    return zipToCountyDF

def prep_zipToHrrDF(zipToHrrDF, filename):
    '''Dartmouth Atlas zip to hospital service area (hsa) to hospital referral region (hrr) crosswalk, one file per year.
    The zip column is named after the year of the file (zipcode15, zipcode19,...) so it is renamed to zip to allow the
    yearly files to be unioned. The year is taken from the filename and not from that column name because the 2019 file
    was copied for the years after 2019, and those copies still carry the zipcode19 header.'''
    year = 2000 + int(re.compile(r'ZipHsaHrr(\d{2})').search(filename).group(1))
    zipColumn = [c for c in zipToHrrDF.columns if re.fullmatch(r'zipcode\d+', c)][0]
    zipToHrrDF = (zipToHrrDF.withColumnRenamed(zipColumn, "zip")
                            .withColumn("zip", F.lpad(F.col("zip"), 5, "0"))
                            .withColumn("hsanum", F.col("hsanum").cast('int'))
                            .withColumn("hrrnum", F.col("hrrnum").cast('int'))
                            .withColumn("year", F.lit(year)))
    return zipToHrrDF

def prep_maPenetrationDF(maPenetrationDF):
    maPenetrationDF = (maPenetrationDF.withColumn("Penetration", F.split( F.trim(F.col("Penetration")), r'\.' ).getItem(0).cast('int') )
                                      .withColumn("Year", F.col("Year").cast('int')))
    return maPenetrationDF

def get_hcrisDF(spark, pathToHcris, yearInitial=2015, yearFinal=2026, filename=None, lastCompleteYear=2024):
    '''Bed counts, bed days, interns and residents, resident to bed ratio and rural status per hospital per year from the
    HCRIS hospital 2552-10 cost reports: one row per (PRVDR_NUM, hcrisYear) with providerHcrisBeds{Icu,CriticalCare,Total},
    providerHcrisBedDays{Icu,CriticalCare,Total}, providerHcrisResidents, providerHcrisResidentToBedRatio,
    providerHcrisIsRural, hcrisReportDays. Run once offline with filename to write the parquet get_data loads
    (filenames["hcris"]); the return value is the uncomputed plan over ~8GB of csv, validate against the parquet.
    pathToHcris holds HOSP10FY{year}/HOSP10_{year}_{rpt,nmrc}.csv as CMS distributes them
    (https://downloads.cms.gov/files/hcris/hosp10fy{year}.zip). Cell positions follow the Provider Reimbursement Manual
    Part 2 chapter 40 (https://www.cms.gov/regulations-and-guidance/guidance/manuals/paper-based-manuals-items/cms021935):
    blank worksheets https://www.cms.gov/files/document/r23p240f.pdf (S-3 Part I p. 40-511, S-2 Part I p. 40-504),
    instructions https://www.cms.gov/files/document/r18p240ipdf.pdf (S-3 Part I sec. 4005.1 p. 40-53, lines 8-14 p. 40-57;
    S-2 Part I sec. 4004.1; E Part A sec. 4030.1 p. 40-170.1 and 40-170.5; Table 2/3 of sec. 4095 p. 40-719.1/40-769).
    Page numbers are the 40-NNN printed on the page, which survive a change of transmittal.'''

    #only rpt and nmrc are read; alpha holds cost center labels, which the S-3 unit lines do not use (HOSP2010_README 5.2)
    rptFiles = [pathToHcris + f"/HOSP10FY{year}/HOSP10_{year}_rpt.csv" for year in range(yearInitial, yearFinal+1)]
    nmrcFiles = [pathToHcris + f"/HOSP10FY{year}/HOSP10_{year}_nmrc.csv" for year in range(yearInitial, yearFinal+1)]

    rptDF = (spark.read.schema(hcrisRptSchema).csv(rptFiles)
                  .select(F.col("RPT_REC_NUM"),
                          F.col("PRVDR_NUM"),
                          F.col("RPT_STUS_CD"),
                          F.to_date(F.col("FY_BGN_DT"), "MM/dd/yyyy").alias("FY_BGN_DT"),
                          F.to_date(F.col("FY_END_DT"), "MM/dd/yyyy").alias("FY_END_DT")))

    #hcrisYear is the calendar year of the midpoint of the reporting period, not the folder's fiscal year: HOSP10FY2019
    #holds periods beginning 10/2018-09/2019 and ending anywhere in 2018-2020. The midpoint puts each report in exactly one
    #calendar year, joinable to the claims on THRU_DT_YEAR like the AHA data. The edge years are thin as a result
    rptDF = (rptDF.withColumn("hcrisReportDays", F.datediff(F.col("FY_END_DT"), F.col("FY_BGN_DT")) + 1)
                  .withColumn("hcrisYear",
                              F.year(F.date_add(F.col("FY_BGN_DT"),
                                                (F.datediff(F.col("FY_END_DT"), F.col("FY_BGN_DT"))/2).cast('int')))))

    #nmrc is one row per worksheet cell keyed by (RPT_REC_NUM, WKSHT_CD, LINE_NUM, CLMN_NUM), LINE_NUM and CLMN_NUM are
    #fixed width zero padded strings so ranges are string comparisons. Beds: Worksheet S-3 Part I column 2 (beds available
    #at the END of the period, 42 CFR 412.105(b)), column 3 bed days available (beds times days in the period); lines 8-13
    #are intensive care, coronary care, burn intensive care, surgical intensive care, other special care, nursery, line 14
    #the form's own Total (lines 7-13, so adults and pediatrics and the nursery too). The units are not cost center coded,
    #so the line number is used, and as a block (00800-00899, ...) because a hospital with several units of a kind
    #subscripts the line (00801, 00802, ... up to 00850 seen). Residents: S-3 Part I line 27 column 9, Interns & Residents
    #FTEs for the whole facility (line 14 is the hospital component alone and disagrees with the public use file on 31 of
    #170 teaching hospitals, line 27 agrees on all). Rural: S-2 Part I line 26 column 1, geographic classification at the
    #BEGINNING of the period, 1 urban 2 rural (line 27, the end of period one, is not read). Resident to bed ratio:
    #Worksheet E Part A (E00A18A per Table 2) line 19 column 1, the IME ratio CMS computes: line 18 (three year average
    #of capped allowable FTEs plus new programs) over line 4 (bed days net of swing, observation, hospice, labor and
    #delivery and COVID days, per day), so NOT providerHcrisResidents over providerHcrisBedsTotal; it runs a median 7%
    #below that quotient. All positions confirmed against CostReport_2019_Final.csv report by report (beds, bed days,
    #rural on all 2039 shared reports; E Part A lines 3, 29, 33 on every report that files them)
    isBedLine = ((F.col("WKSHT_CD")=="S300001") &
                 (F.col("LINE_NUM").between("00800","01299") | (F.col("LINE_NUM")=="01400")))
    isBedCell = isBedLine & (F.col("CLMN_NUM")=="00200")
    isBedDaysCell = isBedLine & (F.col("CLMN_NUM")=="00300")
    isResidentsCell = ((F.col("WKSHT_CD")=="S300001") & (F.col("LINE_NUM")=="02700") & (F.col("CLMN_NUM")=="00900"))
    isRuralCell = ((F.col("WKSHT_CD")=="S200001") & (F.col("LINE_NUM")=="02600") & (F.col("CLMN_NUM")=="00100"))
    isResidentToBedRatioCell = ((F.col("WKSHT_CD")=="E00A18A") & (F.col("LINE_NUM")=="01900") & (F.col("CLMN_NUM")=="00100"))

    #icu is the intensive care block alone, critical care all five blocks (so icu is a subset of it); the range stops at
    #01299 because line 13 is the nursery; the total is read from line 14 rather than summed
    icuBlock = F.col("LINE_NUM").between("00800","00899")
    criticalCareBlock = F.col("LINE_NUM").between("00800","01299")
    totalLine = (F.col("LINE_NUM")=="01400")

    def sum_cells(cell, block):
        return F.sum(F.when(cell & block, F.col("ITM_VAL_NUM")))

    #bed days are long rather than int: a garbage cell there is 365 times a garbage bed count, and one provider has filed
    #a value a third of the way to the int limit. They measure a year of capacity where the counts measure the last day
    cellsDF = (spark.read.schema(hcrisNmrcSchema).csv(nmrcFiles)
                    .filter(isBedCell | isBedDaysCell | isResidentsCell | isRuralCell | isResidentToBedRatioCell)
                    .groupBy("RPT_REC_NUM")
                    .agg(sum_cells(isBedCell, icuBlock).cast('int').alias("providerHcrisBedsIcu"),
                         sum_cells(isBedCell, criticalCareBlock).cast('int').alias("providerHcrisBedsCriticalCare"),
                         sum_cells(isBedCell, totalLine).cast('int').alias("providerHcrisBedsTotal"),
                         sum_cells(isBedDaysCell, icuBlock).cast('long').alias("providerHcrisBedDaysIcu"),
                         sum_cells(isBedDaysCell, criticalCareBlock).cast('long').alias("providerHcrisBedDaysCriticalCare"),
                         sum_cells(isBedDaysCell, totalLine).cast('long').alias("providerHcrisBedDaysTotal"),
                         F.max(F.when(isResidentsCell, F.col("ITM_VAL_NUM"))).alias("providerHcrisResidents"),
                         F.max(F.when(isResidentToBedRatioCell, F.col("ITM_VAL_NUM"))).alias("providerHcrisResidentToBedRatio"),
                         (F.max(F.when(isRuralCell, F.col("ITM_VAL_NUM")))==2).cast('int').alias("providerHcrisIsRural")))

    #nothing validates the bed count at filing and a few providers file something else in it (360044 filed 1594784 beds
    #for FY2020 against bed days for 40, and 36-63 every other year; the public use file carries the same value). Every
    #such report has a sane bed days cell beside it, so a count more than 5 times the beds implied by the bed days is
    #replaced by the implied count: 26 of 59553 reports in 2015-2024, 0.04%, but the whole of the tail a comparison with
    #AHA turns up. One sided because a count far BELOW its bed days is the bed days cell being wrong; 5 times because
    #the two legitimately differ by about 2 when beds change during the period (100079: 524 beds, bed days for 325, then
    #524 against 524). Not fired when the bed days imply less than one bed (absent, 0, or below the days in the period):
    #040011 filed 41 beds against 6 bed days and 41 is the credible one; this also keeps a replacement from being 0,
    #which downstream would read as no unit (020026 files 1 other special care bed against 1 bed day every year)
    def beds_checked_against_bed_days(beds, bedDays):
        impliedBeds = F.col(bedDays)/F.col("hcrisReportDays")
        return (F.when((impliedBeds>=1) & (F.col(beds) > 5*impliedBeds), F.round(impliedBeds))
                 .otherwise(F.col(beds)).cast('int'))

    #the most recent fiscal years are still being filed (September 2026 release: 3508 reports for calendar 2025 against
    #5900-6000 for 2016-2024), so an incomplete year is dropped rather than mistaken for one where the rest filed nothing;
    #raise lastCompleteYear as CMS fills in, None keeps everything. The thin year before yearInitial is kept
    if lastCompleteYear is not None:
        rptDF = rptDF.filter(F.col("hcrisYear") <= lastCompleteYear)

    #a report with S-3 Part I but no intensive care line has no such unit: icu and critical care beds and bed days are 0,
    #not the null a sum over unfiled lines returns; residents 0 on the same reasoning (an empty cell is no program).
    #The total beds, total bed days and rural cells stay null when absent, 0 not being a real value for them. Reports
    #with no bed cell at all (~1% per year) are dropped rather than recorded as having no beds; they may still carry
    #the S-2 and resident cells, which is why the bed columns and not the row decide. The IME ratio is absent for every
    #hospital not paid under IPPS or without residents: 0 where there are no residents (a ratio of 0 whatever the
    #hospital), null where there are (a CAH or other non IPPS teaching hospital, the ratio exists but is not computed):
    #4620 zeros and 200 nulls of 6048 reports in 2019
    hcrisDF = (rptDF.join(cellsDF, on="RPT_REC_NUM", how="inner")
                    .withColumn("providerHcrisBedsIcu",
                                beds_checked_against_bed_days("providerHcrisBedsIcu","providerHcrisBedDaysIcu"))
                    .withColumn("providerHcrisBedsCriticalCare",
                                beds_checked_against_bed_days("providerHcrisBedsCriticalCare","providerHcrisBedDaysCriticalCare"))
                    .withColumn("providerHcrisBedsTotal",
                                beds_checked_against_bed_days("providerHcrisBedsTotal","providerHcrisBedDaysTotal"))
                    .filter(F.col("providerHcrisBedsCriticalCare").isNotNull() | F.col("providerHcrisBedsTotal").isNotNull())
                    .fillna(0, subset=["providerHcrisBedsIcu","providerHcrisBedsCriticalCare","providerHcrisResidents",
                                       "providerHcrisBedDaysIcu","providerHcrisBedDaysCriticalCare"])
                    .withColumn("providerHcrisResidentToBedRatio",
                                F.when(F.col("providerHcrisResidentToBedRatio").isNull() & (F.col("providerHcrisResidents")==0), F.lit(0.0))
                                 .otherwise(F.col("providerHcrisResidentToBedRatio"))))

    #about 1.4% of (PRVDR_NUM, hcrisYear) pairs have two reports, a change of ownership or of fiscal year splitting the
    #year; the longest period wins, highest RPT_REC_NUM breaks ties, and hcrisReportDays is kept so short periods can be dropped
    eachProviderYear = (Window.partitionBy("PRVDR_NUM","hcrisYear")
                              .orderBy(F.col("hcrisReportDays").desc(), F.col("RPT_REC_NUM").desc()))

    hcrisDF = (hcrisDF.withColumn("hcrisReportRank", F.row_number().over(eachProviderYear))
                      .filter(F.col("hcrisReportRank")==1)
                      .drop("hcrisReportRank"))

    if filename is not None:
        hcrisDF.coalesce(1).write.mode("overwrite").parquet(filename)

    return hcrisDF

def add_posHospital(posDF):
    '''posHospital: 1 for POS rows that are hospitals of any kind (see add_posHospitalType), 0 otherwise.'''
    #PRVDR_CTGRY_CD 01 is every Medicare certified hospital; the other codes are SNFs, home health, hospices, ...
    posDF = posDF.withColumn("posHospital", F.when(F.col("PRVDR_CTGRY_CD")=="01", 1).otherwise(0))
    return posDF

def add_posHospitalType(posDF):
    '''posHospitalType: the kind of hospital, from the CCN range CMS assigns (State Operations Manual ch. 2 sec. 2779A1,
    https://www.cms.gov/Regulations-and-Guidance/Guidance/Manuals/downloads/som107c02.pdf); null for non-hospitals.'''
    #the last four characters of the CCN carry the type, the first two are the state
    tail = F.substring(F.col("PRVDR_NUM"), 3, 4)
    number = F.when(tail.rlike("^[0-9]{4}$"), tail.cast("int"))
    posDF = posDF.withColumn("posHospitalType",
                             F.when(F.col("posHospital") != 1, F.lit(None))
                              .when(tail.endswith("E"), "emergency")      #nonparticipating non-Federal emergency hospital (sec. 2052)
                              .when(tail.endswith("F"), "federal")        #nonparticipating Federal emergency hospital, eg VA
                              .when(number.between(1, 879), "acute")      #short term general and specialty hospitals
                              .when(number.between(1300, 1399), "cah")
                              .when(number.between(2000, 2299), "ltch")
                              .when(number.between(3025, 3099), "rehabilitation")
                              .when(number.between(3300, 3399), "childrens")
                              .when(number.between(4000, 4499), "psychiatric")
                              .when(number.between(9800, 9899), "transplant") #a hospital's transplant program, alongside its own CCN
                              .otherwise("other"))
    return posDF

def prep_posDF(posDF, pathToData=None, filename=None, maxCalls=None, keyFilename=None, activeOnly=False):
    '''Builds the POS parquet that get_data loads (filenames["pos"]) from the raw csv, see scripts/01_geocode_pos.py.
    Adds posHospital, posHospitalType, posCah, posShortTerm, posIsRural, posActive, providerFIPS, providerStateFIPS,
    FAC_NAMEProcessed, posZip, posAddress and, with pathToData, the geocoded coordinates of the hospitals (posLat, posLng,
    posGeocode*). All rows are kept. Layout:
    https://data.cms.gov/sites/default/files/2022-10/58ee74d6-9221-48cf-b039-5b7a773bf39a/Layout%20Sep%2022%20Other.pdf'''
    posDF = add_posHospital(posDF)
    posDF = (posDF.withColumn("providerStateFIPS", F.col("FIPS_STATE_CD"))
                  .withColumn("providerFIPS",F.concat( F.col("FIPS_STATE_CD"),F.col("FIPS_CNTY_CD")))
                  .withColumn("posCah", F.when( (F.col("posHospital")==1) &  (F.col("PRVDR_CTGRY_SBTYP_CD")=="11"), 1).otherwise(0))
                  .withColumn("posShortTerm",  F.when( (F.col("posHospital")==1) & (F.col("PRVDR_CTGRY_SBTYP_CD")=="01"), 1).otherwise(0))
                  .withColumn("posIsRural", F.when( F.col("CBSA_URBN_RRL_IND")=="R", F.lit(1))
                                             .when( F.col("CBSA_URBN_RRL_IND")=="U", F.lit(0))
                                             .otherwise(F.lit(None))) #there are nulls but also "001" and "041" (very few though)
                  #the file keeps every provider that ever had a CCN, 00 is the code of the ones still active
                  .withColumn("posActive", F.when( F.col("PGM_TRMNTN_CD")=="00", 1).otherwise(0)))
    posDF = add_processed_name(posDF,colToProcess="FAC_NAME")
    posDF = add_posHospitalType(posDF)
    posDF = posDF.withColumn("posZip", F.substring(F.trim(F.col("ZIP_CD")), 1, 5))
    posDF = add_address(posDF, "ST_ADR", "CITY_NAME", "STATE_CD", "posZip", "posAddress")
    if pathToData is not None:
        #only hospitals are geocoded, every address not in the cache is a paid call; the other provider types keep nulls
        geocodeCols = [field.name for field in get_geocode_schema("pos").fields if field.name != "address"]
        toGeocode = (F.col("posHospital")==1) & ((F.col("posActive")==1) | F.lit(not activeOnly))
        hospitalsDF = add_geocode_info(posDF.filter(toGeocode).select("PRVDR_NUM", "posAddress"),
                                       "posAddress", pathToData, "pos", maxCalls=maxCalls, keyFilename=keyFilename)
        posDF = posDF.join(hospitalsDF.select("PRVDR_NUM", *geocodeCols), on=["PRVDR_NUM"], how="left_outer")
    if filename is not None:
        posDF.coalesce(1).write.mode("overwrite").parquet(filename)
    return posDF

def prep_aamcHospitalsDF(aamcHospitalsDF):
    #Email communication with AAMC:
    #The AAMC member teaching hospitals represent a small portion of all teaching hospitals in the US – 
    #there are approximately 1000 hospitals that offers some form of graduate medical education. 
    #Teaching hospital “major/minor” is commonly defined by the intern-resident-bed ratio (IRB).  
    #Major teaching hospitals are those with an IRB GE 0.25 (so, in a nutshell, one resident for every four staffed beds); 
    #minor teaching hospitals are those with an IRB GT 0 but LT 0.25, and all other hospitals are non-teaching.  
    #This data is only captured for hospitals that accept Medicare and participate in the inpatient prospective payment system.
    #Data Source: These tables are based on AAMC's analysis of FY2020 Medicare cost report data, HCRIS July 2022 release. 
    #If FY2020 isn't available, FY2019 data is used. AAMC membership as of September 2022.
    aamcHospitalsDF = (aamcHospitalsDF.withColumn("aamcCothMemberFy22", F.when( F.col("FY22 COTH Member")=="Y", 1)
                                                                         .when( F.col("FY22 COTH Member")=="N", 0)
                                                                         .otherwise(F.lit(None)))
                                      .withColumn("aamcTeachingStatus", F.when( F.col("Teaching Status")=="Teaching", 1)
                                                                         .when( F.col("Teaching Status")=="Non-Teaching", 0)
                                                                         .otherwise(F.lit(None)))
                                      .withColumn("aamcMajorTeachingStatus", F.when( F.col("Major Teaching Status")=="Major Teaching", 1)
                                                                              .when( F.col("Major Teaching Status")=="Other Teaching", 0)
                                                                              .when( F.col("Major Teaching Status")=="Non-Teaching", 0)
                                                                              .otherwise(F.lit(None))))
    return aamcHospitalsDF

def add_processed_name(DF,colToProcess="providerName"):

    processedCol = colToProcess + "Processed"
    DF = (DF.withColumn(processedCol, 
                             F.regexp_replace(
                                 F.trim( F.lower(F.col(colToProcess)) ), r"\'s|\&|\.|\,| llc| inc| ltd| lp| lc|\(|\)| program", "") ) #replace with nothing
            .withColumn(processedCol,
                             F.regexp_replace( 
                                 F.col(processedCol) , "-| at | of | for | and ", " ") )  #replace with space
            .withColumn(processedCol,
                             F.regexp_replace(
                                 F.col(processedCol) , " {2,}", " ") )) #replace more than 2 spaces with one space 
                                 
    return DF   

def prep_acgmeSitesDF(acgmeSitesDF):

    acgmeSitesDF = acgmeSitesDF.withColumn("siteZip", F.substring(F.trim(F.col("Institution Postal Code")),1,5))
    acgmeSitesDF = acgmeSitesDF.withColumn("siteName", F.col("Institution Name"))
    #acgmeSites do not include any id I can use for linking, so make site name ready for probabilistic matching
    acgmeSitesDF = add_processed_name(acgmeSitesDF, colToProcess="siteName") 
    return acgmeSitesDF

def prep_acgmeProgramsDF(acgmeProgramsDF):

    acgmeProgramsDF = acgmeProgramsDF.withColumn("programZip", F.substring(F.trim(F.col("Program Postal Code")),1,5))
    acgmeProgramsDF = acgmeProgramsDF.withColumn("programName", 
                                                 F.regexp_replace( 
                                                     F.trim( F.lower(F.col("Program Name")) ), " Program", "") )
    #acgmePrograms do not include any id I can use for linking, so make program name ready for probabilistic matching
    acgmeProgramsDF = add_processed_name(acgmeProgramsDF, colToProcess="programName") 
    return acgmeProgramsDF

def add_acgmeSitesInZip(acgmeSitesDF):
    eachZip = Window.partitionBy("institutionZip")
    acgmeSitesDF = acgmeSitesDF.withColumn("acgmeSitesInZip", F.collect_set( F.col("institutionNameProcessed")).over(eachZip)) 
    return acgmeSitesDF

def add_acgmeProgramsInZip(acgmeProgramsDF):
    eachZip = Window.partitionBy("programZip")
    acgmeProgramsDF = acgmeProgramsDF.withColumn("acgmeProgramsInZip", F.collect_set( F.col("programNameProcessed")).over(eachZip)) 
    return acgmeProgramsDF

def add_accredited(acgmeProgramsDF):

    acgmeProgramsDF = acgmeProgramsDF.withColumn("accredited",
                                                 F.when( 
                                                     F.col("Program Accreditation Name").isin(["Continued Accreditation",
                                                                                               "Continued Accreditation with Warning",
                                                                                               "Initial Accreditation",
                                                                                               "Probationary Accreditation",
                                                                                               "Continued Accreditation without Outcomes",
                                                                                               "Initial Accreditation with Warning"]), 1)
                                                  .otherwise(0))
    return acgmeProgramsDF

def add_primaryTaxonomy(npiProvidersDF):

    #starting with spark 3.4, you can use F.array_compact to not allow nulls to enter the array
    codeAndSwitchCols = 'F.array(' + \
                        ','.join(\
                            f'F.array(F.col("Healthcare Provider Taxonomy Code_{x}"),F.col("Healthcare Provider Primary Taxonomy Switch_{x}"))' \
                            for x in range(1,16)) +')'

    npiProvidersDF = npiProvidersDF.withColumn("codeAndSwitch", eval(codeAndSwitchCols))
    npiProvidersDF = npiProvidersDF.withColumn("codeAndSwitchPrimary", F.expr('filter(codeAndSwitch, x -> x[1]=="Y")'))
    npiProvidersDF = npiProvidersDF.withColumn("primaryTaxonomy", F.flatten(F.col("codeAndSwitchPrimary"))[0])
    npiProvidersDF = npiProvidersDF.drop("codeAndSwitch","codeAndSwitchPrimary")
    return npiProvidersDF

def add_cah(npiProvidersDF, primary=True):
    '''Critical access hospitals. 
    https://taxonomy.nucc.org/?searchTerm=282NC0060X'''
    cahTaxonomyCodes = ["282NC0060X"]
    if (primary):
        cahTaxonomyCondition = 'F.col("primaryTaxonomy").isin(cahTaxonomyCodes)'
    else:
        cahTaxonomyCondition = \
                   '(' + '|'.join('(F.col(' + f'"Healthcare Provider Taxonomy Code_{x}"' + ').isin(cahTaxonomyCodes))' \
                   for x in range(1,16)) +')'
    npiProvidersDF = npiProvidersDF.withColumn("cah", F.when(eval(cahTaxonomyCondition), 1).otherwise(0))
    return npiProvidersDF

def add_rach(npiProvidersDF, primary=True):
    '''Rural acute care hospitals. 
    https://taxonomy.nucc.org/?searchTerm=282NR1301X&searchButton=search'''
    rachTaxonomyCodes = ["282NR1301X"]
    if (primary):
        rachTaxonomyCondition = 'F.col("primaryTaxonomy").isin(rachTaxonomyCodes)'
    else:
        rachTaxonomyCondition = \
                   '(' + '|'.join('(F.col(' + f'"Healthcare Provider Taxonomy Code_{x}"' + ').isin(rachTaxonomyCodes))' \
                   for x in range(1,16)) +')'
    npiProvidersDF = npiProvidersDF.withColumn("rach", F.when(eval(rachTaxonomyCondition), 1).otherwise(0))
    return npiProvidersDF   

def add_gach(npiProvidersDF, primary=True):

    # taxonomy codes are not part of MBSF or LDS files, but they are present in the CMS Provider file, they can be linked using NPI
    # in order for a provider to obtain an NPI they must have at least 1 taxonomy code (primary one)
    # but they may also have more than 1 taxonomy codes
    # it seems that the CMS Provider file is quite complete, did not result in loss of rows
    # https://www.cms.gov/Medicare/Provider-Enrollment-and-Certification/Find-Your-Taxonomy-Code

    #GACH: general acute care hospital
    # https://taxonomy.nucc.org/?searchTerm=282N00000X&searchButton=search
    # all of them are listed here: https://taxonomy.nucc.org/
    gachTaxonomyCodes = ["282N00000X"] #my definition of general acute care hospitals
    if (primary):
        gachTaxonomyCondition = 'F.col("primaryTaxonomy").isin(gachTaxonomyCodes)'
    else:
        gachTaxonomyCondition = \
                   '(' + '|'.join('(F.col(' + f'"Healthcare Provider Taxonomy Code_{x}"' + ').isin(gachTaxonomyCodes))' \
                   for x in range(1,16)) +')'

    npiProvidersDF = npiProvidersDF.withColumn("gach", F.when(eval(gachTaxonomyCondition), 1).otherwise(0))
    return npiProvidersDF

def add_rehabilitation(npiProvidersDF, primary=True):
    # https://taxonomy.nucc.org/
    #https://taxonomy.nucc.org/?searchTerm=283X00000X
    # https://data.cms.gov/provider-data/dataset/7t8x-u3ir
    rehabTaxonomyCodes = ["283X00000X", "273Y00000X"] #my definition of rehabilitation hospitals
    if (primary):
        rehabTaxonomyCondition = 'F.col("primaryTaxonomy").isin(rehabTaxonomyCodes)'         
    else: 
        rehabTaxonomyCondition = \
             '(' + '|'.join('(F.col(' + f'"Healthcare Provider Taxonomy Code_{x}"' + ').isin(rehabTaxonomyCodes))' \
                       for x in range(1,16)) +')'
    npiProvidersDF = npiProvidersDF.withColumn("rehabilitation", F.when(eval(rehabTaxonomyCondition), 1).otherwise(0))
    return npiProvidersDF

def add_pediatricHospital(npiProvidersDF):
    childrenHospitalTaxonomyCodes = [ "281PC2000X", "282NC2000X", "283XC2000X" ]
    childrenHospitalCondition = 'F.col("primaryTaxonomy").isin(childrenHospitalTaxonomyCodes)' 
    npiProvidersDF = npiProvidersDF.withColumn("pediatricHospital", F.when(eval(childrenHospitalCondition), 1).otherwise(0))
    return npiProvidersDF

def add_psychiatricHospital(npiProvidersDF):
    psychHospitalTaxonomyCodes = [ "273R00000X", "283Q00000X" ] 
    psychCondition = 'F.col("primaryTaxonomy").isin(psychHospitalTaxonomyCodes)'
    npiProvidersDF = npiProvidersDF.withColumn("psychiatricHospital", F.when(eval(psychCondition), 1).otherwise(0))
    return npiProvidersDF

def add_ltcHospital(npiProvidersDF):
    '''Long Term Care Hospitals'''
    ltcHospitalTaxonomyCodes = [ "282E00000X" ]
    ltcCondition = 'F.col("primaryTaxonomy").isin(ltcHospitalTaxonomyCodes)'
    npiProvidersDF = npiProvidersDF.withColumn("ltcHospital", F.when(eval(ltcCondition), 1).otherwise(0))
    return npiProvidersDF

def prep_npiProvidersDF(npiProvidersDF):
    npiProvidersDF = add_primaryTaxonomy(npiProvidersDF)
    npiProvidersDF = add_gach(npiProvidersDF, primary=True).withColumnRenamed("gach","gachPrimary")
    npiProvidersDF = add_gach(npiProvidersDF, primary=False).withColumnRenamed("gach","gachAll")
    npiProvidersDF = add_rehabilitation(npiProvidersDF, primary=True).withColumnRenamed("rehabilitation","rehabilitationPrimary")
    npiProvidersDF = add_rehabilitation(npiProvidersDF, primary=False).withColumnRenamed("rehabilitation","rehabilitationAll")
    npiProvidersDF = add_cah(npiProvidersDF, primary=True).withColumnRenamed("cah", "cahPrimary")
    npiProvidersDF = add_cah(npiProvidersDF, primary=False).withColumnRenamed("cah", "cahAll")
    npiProvidersDF = add_rach(npiProvidersDF, primary=True).withColumnRenamed("rach", "rachPrimary")
    npiProvidersDF = add_rach(npiProvidersDF, primary=False).withColumnRenamed("rach", "rachAll")
    npiProvidersDF = add_pediatricHospital(npiProvidersDF)
    npiProvidersDF = add_psychiatricHospital(npiProvidersDF)
    npiProvidersDF = add_ltcHospital(npiProvidersDF)
    return npiProvidersDF

def prep_strokeCentersCamargoDF(strokeCentersCamargoDF):
    #note on data: the column name is CCN however some rows include 5 digit long codes and those cannot be CCN numbers 
    #(I checked the CMS documentation), the 5 digit long codes may be state IDs (the methods of their paper include
    #finding stroke centers from state data) or may be something else....
    strokeCentersCamargoDF = strokeCentersCamargoDF.select( F.col("CCN") ).distinct() #CCN 220074 appears twice for some reason... 
    #they probably used excel to get the list and excel removed 0 at the beginning of the CCN strings....
    strokeCentersCamargoDF = strokeCentersCamargoDF.withColumn("CCN",
                                                               F.when( F.length(F.col("CCN"))==5, F.concat(F.lit("0"),F.col("CCN")))
                                                                .otherwise(F.col("CCN")))
    strokeCentersCamargoDF = strokeCentersCamargoDF.withColumn("strokeCenterCamargo", F.lit(1))
    return strokeCentersCamargoDF

def prep_strokeCentersJCDF(strokeCentersJCDF):
    strokeCentersJCDF = add_processed_name(strokeCentersJCDF,colToProcess="OrganizationName")
    return strokeCentersJCDF

def get_nameTokens(col):
    '''The distinctive words of a facility name, as an array Column: "ST MARY MEDICAL CENTER" gives ["mary"].'''
    words = F.split(F.regexp_replace(F.lower(F.coalesce(col, F.lit(""))), r"[^a-z0-9]+", " "), " ")
    return F.array_except(F.array_distinct(words), F.array([F.lit(w) for w in [""] + nameGenericWords]))

def get_nameScore(col1, col2):
    '''How much two facility names agree, 0 to 1: shared distinctive words over the distinctive words of the shorter name.'''
    tokens1, tokens2 = get_nameTokens(col1), get_nameTokens(col2)
    shorter = F.least(F.size(tokens1), F.size(tokens2))
    #1 means every distinctive word of the shorter name is in the other ("Maria Parham Health", "MARIA PARHAM MEDICAL CENTER"),
    #0 nothing in common or a name made of generic words only
    return F.when(shorter > 0, F.size(F.array_intersect(tokens1, tokens2)) / shorter).otherwise(F.lit(0.0))

def prep_jcAccreditationDF(jcDF, pathToData=None, filename=None, maxCalls=None, keyFilename=None, topProgramOnly=False,
                           excludePrograms=None, placesLookup=False):
    '''Builds jcAccreditation.parquet, one row per joint commission site, from the raw export of accredited
    organizations, see scripts/03_geocode_jc.py; scripts/04_match_jc_pos.py reads it to build the per CCN table get_data loads. One row per site and program in the export; with
    topProgramOnly each site keeps its top stroke program (jcProgramRank) and sites without one are dropped, excludePrograms
    drops programs by keyword first. Adds the renamed raw columns (jc*), jcState, jcZip, jcAddress, jcSearchName, jcPlaceQuery, jcProgramRank and,
    with pathToData, the geocoded address (jcLat, jcLng, jcGeocode*) and, with placesLookup, the place found for the site
    name (jcPlace*) and the site location to use (jcSiteLat, jcSiteLng, jcSiteLocationSource).'''
    #parquet does not allow spaces and parentheses in column names
    rawCols = {"HCO ID": "jcHcoId",
               "Organization Name": "jcOrganizationName",
               "Organization Doing Business As (DBA) Name": "jcOrganizationDbaName",
               "State": "jcState",
               "City": "jcCity",
               "Street Address": "jcStreetAddress",
               "Postal Code": "jcPostalCode",
               "Site Name": "jcSiteName",
               "Site Doing Business As (DBA) Name": "jcSiteDbaName",
               "Program": "jcProgram",
               "Effective Date": "jcEffectiveDate",
               "Status": "jcStatus"}
    jcDF = jcDF.select([F.col(raw).alias(new) for raw, new in rawCols.items()])
    stateAbbreviation = F.create_map([F.lit(x) for x in chain(*usStateAbbreviations.items())])
    jcDF = (jcDF.withColumn("jcSiteName", F.coalesce(F.col("jcSiteName"), F.col("jcSiteDbaName"), F.col("jcOrganizationName")))
                .withColumn("jcZip", F.substring(F.trim(F.col("jcPostalCode")), 1, 5)))
    #the address is built from the raw (spelled out) state: it is the geocoding cache key and must not change
    jcDF = add_address(jcDF, "jcStreetAddress", "jcCity", "jcState", "jcZip", "jcAddress")
    #the export spells the state out, the usps abbreviation joins the POS STATE_CD; a 2-letter value passes through
    jcDF = jcDF.withColumn("jcState", F.coalesce(stateAbbreviation[F.lower(F.trim(F.col("jcState")))],
                                                 F.upper(F.trim(F.col("jcState")))))
    #the address is the organization's, not the site's, so the site is searched by its public name: the site DBA name when
    #there is one (the site name is often the legal entity, eg "Sutter Bay Hospitals" for eight hospitals), unless the DBA is
    #generic words only ("Hospital", "General Acute Care Hospital") which would find any hospital, else the site name
    dbaWords = F.split(F.regexp_replace(F.lower(F.coalesce(F.col("jcSiteDbaName"), F.lit(""))), r"[^a-z0-9']+", " "), " ")
    dbaIsSpecific = F.size(F.array_except(dbaWords, F.array([F.lit(w) for w in [""] + jcGenericNameWords]))) > 0
    siteDba = F.when(F.upper(F.trim(F.col("jcSiteDbaName"))).isin("", "N/A") | ~dbaIsSpecific, None).otherwise(F.trim(F.col("jcSiteDbaName")))
    jcDF = (jcDF.withColumn("jcSearchName", F.coalesce(siteDba, F.trim(F.col("jcSiteName"))))
                .withColumn("jcPlaceQuery", F.concat_ws(", ", F.col("jcSearchName"), F.col("jcState"))))
    #stroke programs are ranked by keyword (jcStrokeProgramRanking), case and extra spaces ignored (the export writes
    #"Stroke  Rehabilitation"); requiring "stroke" keeps eg Primary Care Medical Home out; other programs get a null rank
    program = F.regexp_replace(F.lower(F.trim(F.col("jcProgram"))), r"\s+", " ")
    programRank = F.lit(None)
    for rank, keyword in reversed(list(enumerate(jcStrokeProgramRanking, start=1))):
        programRank = F.when(program.contains("stroke") & program.contains(keyword), F.lit(rank)).otherwise(programRank)
    jcDF = jcDF.withColumn("jcProgramRank", programRank.cast("int"))
    for keyword in (excludePrograms or []):
        jcDF = jcDF.filter(~F.coalesce(program.contains(keyword.lower()), F.lit(False)))
    if topProgramOnly:
        #one row per site, its best ranked stroke program; a site with only non stroke programs is dropped
        eachSite = Window.partitionBy("jcHcoId", "jcSiteName", "jcAddress").orderBy("jcProgramRank", "jcProgram")
        jcDF = (jcDF.filter(F.col("jcProgramRank").isNotNull())
                    .withColumn("programRow", F.row_number().over(eachSite)).filter(F.col("programRow")==1).drop("programRow"))
    if pathToData is not None:
        jcDF = add_geocode_info(jcDF, "jcAddress", pathToData, "jc", maxCalls=maxCalls, keyFilename=keyFilename)
    if placesLookup:
        #the name search is biased to within 50 km of the organization's address
        jcDF = add_place_info(jcDF, "jcPlaceQuery", pathToData, "jc", biasLatCol="jcLat", biasLngCol="jcLng",
                              maxCalls=maxCalls, keyFilename=keyFilename)
        #the place is the site's point when it is typed as a care site (a freestanding ED or a campus often comes back
        #medical_clinic rather than hospital), else the address is; a non hospital place is safe to use because
        #add_pos_ccn_info falls back to the organization's address when no hospital is near it
        placeTypes = F.split(F.coalesce(F.col("jcPlaceTypes"), F.lit("")), ",")
        isHospital = F.array_contains(placeTypes, "hospital")
        isCareSite = F.size(F.array_intersect(placeTypes, F.array([F.lit(t) for t in jcPlaceSiteTypes]))) > 0
        usePlace = (F.col("jcPlaceStatus") == "OK") & isCareSite
        jcDF = (jcDF.withColumn("jcPlaceIsHospital", F.when(F.col("jcPlaceStatus") == "OK", isHospital.cast("int")))
                    .withColumn("jcPlaceDistanceKm", #how far the site is from its organization's address
                                get_geodesicDistanceKm(F.col("jcLat"), F.col("jcLng"), F.col("jcPlaceLat"), F.col("jcPlaceLng")))
                    .withColumn("jcSiteLat", F.when(usePlace, F.col("jcPlaceLat")).otherwise(F.col("jcLat")))
                    .withColumn("jcSiteLng", F.when(usePlace, F.col("jcPlaceLng")).otherwise(F.col("jcLng")))
                    .withColumn("jcSiteLocationSource", F.when(usePlace, "place").otherwise("address")))
    if filename is not None:
        jcDF.coalesce(1).write.mode("overwrite").parquet(filename)
    return jcDF

def add_pos_nearest_info(jcDF, posDF, latCol="jcLat", lngCol="jcLng", prefix="posNearest"):
    '''For every joint commission site the nearest and second nearest POS hospital in the same state from the point in
    latCol/lngCol: {prefix}Ccn, FacName, DistanceKm, Active, GeocodeLocationType and {prefix}Second{Ccn, FacName,
    DistanceKm, Active}. posDF is used as given, filter it first. Every row of jcDF is kept.'''
    siteKey = ["jcHcoId", "jcSiteName", "jcAddress"]
    ccn, facName, distance, active, geocodeType = (f"{prefix}Ccn", f"{prefix}FacName", f"{prefix}DistanceKm",
                                                    f"{prefix}Active", f"{prefix}GeocodeLocationType")
    secondCcn, secondFacName, secondDistance, secondActive = (f"{prefix}SecondCcn", f"{prefix}SecondFacName",
                                                              f"{prefix}SecondDistanceKm", f"{prefix}SecondActive")
    hospitalsDF = (posDF.filter((F.col("posHospital")==1) & F.col("posLat").isNotNull())
                        .select(F.col("STATE_CD").alias("jcState"),
                                F.col("PRVDR_NUM").alias(ccn),
                                F.col("FAC_NAME").alias(facName), #for eyeballing only, names play no part
                                F.col("posActive").alias(active),
                                F.col("posGeocodeLocationType").alias(geocodeType),
                                F.col("posLat"), F.col("posLng")))
    #at equal distance (a successor CCN at the same address) the active hospital is preferred
    eachSite = Window.partitionBy(siteKey).orderBy(distance, F.desc(active), ccn)
    #joined on the state rather than every site against every hospital: no api call is involved, the coordinates are already
    #in the parquets, but a full cross join is ~1.5k sites x ~9k hospitals = ~14 million distances where the state block is a
    #few hundred per site. The price is that a site whose nearest hospital is across a state line gets its nearest in-state
    #one. The second nearest says whether the nearest is certain to be the site (same campus, successor CCN)
    nearestDF = (jcDF.filter(F.col(latCol).isNotNull())
                     .select(*siteKey, "jcState", F.col(latCol).alias("siteLat"), F.col(lngCol).alias("siteLng")).distinct()
                     .join(hospitalsDF, on=["jcState"], how="inner")
                     .withColumn(distance,
                                 get_geodesicDistanceKm(F.col("siteLat"), F.col("siteLng"), F.col("posLat"), F.col("posLng")))
                     .withColumn("nearestRow", F.row_number().over(eachSite))
                     .withColumn(secondCcn, F.lead(ccn).over(eachSite))
                     .withColumn(secondFacName, F.lead(facName).over(eachSite))
                     .withColumn(secondDistance, F.lead(distance).over(eachSite))
                     .withColumn(secondActive, F.lead(active).over(eachSite))
                     .filter(F.col("nearestRow")==1)
                     .select(*siteKey, ccn, facName, distance, active, geocodeType,
                             secondCcn, secondFacName, secondDistance, secondActive))
    jcDF = jcDF.join(nearestDF, on=siteKey, how="left_outer")
    return jcDF

def add_pos_ccn_info(jcDF, posDF, maxDistanceKm=0.5, tieDistanceKm=0.1, relaxedDistanceKm=5.0, nameScoreMin=1.0, overridesDF=None):
    '''Assigns each joint commission site the CCN of the POS hospital it is: the nearest hospital of posDF within
    maxDistanceKm of the site's own point (nearestSite), else of its organization's address (nearestParent), else within
    relaxedDistanceKm with a matching name (nearestNamed), else none; overridesDF (jcHcoId, jcSiteName, keepCcn) replaces
    single answers. Pass posDF filtered to active acute and cah hospitals. Adds posNearest*, posParentNearest*, posCcn,
    posCcnFacName, posCcnDistanceKm, posCcnActive, posCcnPass, posMatchMethod, posMatchAmbiguous, posCcnNameScore.
    Every row of jcDF is kept; several sites may share a CCN (campuses under one provider agreement).'''
    if "jcSiteLat" not in jcDF.columns:
        jcDF = (jcDF.withColumn("jcSiteLat", F.col("jcLat")).withColumn("jcSiteLng", F.col("jcLng"))
                    .withColumn("jcSiteLocationSource", F.lit("address")))
    #two lookups: from the site's own point and from the organization's address. A campus that bills under its parent
    #(NewYork-Presbyterian Allen under 330101) is listed by CMS only at the parent's address, so when nothing is at the
    #site's point the parent's address is tried; a retired CCN at a campus (Presbyterian Hospital 330012 at Columbia)
    #must not win over the parent's, which is why posDF should hold active hospitals only
    jcDF = add_pos_nearest_info(jcDF, posDF, latCol="jcSiteLat", lngCol="jcSiteLng", prefix="posNearest")
    jcDF = add_pos_nearest_info(jcDF, posDF, latCol="jcLat", lngCol="jcLng", prefix="posParentNearest")
    siteHit = (F.col("jcSiteLocationSource") == "place") & (F.col("posNearestDistanceKm") <= maxDistanceKm)
    parentHit = F.col("posParentNearestDistanceKm") <= maxDistanceKm
    method = F.when(siteHit, "nearestSite").when(parentHit, "nearestParent").otherwise("none")
    jcDF = jcDF.withColumn("posMatchMethod", method)
    def pick(siteCol, parentCol):
        return (F.when(F.col("posMatchMethod") == "nearestSite", F.col(siteCol))
                 .when(F.col("posMatchMethod") == "nearestParent", F.col(parentCol)))
    jcDF = (jcDF.withColumn("posCcn", pick("posNearestCcn", "posParentNearestCcn"))
                .withColumn("posCcnFacName", pick("posNearestFacName", "posParentNearestFacName"))
                .withColumn("posCcnDistanceKm", pick("posNearestDistanceKm", "posParentNearestDistanceKm")) #kept for a stricter cutoff downstream
                .withColumn("posCcnActive", pick("posNearestActive", "posParentNearestActive"))
                .withColumn("posCcnPass", F.when(F.col("posMatchMethod") == "nearestSite", "site")
                                           .when(F.col("posMatchMethod") == "nearestParent", "parent"))
                #ambiguous: another hospital within tieDistanceKm of the chosen one, so the choice rested on the tie break
                .withColumn("posMatchAmbiguous",
                            F.when(F.col("posMatchMethod") == "nearestSite",
                                   (F.col("posNearestSecondDistanceKm") <= tieDistanceKm).cast("int"))
                             .when(F.col("posMatchMethod") == "nearestParent",
                                   (F.col("posParentNearestSecondDistanceKm") <= tieDistanceKm).cast("int")))
                .withColumn("posMatchAmbiguous", F.when(F.col("posCcn").isNotNull(), F.coalesce(F.col("posMatchAmbiguous"), F.lit(0)))))
    #last resort and the only place a name is used: for the sites still unmatched, the hospital within relaxedDistanceKm
    #whose name contains every distinctive word of the shorter name (get_nameScore of nameScoreMin), ties by distance.
    #This recovers hospitals whose POS coordinate is off (PO box address, partial geocode, a move to a new building);
    #on the Dec 2022 data it recovered 18 of 38 unmatched sites with no error, any looser score admitted wrong hospitals
    siteKey = ["jcHcoId", "jcSiteName", "jcAddress"]
    hospitalsDF = (posDF.filter((F.col("posHospital")==1) & F.col("posLat").isNotNull())
                        .select(F.col("STATE_CD").alias("jcState"), F.col("PRVDR_NUM").alias("namedCcn"),
                                F.col("FAC_NAME").alias("namedFacName"), F.col("posActive").alias("namedActive"),
                                F.col("posLat"), F.col("posLng")))
    eachSite = Window.partitionBy(siteKey).orderBy(F.desc("namedScore"), "namedDistanceKm", "namedCcn")
    namedDF = (jcDF.filter((F.col("posMatchMethod") == "none") & F.col("jcSiteLat").isNotNull())
                   .select(*siteKey, "jcState", "jcSearchName", "jcSiteLat", "jcSiteLng").distinct()
                   .join(hospitalsDF, on=["jcState"], how="inner")
                   .withColumn("namedDistanceKm",
                               get_geodesicDistanceKm(F.col("jcSiteLat"), F.col("jcSiteLng"), F.col("posLat"), F.col("posLng")))
                   .filter(F.col("namedDistanceKm") <= relaxedDistanceKm)
                   .withColumn("namedScore", get_nameScore(F.col("jcSearchName"), F.col("namedFacName")))
                   .filter(F.col("namedScore") >= nameScoreMin)
                   .withColumn("namedRow", F.row_number().over(eachSite))
                   .filter(F.col("namedRow") == 1)
                   .select(*siteKey, "namedCcn", "namedFacName", "namedDistanceKm", "namedActive", "namedScore"))
    jcDF = jcDF.join(namedDF, on=siteKey, how="left_outer")
    named = F.col("namedCcn").isNotNull()
    jcDF = (jcDF.withColumn("posCcn", F.when(named, F.col("namedCcn")).otherwise(F.col("posCcn")))
                .withColumn("posCcnFacName", F.when(named, F.col("namedFacName")).otherwise(F.col("posCcnFacName")))
                .withColumn("posCcnDistanceKm", F.when(named, F.col("namedDistanceKm")).otherwise(F.col("posCcnDistanceKm")))
                .withColumn("posCcnActive", F.when(named, F.col("namedActive")).otherwise(F.col("posCcnActive")))
                .withColumn("posCcnPass", F.when(named, "named").otherwise(F.col("posCcnPass")))
                .withColumn("posMatchMethod", F.when(named, "nearestNamed").otherwise(F.col("posMatchMethod")))
                .withColumn("posMatchAmbiguous", F.when(named, F.lit(0)).otherwise(F.col("posMatchAmbiguous")))
                .withColumn("posCcnNameScore", F.when(named, F.col("namedScore"))) #only the name based assignments carry a score
                .drop("namedCcn", "namedFacName", "namedDistanceKm", "namedActive", "namedScore"))
    if overridesDF is not None:
        #hand picked exceptions, a blank keepCcn means no CCN
        hospitalsDF = posDF.select(F.col("PRVDR_NUM").alias("keepCcn"), F.col("FAC_NAME").alias("keepFacName"),
                                   F.col("posActive").alias("keepActive"))
        overridesDF = (overridesDF.select("jcHcoId", "jcSiteName", F.trim(F.col("keepCcn")).alias("keepCcn"), F.lit(1).alias("overridden"))
                                  .dropDuplicates(["jcHcoId", "jcSiteName"])
                                  .join(hospitalsDF, on=["keepCcn"], how="left_outer"))
        jcDF = jcDF.join(overridesDF, on=["jcHcoId", "jcSiteName"], how="left_outer")
        keep = F.col("overridden") == 1
        keepSome = keep & (F.col("keepCcn") != "")
        jcDF = (jcDF.withColumn("posCcn", F.when(keepSome, F.col("keepCcn")).when(keep, F.lit(None)).otherwise(F.col("posCcn")))
                    .withColumn("posCcnFacName", F.when(keepSome, F.col("keepFacName")).when(keep, F.lit(None)).otherwise(F.col("posCcnFacName")))
                    .withColumn("posCcnActive", F.when(keepSome, F.col("keepActive")).when(keep, F.lit(None)).otherwise(F.col("posCcnActive")))
                    .withColumn("posCcnDistanceKm", F.when(keep, F.lit(None)).otherwise(F.col("posCcnDistanceKm")))
                    .withColumn("posCcnPass", F.when(keep, F.lit(None)).otherwise(F.col("posCcnPass")))
                    .withColumn("posMatchAmbiguous", F.when(keep, F.lit(None)).otherwise(F.col("posMatchAmbiguous")))
                    .withColumn("posCcnNameScore", F.when(keep, F.lit(None)).otherwise(F.col("posCcnNameScore")))
                    .withColumn("posMatchMethod", F.when(keepSome, "override").when(keep, "none").otherwise(F.col("posMatchMethod")))
                    .drop("keepCcn", "keepFacName", "keepActive", "overridden"))
    return jcDF

#The joint commission stroke certifications reach the claims as follows (scripts/01-04). The export of certified sites
#carries no CCN, so sites are linked to POS hospitals by location, not by name: the POS hospital addresses are geocoded
#(01), each site's organization address is geocoded and, because that address is the organization's rather than the
#site's, the site's public name is searched with the Places API (02), the results are attached and one row per site with
#its top stroke program is written (03, prep_jcAccreditationDF), and each site is assigned the nearest active acute or
#critical access hospital within 0.5 km of its own point, else of its organization's address (a campus billing under its
#parent), else, name checked, within 5 km (04, add_pos_ccn_info). get_ccn_jc_info then folds the sites into one row per
#CCN, the ccnStrokeCertification parquet get_data loads and base.add_provider_stroke_certification_info joins on PROVIDER.
def get_ccn_jc_info(jcDF):
    '''One row per CCN that received a joint commission site (see add_pos_ccn_info), the table the claims join on
    PROVIDER: jcSites, jcBestProgramRank, jcBestProgram, jcBestProgramSite, jcBestProgramMatchMethod, jcSiteNames,
    jcMatchMethods, jcMaxCcnDistanceKm, jcCertificationConfidence.'''
    #the best program is the highest stroke certification among the CCN's sites: a stroke transfer to any campus of the
    #provider shows the same CCN
    eachCcn = Window.partitionBy("posCcn").orderBy(F.asc_nulls_last("jcProgramRank"), "jcProgram", "jcSiteName")
    ccnDF = (jcDF.filter(F.col("posCcn").isNotNull())
                 .withColumn("bestRow", F.row_number().over(eachCcn))
                 .groupBy("posCcn")
                 .agg(F.count("*").alias("jcSites"),
                      F.min("jcProgramRank").alias("jcBestProgramRank"),
                      F.first(F.when(F.col("bestRow") == 1, F.col("jcProgram")), ignorenulls=True).alias("jcBestProgram"),
                      F.first(F.when(F.col("bestRow") == 1, F.col("jcSiteName")), ignorenulls=True).alias("jcBestProgramSite"),
                      F.first(F.when(F.col("bestRow") == 1, F.col("posMatchMethod")), ignorenulls=True).alias("jcBestProgramMatchMethod"),
                      F.collect_list("jcSiteName").alias("jcSiteNames"),
                      F.array_sort(F.collect_set("posMatchMethod")).alias("jcMatchMethods"),
                      F.max("posCcnDistanceKm").alias("jcMaxCcnDistanceKm")))
    #how sure the certification is the CCN's, from how the site holding it was assigned and whether other sites share the CCN:
    #4 a hospital sits where Google puts the site's name and it is the CCN's only site, 3 same but other sites share the CCN,
    #2 a campus folded into the parent's CCN (or an override), 1 rests on a name match
    ccnDF = ccnDF.withColumn("jcCertificationConfidence",
                             F.when((F.col("jcBestProgramMatchMethod") == "nearestSite") & (F.col("jcSites") == 1), 4)
                              .when(F.col("jcBestProgramMatchMethod") == "nearestSite", 3)
                              .when(F.col("jcBestProgramMatchMethod").isin("nearestParent", "override"), 2)
                              .when(F.col("jcBestProgramMatchMethod") == "nearestNamed", 1))
    return ccnDF

def add_ccn_from_pos(DF,posDF, providerZip="providerZip",providerName="providerNameProcessed"): #assumes a zipCode column, providerNameProcessed
    DF = DF.join(posDF
                     .select(F.col("FAC_NAMEProcessed"),F.col("ZIP_CD"),F.col("PRVDR_NUM")),
                 on=[F.col("ZIP_CD")==F.col(f"{providerZip}")],
                 how="inner")
    DF = DF.withColumn("levenshteinDistance",
                       F.levenshtein(F.col(f"{providerName}"), F.col("FAC_NAMEProcessed")))
    eachZip = Window.partitionBy(f"{providerZip}")
    DF = DF.withColumn("minLevenshteinDistance",
                       F.min(F.col("levenshteinDistance")).over(eachZip))
    DF = DF.filter(F.col("minLevenshteinDistance")==F.col("levenshteinDistance"))
    return DF

def run_data_tests(data):
    '''The source files are read as CSV with spark's default quote character (") so any field that uses single quotes
    as its text qualifier (eg the HCUP procedure codes were read as 'value' rather than value) keeps those quotes as
    literal characters. When such quotes are not stripped in the corresponding prep_* function they silently break
    joins and comparisons against the (unquoted) claims data, producing wrong results with no error.
    This tests that no string column in any of the loaded source dataframes still contains values wrapped in quotes.'''
    for source in list(data.keys()):
        df = data[source]
        if not isinstance(df, DataFrame): #eg geojsonCounty is a dict, not a spark dataframe
            continue
        strCols = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
        if not strCols:
            continue
        #a quote-qualified field starts and ends with the same quote character, eg 'value' or "value"
        wrapped = reduce(lambda x, y: x | y,
                         [F.col(f"`{c}`").rlike(r"^(['\"]).*\1$") for c in strCols])
        wrappedCount = df.filter(wrapped).count()
        assert 0 == wrappedCount, f"{source} includes {wrappedCount} string values wrapped in quote characters"

def print_partition_size_info(df):
    print(df.withColumn("partID", F.spark_partition_id()).groupBy("partID").count().describe("count").show())

