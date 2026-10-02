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
    #accredited organizations export from the joint commission website, the parquet is built once by prep_jcAccreditationDF
    #from the raw csv (same folder) and carries the geocoded coordinates of the sites, see prep_jcAccreditationDF
    filenames["jcAccreditation"] = [pathToData + "/JOINT-COMMISSION/jcAccreditation.parquet"]

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

    if file in ["hcris", "pos", "jcAccreditation"]:
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
    '''Bed counts, number of interns and residents, and rural/urban status per hospital per year from the
    CMS HCRIS hospital 2552-10 cost report files.

    pathToHcris holds one folder per federal fiscal year, HOSP10FY{year}, each containing the three
    headerless CSVs HOSP10_{year}_{rpt,nmrc,alpha}.csv as CMS distributes them in HOSP10FY{year}.ZIP
    (https://downloads.cms.gov/files/hcris/hosp10fy{year}.zip). Only rpt and nmrc are read; alpha holds
    the cost center labels, which the intensive care lines do not use (see below). This is meant to be
    run once, offline: when filename is given the result is written there as a single parquet, and it is
    that parquet the analysis code reads, so the ~8GB of source CSV is never scanned again. The returned
    dataframe is the uncomputed plan over the CSVs, so validate against the written parquet rather than
    against it.

    The nmrc file is one row per worksheet cell, keyed by (RPT_REC_NUM, WKSHT_CD, LINE_NUM, CLMN_NUM),
    so a measure is identified by its position on the cost report form. Beds live on Worksheet S-3
    Part I (WKSHT_CD S300001), column 2 (CLMN_NUM 00200), and the line blocks are intensive care
    00800-00899, coronary care 00900-00999, burn intensive care 01000-01099, surgical intensive care
    01100-01199, other special care 01200-01299, and total hospital beds 01400. HOSP2010_README.txt
    section 5.2 is explicit that these units are NOT cost center coded on S-3, so the actual line number
    is extracted rather than a label looked up in the alpha file. The line blocks are ranges rather than
    single lines because a hospital with more than one unit of a kind subscripts the line (00801, 00802,
    ... up to 00850 has been observed), so the beds of a multi-ICU hospital are only complete when the
    whole block is summed. LINE_NUM and CLMN_NUM are fixed-width zero-padded strings, which is why the
    ranges can be expressed as string comparisons. providerHcrisBedsIcu is the 00800-00899 block alone
    while providerHcrisBedsCriticalCare is all five blocks, so the first is a subset of the second rather
    than a category beside it. The critical care range ends at 01299 because line 13 is the nursery,
    which is not special care, and providerHcrisBedsTotal is read from line 01400, the form's own Total,
    rather than derived by summing the unit lines.

    Column 3 of the same lines is Bed Days Available, the beds of column 2 multiplied by the days in the
    cost reporting period. It is summed over the same three line groups into providerHcrisBedDaysIcu,
    providerHcrisBedDaysCriticalCare and providerHcrisBedDaysTotal, and it is also what column 2 is checked
    against: a bed count is replaced by the bed days divided by hcrisReportDays, rounded to a whole bed,
    whenever the count filed is more than 5 times that. Bed days are the beds a hospital had times the days
    it had them, so they measure a whole year of capacity where the bed counts measure the single day the
    period ends on, which is why they are worth carrying and not only worth checking against. They are long
    rather than int because a garbage cell in this column is 365 times larger than a garbage bed count, and
    one provider has already filed a value a third of the way to the int limit.

    The check is needed because nothing validates column 2 at filing time and some
    providers put a number in it that is not a bed count at all, which nothing downstream of them catches
    either -- CMS's own cost report public use file reads the same cell and carries the same values, so
    this is what the hospital filed rather than a misread of the file. Every
    such report has a sane bed days cell beside the wrong count and a sane count in the provider's own
    other years: 360044 filed 1594784 beds for FY2020 against 14640 bed days, 40 beds, and filed 36 to 63
    every other year; 131316 filed 52913 for FY2019 against 7665 bed days, 21 beds, and 21 every other
    year; 370215, 223300, 310028, 050179, 521357, 391300 and 493033 are the same story. Building the
    2015-2024 parquet, the check replaces a count on 26 of its 59553 reports, 0.04%: 22 totals, 12 of them
    in the thousands, and 4 reports whose total was sound while an intensive or special care line was not,
    among them a 56 bed intensive care unit against bed days for 8 filed by a hospital that files 8 in
    every other year, and a coronary care line reading 2191 beds against bed days for 10. The replacement
    is rare, then, but it is the whole of the tail: these are the reports a comparison against the AHA bed
    counts turns up first.

    The check is deliberately one sided and its threshold deliberately loose. It is one sided because
    a count far BELOW its bed days is the other cell being wrong, not this one: those reports file a bed
    days cell 5 to 10 times too large beside a count that is stable across the provider's other years, and
    replacing the good count with the bad bed days would be the error. It is 5 times rather than something
    tighter because the two cells legitimately disagree by a factor of about 2: column 2 is the beds
    available at the END of the period while bed days accumulate over it, so a hospital that adds or closes
    beds partway through the year has a real, correctly filed gap between them (100079 filed 524 beds for a
    period whose bed days imply 325, then 524 against 524 the following year).

    The check also refuses to fire when the bed days cell implies less than one bed, that is when it is
    absent, zero, or smaller than the days in the period, because a bed days cell that small is itself
    not a credible number and the check would then be correcting the sound cell with the broken one:
    040011 filed 41 beds for FY2016 against 6 bed days, a hospital open six bed days in a year, and its
    41 is the more believable of the two. This is also what keeps a replacement from ever coming out as
    0, which would be read downstream as the hospital having no unit of that kind rather than as a count
    that could not be trusted -- 020026 files 1 other special care bed against 1 bed day every year and
    keeps its 1.

    The lines and columns are those of the blank worksheets and their line by line instructions in the
    Provider Reimbursement Manual Part 2 (CMS Pub. 15-2) chapter 40
    (https://www.cms.gov/regulations-and-guidance/guidance/manuals/paper-based-manuals-items/cms021935).
    The blank worksheets are section 4090, https://www.cms.gov/files/document/r23p240f.pdf as of
    transmittal 23, with S-3 Part I on page 40-511 and S-2 Part I line 26 on page 40-504. The line by
    line instructions are https://www.cms.gov/files/document/r18p240ipdf.pdf as of transmittal 18, S-3
    Part I at section 4005.1 starting on page 40-53 with the instructions for lines 8 through 14 on page
    40-57, and S-2 Part I at section 4004.1. Those page numbers are the 40-NNN printed in the corner of
    the page, which is what the manual cross references and what survives a change of transmittal, not
    the position in the pdf. S-3 Part I column 2 is No. of
    Beds, defined there as the beds available for use by patients at the end of the cost reporting period
    per 42 CFR 412.105(b), and column 3 is Bed Days Available, instructed to be that count multiplied by
    the number of days in the cost reporting period, which is what makes the two checkable against each
    other; lines 8 through 13 are intensive care, coronary care, burn intensive care,
    surgical intensive care, other special care and nursery; and line 14, Total, is instructed to be the
    sum of lines 7 through 13 in columns 2 through 8, so it also counts the adults and pediatrics of line
    7 and the nursery of line 13. Column 9 is Interns & Residents FTEs and line 27 is Total, the sum of
    lines 14 through 26. S-2 Part I line 26 is the standard geographic classification, not the wage one,
    at the BEGINNING of the cost reporting period, 1 for urban and 2 for rural; line 27 is the same
    classification at the end of the period and is not read here.

    Two single cells are read besides the bed blocks: providerHcrisResidents, the number of interns and residents
    the facility employed stated as an FTE count, on S-3 Part I line 27 column 9, and the urban/rural
    geographic classification on Worksheet S-2 Part I (WKSHT_CD S200001) line 26 column 1, coded 1 for
    urban and 2 for rural. Line 27 of S-3 Part I is the whole facility, subproviders included, while
    line 14 is the hospital component alone. All of these positions were confirmed against CMS's own
    Cost Report public use file (CostReport_2019_Final.csv), which carries rpt_rec_num and so joins to
    the raw files report by report: across the 2039 reports it shares with HOSP10FY2019 its Number of
    Beds, Total Bed Days Available and Rural Versus Urban agree with these cells on every report, and
    its Number of Interns and Residents (FTE) agrees with line 27 on all 170 teaching hospitals while
    line 14 disagrees on 31 of them. That public use file is therefore no
    longer needed for these measures: the columns here are the same numbers for every year of HCRIS
    rather than the single 2018 snapshot the code used to read as hospCost2018.

    providerHcrisResidentToBedRatio is the intern and resident to bed ratio (IRB) CMS itself computes for the
    indirect medical education payment, read from Worksheet E Part A (WKSHT_CD E00A18A per Table 2 of the
    electronic reporting specifications, section 4095) line 19 column 1, Current year resident to bed
    ratio. The instructions are section 4030.1 of the same transmittal 18 pdf, line 4 on page 40-170.1
    and lines 18 through 21 on page 40-170.5, and Table 3 of section 4095 lists the cell on page
    40-769 (Table 2 is page 40-719.1): line 19 is line 18 divided by line 4, where line 18 is the adjusted rolling average FTE count,
    the three year average of the allowable FTEs after the IME cap plus the residents of new programs
    and of closed hospitals, and line 4 is Bed Days Available (S-3 Part I column 3, line 14 plus line
    32) less the swing bed, observation, hospice, labor and delivery and COVID-19 expansion days,
    divided by the days in the period. It is therefore NOT providerHcrisResidents over
    providerHcrisBedsTotal: the numerator is capped, averaged and limited to the hospital component
    where providerHcrisResidents is every resident in the facility this year, and the denominator is an
    average over the period net of those days where providerHcrisBedsTotal is the end of period count.
    It is the ratio the major teaching threshold of 0.25 is stated on (see prep_aamcHospitalsDF) and
    what AAMC's FY20 IRB, used by base.add_rbr, was computed from for the one year it covers. Line 20
    is the prior year ratio and line 21 the lesser of the two, which is what the payment formula uses;
    neither is read here. The IME lines are completed only by hospitals paid under the inpatient
    prospective payment system that train residents, so the cell is absent for everyone else. It is set
    to 0 where it is absent and providerHcrisResidents is 0, no residents being a ratio of 0 whatever the
    hospital is, and left null where it is absent and the facility does have residents (a critical
    access or other non-IPPS teaching hospital), since there the ratio exists and the form just does
    not compute it. CMS's Cost Report public use file has no resident to bed ratio column to compare against,
    but it does carry three other Worksheet E Part A cells, Managed Care Simulated Payments (line 3),
    Total IME Payment (line 29) and Allowable DSH Percentage (line 33), and across the 4860 reports
    CostReport_2019_Final.csv shares with HOSP10FY2019 they agree with E00A18A lines 00300, 02900 and
    03300 column 00100 on every report that files them (587, 463 and 1909), which confirms the
    worksheet code and the line numbering. Line 19 itself equals line 18 over line 4 on all 1228
    HOSP10FY2019 reports that file it, ranges from 0.0002 to 2.28 with a median of 0.11 and 360 reports
    at or above 0.25, and runs a median 7% below providerHcrisResidents over providerHcrisBedsTotal,
    within 20% of it on 62% of reports. Of the 6048 reports with a bed count, 4620 get the 0 and 200
    stay null: 52 children's, 50 psychiatric, 28 rehabilitation, 18 critical access, 6 long term care
    and 46 short term acute hospitals, the last mostly with a handful of residents. Unlike the bed
    counts the cell is not checked against another cell, none of the values filed calling for it.

    hcrisYear is the calendar year containing the midpoint of the cost reporting period, not the fiscal
    year of the folder the report came from: the HOSP10FY{year} file groups reports by the federal fiscal
    year their period BEGINS in, so eg HOSP10FY2019 holds periods beginning 10/01/2018 through 09/30/2019
    and ending anywhere in 2018-2020. The midpoint assigns each report to exactly one calendar year and
    lets the result join to the claims on THRU_DT_YEAR the way the AHA data does. Because of this the
    edge years are thin: yearInitial contributes a handful of reports to the calendar year before it, and
    the most recent fiscal years are still being filed, so their calendar years are incomplete.
    lastCompleteYear drops the latter: reports whose hcrisYear is after it are not kept, so a year for
    which only some hospitals have filed cannot be mistaken for a year in which the rest filed nothing.
    From the September 2026 release calendar year 2025 had 3508 reports against the 5900 to 6000 of
    every year from 2016 through 2024, which is why the default is 2024; raise it as CMS fills the later
    years in, or pass None to keep everything. The thin year before yearInitial is not dropped.

    About 1.4% of (PRVDR_NUM, hcrisYear) pairs have more than one report, from a change of ownership or
    a change of fiscal year splitting the year into two short periods. The longest period wins, with the
    highest RPT_REC_NUM breaking ties so the choice is deterministic. hcrisReportDays is kept so a caller can drop the reports
    covering well under a year.

    A report that filed S-3 Part I but no intensive care line reported no intensive care unit, so
    providerHcrisBedsIcu and providerHcrisBedsCriticalCare are 0 rather than null there, the null being
    what summing a block none of whose lines the report filed returns, and providerHcrisBedDaysIcu and
    providerHcrisBedDaysCriticalCare are 0 for the same reason and in the same rows. providerHcrisResidents is 0 on the same
    reasoning: a hospital with no teaching program leaves the cell empty, which is how the public use
    file leaves it too, but no residents is the real value. providerHcrisBedsTotal is left null when its cell is
    absent, since zero total beds is not a real value, as is providerHcrisBedDaysTotal when its own cell is
    absent, and providerHcrisIsRural when the report filed no
    S-2 Part I line 26. The bed and bed days columns are filled independently of each other, so a report can
    carry a count with no bed days beside it, which is the 040011 case above. Reports that filed no S-3 Part I
    bed cell at all (about 1% per year) are dropped rather than recorded as having no beds, which is why
    the bed columns, not the mere presence of a row in the aggregation, decide what the result keeps: a
    report can file the S-2 and resident cells while filing no bed cell.'''

    rptFiles = [pathToHcris + f"/HOSP10FY{year}/HOSP10_{year}_rpt.csv" for year in range(yearInitial, yearFinal+1)]
    nmrcFiles = [pathToHcris + f"/HOSP10FY{year}/HOSP10_{year}_nmrc.csv" for year in range(yearInitial, yearFinal+1)]

    rptDF = (spark.read.schema(hcrisRptSchema).csv(rptFiles)
                  .select(F.col("RPT_REC_NUM"),
                          F.col("PRVDR_NUM"),
                          F.col("RPT_STUS_CD"),
                          F.to_date(F.col("FY_BGN_DT"), "MM/dd/yyyy").alias("FY_BGN_DT"),
                          F.to_date(F.col("FY_END_DT"), "MM/dd/yyyy").alias("FY_END_DT")))

    rptDF = (rptDF.withColumn("hcrisReportDays", F.datediff(F.col("FY_END_DT"), F.col("FY_BGN_DT")) + 1)
                  .withColumn("hcrisYear",
                              F.year(F.date_add(F.col("FY_BGN_DT"),
                                                (F.datediff(F.col("FY_END_DT"), F.col("FY_BGN_DT"))/2).cast('int')))))

    isBedLine = ((F.col("WKSHT_CD")=="S300001") &
                 (F.col("LINE_NUM").between("00800","01299") | (F.col("LINE_NUM")=="01400")))
    isBedCell = isBedLine & (F.col("CLMN_NUM")=="00200")
    isBedDaysCell = isBedLine & (F.col("CLMN_NUM")=="00300")
    isResidentsCell = ((F.col("WKSHT_CD")=="S300001") & (F.col("LINE_NUM")=="02700") & (F.col("CLMN_NUM")=="00900"))
    isRuralCell = ((F.col("WKSHT_CD")=="S200001") & (F.col("LINE_NUM")=="02600") & (F.col("CLMN_NUM")=="00100"))
    isResidentToBedRatioCell = ((F.col("WKSHT_CD")=="E00A18A") & (F.col("LINE_NUM")=="01900") & (F.col("CLMN_NUM")=="00100"))

    icuBlock = F.col("LINE_NUM").between("00800","00899")
    criticalCareBlock = F.col("LINE_NUM").between("00800","01299")
    totalLine = (F.col("LINE_NUM")=="01400")

    def sum_cells(cell, block):
        return F.sum(F.when(cell & block, F.col("ITM_VAL_NUM")))

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

    def beds_checked_against_bed_days(beds, bedDays):
        impliedBeds = F.col(bedDays)/F.col("hcrisReportDays")
        return (F.when((impliedBeds>=1) & (F.col(beds) > 5*impliedBeds), F.round(impliedBeds))
                 .otherwise(F.col(beds)).cast('int'))

    if lastCompleteYear is not None:
        rptDF = rptDF.filter(F.col("hcrisYear") <= lastCompleteYear)

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

    eachProviderYear = (Window.partitionBy("PRVDR_NUM","hcrisYear")
                              .orderBy(F.col("hcrisReportDays").desc(), F.col("RPT_REC_NUM").desc()))

    hcrisDF = (hcrisDF.withColumn("hcrisReportRank", F.row_number().over(eachProviderYear))
                      .filter(F.col("hcrisReportRank")==1)
                      .drop("hcrisReportRank"))

    if filename is not None:
        hcrisDF.coalesce(1).write.mode("overwrite").parquet(filename)

    return hcrisDF

def add_posHospital(posDF):
    '''1 for the POS rows that are hospitals (PRVDR_CTGRY_CD 01, every Medicare certified hospital kind, see
    add_posHospitalType), 0 for the other provider categories (skilled nursing facilities, home health agencies,
    hospices, ...).'''
    posDF = posDF.withColumn("posHospital", F.when(F.col("PRVDR_CTGRY_CD")=="01", 1).otherwise(0))
    return posDF

def add_posHospitalType(posDF):
    '''The kind of hospital a POS hospital row (PRVDR_CTGRY_CD 01) is, read from the last four characters of its CCN as
    CMS assigns them (State Operations Manual chapter 2, section 2779A1,
    https://www.cms.gov/Regulations-and-Guidance/Guidance/Manuals/downloads/som107c02.pdf): acute (0001-0879, short term
    general and specialty hospitals), cah (1300-1399), ltch (2000-2299), rehabilitation (3025-3099), childrens
    (3300-3399), psychiatric (4000-4499), transplant (9800-9899, the transplant center number an approved transplant
    program within a hospital carries alongside the hospital's own CCN), emergency (E in the sixth position, a
    nonparticipating non-Federal emergency hospital, section 2052), federal (F in the sixth position, a nonparticipating
    Federal emergency hospital, eg a VA medical center) and other. Null for non-hospital rows.'''
    tail = F.substring(F.col("PRVDR_NUM"), 3, 4)
    number = F.when(tail.rlike("^[0-9]{4}$"), tail.cast("int"))
    posDF = posDF.withColumn("posHospitalType",
                             F.when(F.col("posHospital") != 1, F.lit(None))
                              .when(tail.endswith("E"), "emergency")
                              .when(tail.endswith("F"), "federal")
                              .when(number.between(1, 879), "acute")
                              .when(number.between(1300, 1399), "cah")
                              .when(number.between(2000, 2299), "ltch")
                              .when(number.between(3025, 3099), "rehabilitation")
                              .when(number.between(3300, 3399), "childrens")
                              .when(number.between(4000, 4499), "psychiatric")
                              .when(number.between(9800, 9899), "transplant")
                              .otherwise("other"))
    return posDF

def prep_posDF(posDF, pathToData=None, filename=None, maxCalls=None, keyFilename=None, activeOnly=False):
    '''Builds the POS parquet that get_data loads (filenames["pos"]) from the raw csv, a one-off run in a notebook:
        rawPosDF = spark.read.csv(pathToData + "/PROVIDER-OF-SERVICES/POS_OTHER_DEC22.csv", header=True)
        posDF = prep_posDF(rawPosDF, pathToData=pathToData, filename=pathToData + "/PROVIDER-OF-SERVICES/pos.parquet")
    Adds posHospital (see add_posHospital), posCah, posShortTerm, posIsRural, posActive, posHospitalType (see
    add_posHospitalType), providerFIPS,
    providerStateFIPS, FAC_NAMEProcessed, posZip and
    posAddress (see geocoding.add_address) and, when pathToData is given, the geocoded coordinates of the hospitals
    (PRVDR_CTGRY_CD 01): posLat, posLng, posGeocodeLocationType, posGeocodeFormattedAddress, posGeocodePartialMatch,
    posGeocodeStatus (see geocoding.add_geocode_info). Only hospitals are geocoded because every distinct address that is
    not in the cache is a paid Google Geocoding API call; the other provider types keep null coordinates. The file keeps
    every provider that ever had a CCN, terminated ones included (PGM_TRMNTN_CD 00 is an active provider, posActive), and
    with activeOnly=True only the active hospitals are geocoded. All rows of the raw file are kept.
    Rebuild the parquet whenever the raw csv is replaced, the address cache makes that cheap. maxCalls (cost guard) and
    keyFilename (where the API key is, default pathToData/GEOCODING/key.csv) are passed to geocode_addresses.
    Layout: https://data.cms.gov/sites/default/files/2022-10/58ee74d6-9221-48cf-b039-5b7a773bf39a/Layout%20Sep%2022%20Other.pdf'''
    posDF = add_posHospital(posDF)
    posDF = (posDF.withColumn("providerStateFIPS", F.col("FIPS_STATE_CD"))
                  .withColumn("providerFIPS",F.concat( F.col("FIPS_STATE_CD"),F.col("FIPS_CNTY_CD")))
                  .withColumn("posCah", F.when( (F.col("posHospital")==1) &  (F.col("PRVDR_CTGRY_SBTYP_CD")=="11"), 1).otherwise(0))
                  .withColumn("posShortTerm",  F.when( (F.col("posHospital")==1) & (F.col("PRVDR_CTGRY_SBTYP_CD")=="01"), 1).otherwise(0))
                  .withColumn("posIsRural", F.when( F.col("CBSA_URBN_RRL_IND")=="R", F.lit(1))
                                             .when( F.col("CBSA_URBN_RRL_IND")=="U", F.lit(0))
                                             .otherwise(F.lit(None))) #there are nulls but also "001" and "041" (very few though)
                  .withColumn("posActive", F.when( F.col("PGM_TRMNTN_CD")=="00", 1).otherwise(0)))
    posDF = add_processed_name(posDF,colToProcess="FAC_NAME")
    posDF = add_posHospitalType(posDF)
    posDF = posDF.withColumn("posZip", F.substring(F.trim(F.col("ZIP_CD")), 1, 5))
    posDF = add_address(posDF, "ST_ADR", "CITY_NAME", "STATE_CD", "posZip", "posAddress")
    if pathToData is not None:
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

jcStrokeProgramRanking = ["comprehensive", "thrombectomy", "primary", "acute stroke ready", "stroke rehabilitation"]
jcGenericNameWords = ["hospital", "hospitals", "medical", "center", "centre", "general", "acute", "care", "the", "inc", "llc",
                      "lp", "ltd", "health", "healthcare", "system", "services", "regional", "community", "of", "and", "a", "an"]
jcPlaceSiteTypes = ["hospital", "medical_center", "medical_clinic", "health"]
nameGenericWords = jcGenericNameWords + ["campus", "hosp", "ctr", "med", "memorial", "st", "saint", "university", "county",
                                        "baptist", "methodist", "mercy", "providence"]

def get_nameTokens(col):
    '''The distinctive words of a facility name as an array Column: lower cased, split on anything that is not a letter
    or digit, generic words removed (nameGenericWords), duplicates removed. "ST MARY MEDICAL CENTER" gives ["mary"].'''
    words = F.split(F.regexp_replace(F.lower(F.coalesce(col, F.lit(""))), r"[^a-z0-9]+", " "), " ")
    return F.array_except(F.array_distinct(words), F.array([F.lit(w) for w in [""] + nameGenericWords]))

def get_nameScore(col1, col2):
    '''How much two facility names agree, as a Column between 0 and 1: the distinctive words they share
    (get_nameTokens) divided by the distinctive words of the shorter name, so 1 means every distinctive word of the
    shorter name is in the other ("Maria Parham Health" and "MARIA PARHAM MEDICAL CENTER"), 0 means nothing in common or
    a name made of generic words only.'''
    tokens1, tokens2 = get_nameTokens(col1), get_nameTokens(col2)
    shorter = F.least(F.size(tokens1), F.size(tokens2))
    return F.when(shorter > 0, F.size(F.array_intersect(tokens1, tokens2)) / shorter).otherwise(F.lit(0.0))

def prep_jcAccreditationDF(jcDF, pathToData=None, filename=None, maxCalls=None, keyFilename=None, topProgramOnly=False,
                           excludePrograms=None, placesLookup=False):
    '''Builds the parquet that get_data loads (filenames["jcAccreditation"]) from the raw joint commission export, a
    one-off run in a notebook:
        rawJcDF = spark.read.csv(pathToData + "/JOINT-COMMISSION/<export>.csv", header=True)
        jcDF = prep_jcAccreditationDF(rawJcDF, pathToData=pathToData, filename=pathToData + "/JOINT-COMMISSION/jcAccreditation.parquet")
    The export has one row per site and accreditation program (HCO ID, Organization Name, Organization Doing Business As
    (DBA) Name, State, City, Street Address, Postal Code, Site Name, Site Doing Business As (DBA) Name, Program, Effective
    Date, Status) and no CCN or NPI, so the sites are located by their address instead: jcSiteName (Site Name, falling
    back to the site DBA name and then the organization name), jcState (the export spells the state out, jcState is the
    usps abbreviation from usStateAbbreviations so it joins the POS STATE_CD; a 2-letter value passes through), jcZip,
    jcAddress (built from the raw State column, which is the geocoding cache key and must not change), jcSearchName and
    jcPlaceQuery (the site's public name, the site DBA name when there is one since the site name is often the legal
    entity, eg "Sutter Bay Hospitals" for eight different hospitals, unless the DBA is only generic words such as
    "Hospital" or "General Acute Care Hospital" (jcGenericNameWords), which would find any hospital, else the site name,
    and that name with the state,
    the Places text search key the address cannot replace because the address is the organization's, not the site's) and,
    when pathToData is given, jcLat, jcLng, jcGeocodeLocationType, jcGeocodeFormattedAddress, jcGeocodePartialMatch,
    jcGeocodeStatus (see geocoding.add_geocode_info). An address repeated across programs is geocoded once.
    With placesLookup=True (and pathToData) the Places text search result for jcPlaceQuery is attached too, biased to
    within 50 km of the address point (see geocoding.add_place_info): jcPlaceLat, jcPlaceLng, jcPlaceName,
    jcPlaceAddress, jcPlaceTypes, jcPlaceStatus, jcPlaceIsHospital (the types include hospital), jcPlaceDistanceKm (how
    far the place is from the address, ie how far the site is from its organization), and the site location to use,
    jcSiteLat, jcSiteLng: the place when it was found and is typed as a care site (jcPlaceSiteTypes: hospital,
    medical_center, medical_clinic, health; a freestanding emergency department or a campus often comes back typed
    medical_clinic), else the address (jcSiteLocationSource is place or address). A place that is not a hospital is
    safe to use as the site point because add_pos_ccn_info falls back to the organization's address when no hospital
    is near it.
    jcProgramRank orders the stroke certifications, matched regardless of case and of extra spaces (the export writes
    "Stroke  Rehabilitation") by "stroke" plus a keyword in jcProgram (jcStrokeProgramRanking, "stroke" keeps eg Primary
    Care Medical Home out): 1 comprehensive, 2 thrombectomy-capable,
    3 primary, 4 acute stroke ready, 5 stroke rehabilitation, null for any other program. With topProgramOnly=True a site (jcHcoId, jcSiteName, jcAddress) keeps
    one row, the one with the best jcProgramRank; a site with only other programs keeps one of them (first by jcProgram
    alphabetically) with a null rank, so no site is lost. excludePrograms is a list of keywords: a row whose jcProgram
    contains one of them (regardless of case) is dropped before that collapse, so eg ["stroke rehabilitation"] removes
    the rehabilitation certification (rank 5, kept in the ranking so the rank values stay stable) and a site that had
    only it. By default all rows are kept. The raw columns are renamed
    (jcHcoId, jcOrganizationName, jcOrganizationDbaName, jcCity, jcStreetAddress, jcPostalCode, jcSiteDbaName,
    jcProgram, jcEffectiveDate, jcStatus) because parquet does not allow spaces and parentheses in column names.
    maxCalls (cost guard) and keyFilename (where the API key is, default pathToData/GEOCODING/key.csv) are passed to
    geocode_addresses.'''
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
    jcDF = add_address(jcDF, "jcStreetAddress", "jcCity", "jcState", "jcZip", "jcAddress")
    jcDF = jcDF.withColumn("jcState", F.coalesce(stateAbbreviation[F.lower(F.trim(F.col("jcState")))],
                                                 F.upper(F.trim(F.col("jcState")))))
    dbaWords = F.split(F.regexp_replace(F.lower(F.coalesce(F.col("jcSiteDbaName"), F.lit(""))), r"[^a-z0-9']+", " "), " ")
    dbaIsSpecific = F.size(F.array_except(dbaWords, F.array([F.lit(w) for w in [""] + jcGenericNameWords]))) > 0
    siteDba = F.when(F.upper(F.trim(F.col("jcSiteDbaName"))).isin("", "N/A") | ~dbaIsSpecific, None).otherwise(F.trim(F.col("jcSiteDbaName")))
    jcDF = (jcDF.withColumn("jcSearchName", F.coalesce(siteDba, F.trim(F.col("jcSiteName"))))
                .withColumn("jcPlaceQuery", F.concat_ws(", ", F.col("jcSearchName"), F.col("jcState"))))
    program = F.regexp_replace(F.lower(F.trim(F.col("jcProgram"))), r"\s+", " ")
    programRank = F.lit(None)
    for rank, keyword in reversed(list(enumerate(jcStrokeProgramRanking, start=1))):
        programRank = F.when(program.contains("stroke") & program.contains(keyword), F.lit(rank)).otherwise(programRank)
    jcDF = jcDF.withColumn("jcProgramRank", programRank.cast("int"))
    for keyword in (excludePrograms or []):
        jcDF = jcDF.filter(~F.coalesce(program.contains(keyword.lower()), F.lit(False)))
    if topProgramOnly:
        eachSite = Window.partitionBy("jcHcoId", "jcSiteName", "jcAddress").orderBy(F.asc_nulls_last("jcProgramRank"), "jcProgram")
        jcDF = jcDF.withColumn("programRow", F.row_number().over(eachSite)).filter(F.col("programRow")==1).drop("programRow")
    if pathToData is not None:
        jcDF = add_geocode_info(jcDF, "jcAddress", pathToData, "jc", maxCalls=maxCalls, keyFilename=keyFilename)
    if placesLookup:
        jcDF = add_place_info(jcDF, "jcPlaceQuery", pathToData, "jc", biasLatCol="jcLat", biasLngCol="jcLng",
                              maxCalls=maxCalls, keyFilename=keyFilename)
        placeTypes = F.split(F.coalesce(F.col("jcPlaceTypes"), F.lit("")), ",")
        isHospital = F.array_contains(placeTypes, "hospital")
        isCareSite = F.size(F.array_intersect(placeTypes, F.array([F.lit(t) for t in jcPlaceSiteTypes]))) > 0
        usePlace = (F.col("jcPlaceStatus") == "OK") & isCareSite
        jcDF = (jcDF.withColumn("jcPlaceIsHospital", F.when(F.col("jcPlaceStatus") == "OK", isHospital.cast("int")))
                    .withColumn("jcPlaceDistanceKm",
                                get_geodesicDistanceKm(F.col("jcLat"), F.col("jcLng"), F.col("jcPlaceLat"), F.col("jcPlaceLng")))
                    .withColumn("jcSiteLat", F.when(usePlace, F.col("jcPlaceLat")).otherwise(F.col("jcLat")))
                    .withColumn("jcSiteLng", F.when(usePlace, F.col("jcPlaceLng")).otherwise(F.col("jcLng")))
                    .withColumn("jcSiteLocationSource", F.when(usePlace, "place").otherwise("address")))
    if filename is not None:
        jcDF.coalesce(1).write.mode("overwrite").parquet(filename)
    return jcDF

def add_pos_nearest_info(jcDF, posDF, latCol="jcLat", lngCol="jcLng", prefix="posNearest"):
    '''For every joint commission site (jcHcoId, jcSiteName, jcAddress, see prep_jcAccreditationDF) the nearest POS
    hospital in the same state by geodesic distance from the site point in latCol/lngCol: {prefix}Ccn, {prefix}FacName
    (for eyeballing only, names play no part), {prefix}DistanceKm, {prefix}Active (see posActive) and
    {prefix}GeocodeLocationType (how precise the hospital's coordinate is), and the runner up, {prefix}SecondCcn,
    {prefix}SecondFacName, {prefix}SecondDistanceKm, {prefix}SecondActive: a second hospital about as close as the
    first (same campus, a successor CCN at the same address) means the nearest one is not certain to be the site.
    Closed hospitals compete too, at equal distance the active one is preferred. posDF is used as given, so filter it
    first (eg to posHospitalType acute and cah). The join is blocked on the state (jcState == STATE_CD) to avoid a
    full cross join, so a site whose nearest hospital is across a state line is shown its nearest in-state hospital
    instead. A site without coordinates, or in a state without geocoded hospitals, keeps nulls; every row of jcDF is
    kept. The prefix lets the lookup run twice, from the site's own point and from its organization's address.'''
    siteKey = ["jcHcoId", "jcSiteName", "jcAddress"]
    ccn, facName, distance, active, geocodeType = (f"{prefix}Ccn", f"{prefix}FacName", f"{prefix}DistanceKm",
                                                    f"{prefix}Active", f"{prefix}GeocodeLocationType")
    secondCcn, secondFacName, secondDistance, secondActive = (f"{prefix}SecondCcn", f"{prefix}SecondFacName",
                                                              f"{prefix}SecondDistanceKm", f"{prefix}SecondActive")
    hospitalsDF = (posDF.filter((F.col("posHospital")==1) & F.col("posLat").isNotNull())
                        .select(F.col("STATE_CD").alias("jcState"),
                                F.col("PRVDR_NUM").alias(ccn),
                                F.col("FAC_NAME").alias(facName),
                                F.col("posActive").alias(active),
                                F.col("posGeocodeLocationType").alias(geocodeType),
                                F.col("posLat"), F.col("posLng")))
    eachSite = Window.partitionBy(siteKey).orderBy(distance, F.desc(active), ccn)
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
    '''Assigns each joint commission site the CCN of the POS hospital it is, from the geocoded locations alone, no
    names and no hand review. In simple words: where is the site? (jcSiteLat/jcSiteLng from prep_jcAccreditationDF:
    the spot Google found for the site's name when that spot is a hospital, else the organization's address); which
    hospital is there? (the nearest hospital of posDF in the same state within maxDistanceKm, and when another one is
    within tieDistanceKm of it the open one is preferred, see add_pos_nearest_info); nothing there? (when the site's
    own spot found nothing, try again from the organization's address: a campus that bills under its parent, such as
    NewYork-Presbyterian Allen under 330101, is listed by CMS only at the parent's address, so that is where it is
    found); still nothing? (as a last resort, and the only place a name is used: the hospital within relaxedDistanceKm
    of the site's spot whose name contains every distinctive word of the shorter of the two names, get_nameScore equal
    to nameScoreMin, ties by distance; this recovers hospitals whose POS coordinate is off, a PO box address, a partial
    geocode or a move to a new building, and on the Dec 2022 data it recovered 18 of 38 unmatched sites with no error,
    while any looser score admitted wrong hospitals); still nothing? (no CCN). Pass posDF filtered to the hospitals
    that can be the site: posHospitalType acute
    and cah, and posActive 1 since the joint commission list is current so every site bills under an open CCN (a
    retired CCN at a campus, eg Presbyterian Hospital 330012 at the Columbia campus, must not win over the parent's). overridesDF, optional, holds hand picked exceptions, rows of jcHcoId, jcSiteName, keepCcn (blank for no
    CCN) that replace the rule's answer.
    Adds posNearest* and posParentNearest* (the two lookups, for inspection), posCcn, posCcnFacName (eyeballing only),
    posCcnDistanceKm, posCcnActive, posCcnPass (site, parent, named, null: which lookup it came from), posMatchMethod
    (nearestSite: found at the spot Google gave for the site's name; nearestParent: found at the organization's address,
    either because the site's own spot had nothing or because the name search gave nothing usable and the address was
    all there was; nearestNamed; override; none), posCcnNameScore (the name score, only for nearestNamed, so
    the name based assignments can be set aside downstream) and posMatchAmbiguous (1 when another hospital was within tieDistanceKm
    of the chosen one, so the choice rested on the tie break). Every row of jcDF is kept; several sites may share a
    CCN (campuses under one Medicare provider agreement). The distance is kept so that a stricter cutoff can be
    applied downstream.'''
    if "jcSiteLat" not in jcDF.columns:
        jcDF = (jcDF.withColumn("jcSiteLat", F.col("jcLat")).withColumn("jcSiteLng", F.col("jcLng"))
                    .withColumn("jcSiteLocationSource", F.lit("address")))
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
                .withColumn("posCcnDistanceKm", pick("posNearestDistanceKm", "posParentNearestDistanceKm"))
                .withColumn("posCcnActive", pick("posNearestActive", "posParentNearestActive"))
                .withColumn("posCcnPass", F.when(F.col("posMatchMethod") == "nearestSite", "site")
                                           .when(F.col("posMatchMethod") == "nearestParent", "parent"))
                .withColumn("posMatchAmbiguous",
                            F.when(F.col("posMatchMethod") == "nearestSite",
                                   (F.col("posNearestSecondDistanceKm") <= tieDistanceKm).cast("int"))
                             .when(F.col("posMatchMethod") == "nearestParent",
                                   (F.col("posParentNearestSecondDistanceKm") <= tieDistanceKm).cast("int")))
                .withColumn("posMatchAmbiguous", F.when(F.col("posCcn").isNotNull(), F.coalesce(F.col("posMatchAmbiguous"), F.lit(0)))))
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
                .withColumn("posCcnNameScore", F.when(named, F.col("namedScore")))
                .drop("namedCcn", "namedFacName", "namedDistanceKm", "namedActive", "namedScore"))
    if overridesDF is not None:
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

def get_ccn_jc_info(jcDF):
    '''One row per CCN that received at least one joint commission site (see add_pos_ccn_info): jcSites (how many
    sites, campuses under one provider agreement), jcBestProgramRank and jcBestProgram (the highest stroke certification
    among them, see prep_jcAccreditationDF), jcBestProgramSite and jcBestProgramMatchMethod (the site that holds that
    certification and how it was assigned to the CCN, the measure of how sure the certification is the CCN's:
    nearestSite, a hospital sits where Google puts the site's name; nearestParent, a campus folded into the parent's
    CCN, so the provider as a whole is credited with a campus's certification; nearestNamed, rests on a name match),
    jcSiteNames, jcMatchMethods (every method among the CCN's sites), jcMaxCcnDistanceKm and jcCertificationConfidence,
    the two folded into one ordinal scale: 4 nearestSite and the CCN's only site, 3 nearestSite but other sites share
    the CCN, 2 nearestParent (or an override), 1 nearestNamed. This is what the claims join on PROVIDER: a stroke
    transfer to any campus of the provider shows the same CCN.'''
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

