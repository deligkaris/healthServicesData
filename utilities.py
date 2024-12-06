import pyspark.sql.functions as F
from pyspark.sql.window import Window
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
daysInYearsPrior = F.create_map([F.lit(x) for x in chain(*daysInYearsPriorDict.items())])
monthsInYearsPrior = F.create_map([F.lit(x) for x in chain(*monthsInYearsPriorDict.items())])

#definition of which states (fips codes) belong to which region
usRegionFipsCodes = {"west":  ["04", "08", "16", "35", "30", "49", "32", "56", "02", "06", "15", "41", "53"],
                     "south": ["10", "11", "12", "13", "24", "37", "45", "51", "54", "01", "21", "28", "47", "05", "22", "40", "48"],
                     "midwest": ["18", "17", "26", "39", "55", "19", "20", "27", "29", "31", "38", "46"],
                     "northeast": ["09", "23", "25", "33", "44", "50", "34", "36", "42"]}

def get_filenames(pathToData, pathToAHAData, yearInitial, yearFinal):

    filenames = dict()

    #AHRQ compendium of US health systems: https://www.ahrq.gov/chsp/data-resources/compendium.html
    #data exist only for years 2016, 2018, 2020, 2021, I copied the 2016 data to 2017 and the 2018 data to 2019
    filenames["chspHosp"] = [pathToData + f'/CHSP/chsp-hospital-linkage-year{year}.csv' for year in range(2016,2022)]

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
    filenames["census2021"] = [pathToData + "/CENSUS/census-2021-oh.csv"]

    # to calculate population density I need to use the Gazetteer file:
    #  https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.2020.html
    filenames["gazetteer2020"] = [pathToData + "/CENSUS/2020_Gaz_counties_national.csv"] 

    # these data were obtained from CMS, for now just use the latest data available
    # https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Cost-Reports/Hospital-2010-form
    # https://data.cms.gov/provider-compliance/cost-report/hospital-provider-cost-report
    # https://data.cms.gov/resources/hospital-provider-cost-report-data-dictionary
    filenames["hospGme2021"] = [pathToData + '/HOSP10-REPORTS/IME_GME/IME_GME2021.CSV']
    filenames["hospCost2018"] = [pathToData + '/HOSP10-REPORTS/COST-REPORTS/2018_CSV_2.csv'] 

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
    filenames["pos"] = [pathToData + "/PROVIDER-OF-SERVICES/POS_OTHER_DEC22.csv"]
   
    #https://www.neighborhoodatlas.medicine.wisc.edu/
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

    df = spark.read.csv(filename, header=True)
    
    if file=="npi":
        df = prep_npiProvidersDF(df)
    elif file=="maPenetration":
        df = prep_maPenetrationDF(df)
    elif file=="hospCost2018":
        df = prep_hospCostDF(df)
    elif file=="pos":
        df = prep_posDF(df)
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
    elif file=="aha":
        df = prep_ahaDF(df, filename)
    elif file=="chspHosp":
        df = prep_chspHospDF(df, filename)
    elif file=="cmi":
        df = prep_cmiDF(df)
    return df   

def get_data(yearInitial, yearFinal, spark, pathToData='/users/PAS2164/deligkaris/DATA', pathToAHAData='/fs/ess/PAS2164/AHA'):
    '''pathToData: where I keep all non-CMS data, pathToAHAData: where all AHA data are stored'''
    filenames = get_filenames(pathToData, pathToAHAData, yearInitial, yearFinal)
    data = read_data(spark, filenames)
    return data

def prep_chspHospDF(chspHospDF, filename):
    chspYear = int(re.compile(r'year\d{4}').search(filename).group()[4:])
    chspHospDF = (chspHospDF.withColumn("year", F.lit(chspYear)))
    #for reasons unknown, the same CCN, 104079, appears in 2 lines in this file with two different compendium_hospital_id, and all else the same
    #I filter out the line with the ID that is not used at later years
    chspHospDF = chspHospDF.filter(F.col("compendium_hospital_id")!="CHSP00008136")
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
                  .withColumn("ahaBeds", F.col("BDH").cast('int'))           #total facility beds - nursing home beds
                  .withColumn("ahaSize", F.when( F.col("ahaBeds").isNull(), F.lit(None))
                                          .when( F.col("ahaBeds")<100, 0)
                                          .when( (F.col("ahaBeds")>=100)&(F.col("ahaBeds")<400), 1)
                                          .when( F.col("ahaBeds")>=400, 2)
                                          .otherwise(F.lit(None)))
                  .withColumn("FTERES", F.col("FTERES").cast('int'))         #full time equivalent residents and interns
                  .withColumn("LAT", F.col("LAT").cast('double'))
                  .withColumn("LONG", F.col("LONG").cast('double'))
                  .withColumn("ahaResidentToBedRatio", F.col("FTERES")/F.col("ahaBeds"))
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
                                           .otherwise(F.lit(None))))

    if ahaYear > 2016:
        ahaDF = (ahaDF.withColumn("STRCHOS", F.col("STRCHOS").cast('int'))
                      .withColumn("STRCSYS", F.col("STRCSYS").cast('int'))
                      .withColumn("STRCVEN", F.col("STRCVEN").cast('int')))

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

def prep_maPenetrationDF(maPenetrationDF):

    maPenetrationDF = (maPenetrationDF.withColumn("Penetration", 
                                                 F.split( F.trim(F.col("Penetration")), '\.' ).getItem(0).cast('int') )
                                      .withColumn("Year",
                                                  F.col("Year").cast('int')))

    return maPenetrationDF

def prep_hospCostDF(hospCostDF):

    eachCCN = Window.partitionBy("Provider CCN")

    hospCostDF = (hospCostDF.withColumn("maxTotalBedDaysAvailable",
                                       F.max(F.col("Total Bed Days Available").cast('int')).over(eachCCN))
                            .filter(F.col("maxTotalBedDaysAvailable")==(F.col("Total Bed Days Available").cast('int')))
                            .drop("maxTotalBedDaysAvailable"))
    return hospCostDF

def prep_posDF(posDF):
    #https://data.cms.gov/sites/default/files/2022-10/58ee74d6-9221-48cf-b039-5b7a773bf39a/Layout%20Sep%2022%20Other.pdf
    #posDF = posDF.withColumn("providerName", F.col("FAC_NAME"))
    posDF = (posDF.withColumn("providerStateFIPS", F.col("FIPS_STATE_CD"))  
                  .withColumn("providerFIPS",F.concat( F.col("FIPS_STATE_CD"),F.col("FIPS_CNTY_CD")))
                  .withColumn("hospital", F.when( F.col("PRVDR_CTGRY_CD")=="01", 1).otherwise(0))
                  .withColumn("cah", F.when( (F.col("hospital")==1) &  (F.col("PRVDR_CTGRY_SBTYP_CD")=="11"), 1).otherwise(0))
                  .withColumn("shortTerm",  F.when( (F.col("hospital")==1) & (F.col("PRVDR_CTGRY_SBTYP_CD")=="01"), 1).otherwise(0)))
    posDF = add_processed_name(posDF,colToProcess="FAC_NAME")
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
                                 F.trim( F.lower(F.col(colToProcess)) ), "\'s|\&|\.|\,| llc| inc| ltd| lp| lc|\(|\)| program", "") ) #replace with nothing
            .withColumn(processedCol,
                             F.regexp_replace( 
                                 F.col(processedCol) , "-| at | of | for | and ", " ") )  #replace with space
            .withColumn(processedCol,
                             F.regexp_replace(
                                 F.col(processedCol) , " {2,}", " ") )) #replace more than 2 spaces with one space 
                                 
    return DF   

def prep_acgmeSitesDF(acgmeSitesDF):

    acgmeSitesDF = acgmeSitesDF.withColumn("siteZip", 
                                           F.substring(F.trim(F.col("Institution Postal Code")),1,5))

    acgmeSitesDF = acgmeSitesDF.withColumn("siteName", F.col("Institution Name"))

    #acgmeSites do not include any id I can use for linking, so make site name ready for probabilistic matching
    acgmeSitesDF = add_processed_name(acgmeSitesDF, colToProcess="siteName") 

    return acgmeSitesDF

def prep_acgmeProgramsDF(acgmeProgramsDF):

    acgmeProgramsDF = acgmeProgramsDF.withColumn("programZip",
                                           F.substring(F.trim(F.col("Program Postal Code")),1,5))

    acgmeProgramsDF = acgmeProgramsDF.withColumn("programName", 
                                                 F.regexp_replace( 
                                                     F.trim( F.lower(F.col("Program Name")) ), " Program", "") )

    #acgmePrograms do not include any id I can use for linking, so make program name ready for probabilistic matching
    acgmeProgramsDF = add_processed_name(acgmeProgramsDF, colToProcess="programName") 

    return acgmeProgramsDF

def add_acgmeSitesInZip(acgmeSitesDF):

    eachZip = Window.partitionBy("institutionZip")
    acgmeSitesDF = acgmeSitesDF.withColumn("acgmeSitesInZip",
                                           F.collect_set( F.col("institutionNameProcessed")).over(eachZip)) 

    return acgmeSitesDF

def add_acgmeProgramsInZip(acgmeProgramsDF):

    eachZip = Window.partitionBy("programZip")
    acgmeProgramsDF = acgmeProgramsDF.withColumn("acgmeProgramsInZip",
                                           F.collect_set( F.col("programNameProcessed")).over(eachZip)) 

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

    npiProvidersDF = npiProvidersDF.withColumn("codeAndSwitch",
                                               eval(codeAndSwitchCols))

    npiProvidersDF = npiProvidersDF.withColumn("codeAndSwitchPrimary",
                                               F.expr('filter(codeAndSwitch, x -> x[1]=="Y")'))

    npiProvidersDF = npiProvidersDF.withColumn("primaryTaxonomy",
                                               F.flatten(F.col("codeAndSwitchPrimary"))[0])

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

    npiProvidersDF = npiProvidersDF.withColumn("gach",
                                               F.when(eval(gachTaxonomyCondition), 1)
                                                .otherwise(0))

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

def print_partition_sizes(df):
    print(df.withColumn("partID", F.spark_partition_id()).groupBy("partID").count().describe("count").show())

