import pyspark.sql.functions as F
from pyspark.sql.window import Window
from urllib.request import urlopen
import json

def get_filename_dicts(pathToData, yearInitial, yearFinal):

    # NPI numbers and other provider information obtained from CMS: https://download.cms.gov/nppes/NPI_Files.html
    npiFilename = pathToData + '/npidata_pfile_20050523-20220807.csv'

    # got this file from https://data.nber.org/data/cbsa-msa-fips-ssa-county-crosswalk.html
    # found it using some insight from https://resdac.org/cms-data/variables/county-code-claim-ssa
    cbsaFilename = pathToData + '/cbsatocountycrosswalk.csv'

    # county level data were obtained from US Census Bureau
    # https://www.census.gov/cgi-bin/geo/shapefiles/index.php
    shpCountyFilename = pathToData + "/tl2021county/tl_2021_us_county.shp"

    
    #county-level data from Plotly
    geojsonCountyFilename = 'https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json'

    # data from USDA ERS
    # https://www.ers.usda.gov/data-products/atlas-of-rural-and-small-town-america/download-the-data/
    usdaErsPeopleFilename = pathToData + "/USDA-ERS/People.csv"
    usdaErsJobsFilename = pathToData + "/USDA-ERS/Jobs.csv"
    usdaErsIncomeFilename = pathToData + "/USDA-ERS/Income.csv"
    # https://www.ers.usda.gov/data-products/rural-urban-continuum-codes.aspx
    usdaErsRuccFilename = pathToData + "/USDA-ERS/ruralurbancodes2013.csv"

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
    census2021Filename = pathToData + "/CENSUS/census-2021-oh.csv"

    # to calculate population density I need to use the Gazetteer file:
    #  https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.2020.html
    censusGazetteer2020Filename = pathToData + "/CENSUS/2020_Gaz_counties_national.csv" #the .txt file does not work

    # these data were obtained from CMS, for now just use the latest data available
    # https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Cost-Reports/Hospital-2010-form
    # https://data.cms.gov/provider-compliance/cost-report/hospital-provider-cost-report
    # https://data.cms.gov/resources/hospital-provider-cost-report-data-dictionary
    hospGme2021Filename = pathToData + '/HOSP10-REPORTS/IME_GME/IME_GME2021.CSV'
    hospCost2018Filename = pathToData + '/HOSP10-REPORTS/COST-REPORTS/2018_CSV_2.csv'

    # a CSV file that JB scraped from the CBI website: https://www.communitybenefitinsight.org
    # has hospital identifiers + hospital size + rural/urban + location + a couple other useful variables…
    # mostly, though, it has a bunch of financial details that aren’t terribly relevant to us. 
    # We could link in census data to get a sense of the socioeconomic region for hospitals as well...
    cbiHospitalsFilename = pathToData + '/COMMUNITY-BENEFIT-INSIGHT/cbiHospitals.csv' # retrieved September 30, 2022 from webpage
    cbiDetailsFilename = pathToData + '/COMMUNITY-BENEFIT-INSIGHT/allHospitalsWithDetails.csv' # obtained from JB September 2022

    # CAUTION: the NPI to CCN and CCN to NPI correspondence is NOT 1-to-1!! (for 1 CCN you have several NPIs and for 1 NPIs you have several CCNs....) 
    # I think that F.col("othpidty")=="6" will give the CCN number, in the description file below it is listed as Medicare OSCAR...
    # https://www.nber.org/research/data/national-provider-identifier-npi-medicare-ccn-crosswalk
    # https://data.nber.org/npi/desc/othpid/desc.txt for a description of what the variables in this file mean
    npiMedicareXwFilename = pathToData + "/npi_medicarexw.csv"

    #https://www.huduser.gov/portal/datasets/usps_crosswalk.html#data
    #county column: 5 digit unique 2000 or 2010 Census county GEOID consisting of state FIPS + county FIPS.
    #zip codes split between counties are listed more than once and their ratios are shown
    zipToCountyFilename = pathToData + "/HUD/ZIP_COUNTY_122021.csv"


    pathMA = pathToData +'/MEDICARE-ADVANTAGE' 

    # https://resdac.org/articles/public-use-sources-managed-care-enrollment-and-penetration-rates
    # https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/MCRAdvPartDEnrolData/MA-State-County-Penetration
    # Rates are posted for all 12 months of the year, I chose July, because it is outside Medicare and MA enrollment periods and at the middle
    # of the non-enrollment periods
    #because there are several MA penetration rate files, put them in a dictionary
    maPenetrationFilenames = {}

    #all filenames will include their absolute paths
    for iYear in range(yearInitial,yearFinal+1): #remember range does not include the last point
        maPenetrationFilenames[f'{iYear}'] = pathMA + f"/State_County_Penetration_MA_{iYear}_07/State_County_Penetration_MA_{iYear}_07_withYear.csv"

    #this set is for Medicare-registered hospitals only, the hospital ID is CCN (I checked) but the documentation does not state that
    #https://data.cms.gov/provider-data/dataset/xubh-q36u
    #in a quick test with inpatient stroke claims, this set was about 98.5% complete, using county names,
    #but for outpatient claims, this set was about 81% complete
    medicareHospitalInfoFilename = pathToData + "/Hospital_General_Information.csv"

    #https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/provider-of-services-file-hospital-non-hospital-facilities
    posFilename = pathToData + "/PROVIDER-OF-SERVICES/POS_OTHER_DEC22.csv"

    #https://www.neighborhoodatlas.medicine.wisc.edu/
    adiFilename = pathToData + "/ATLAS-DISCRIMINATION-INDEX/US_2020_ADI_CensusBlockGroup_v3.2.csv"

    #have permission from AAMC to use this dataset for a single project only
    aamcHospitalsFilename = pathToData + "/AAMC/teachingHospitalRequest-modifiedHeaders.csv"

    #https://apps.acgme.org/ads/Public, I submitted a request to the data retrieval system and I got the data in an email
    acgmeSitesFilename = pathToData + "/ACGME/ParticipatingSiteListingAY20212022.csv"

    acgmeProgramsFilename = pathToData + "/ACGME/ProgramListingAY20212022.csv"

    #https://onlinelibrary.wiley.com/doi/10.1002/emp2.12673
    strokeCentersCamargoFilename = pathToData + "/CAMARGO-GROUP/2018_Stroke_CMS_2023apr-modifiedHeader.csv"

    #joint commission website
    strokeCentersJCFilename = pathToData + "/JOINT-COMMISSION/StrokeCertificationList.csv"

    return (npiFilename, cbsaFilename, shpCountyFilename, geojsonCountyFilename, usdaErsPeopleFilename, usdaErsJobsFilename,
            usdaErsIncomeFilename, usdaErsRuccFilename, census2021Filename, censusGazetteer2020Filename, cbiHospitalsFilename, cbiDetailsFilename,
            hospGme2021Filename, hospCost2018Filename, npiMedicareXwFilename, zipToCountyFilename, maPenetrationFilenames,
            medicareHospitalInfoFilename, posFilename, adiFilename, aamcHospitalsFilename, acgmeSitesFilename, acgmeProgramsFilename,
            strokeCentersCamargoFilename, strokeCentersJCFilename)


def read_data(spark, 
              npiFilename, 
              cbsaFilename, 
              usdaErsPeopleFilename, usdaErsJobsFilename,usdaErsIncomeFilename, usdaErsRuccFilename,
              census2021Filename, censusGazetteer2020Filename,
              cbiHospitalsFilename, cbiDetailsFilename,
              hospGme2021Filename, hospCost2018Filename, npiMedicareXwFilename, zipToCountyFilename, maPenetrationFilenames,
              medicareHospitalInfoFilename, posFilename, adiFilename, aamcHospitalsFilename, acgmeSitesFilename, acgmeProgramsFilename,
              strokeCentersCamargoFilename, strokeCentersJCFilename):

     npiProviders = spark.read.csv(npiFilename, header="True") # read CMS provider information
     cbsa = spark.read.csv(cbsaFilename, header="True") # read CBSA information

     ersPeople = spark.read.csv(usdaErsPeopleFilename, header="True")
     ersJobs = spark.read.csv(usdaErsJobsFilename, header="True")
     ersIncome = spark.read.csv(usdaErsIncomeFilename, header="True")
     ersRucc = spark.read.csv(usdaErsRuccFilename, header="True")

     census = spark.read.csv(census2021Filename,header=True)
     gazetteer = (spark.read
                       .option("delimiter","\t")
                       .option("inferSchema", "true")
                       .csv(censusGazetteer2020Filename, header=True))

     npiMedicareXw = spark.read.csv(npiMedicareXwFilename,header="True")

     hospGme2021 = spark.read.csv(hospGme2021Filename,header="True") # read HOSP cost report data
     hospCost2018 =spark.read.csv(hospCost2018Filename,header="True")

     cbiHospitals =spark.read.csv(cbiHospitalsFilename, header="True") # read CBI information
     cbiDetails = spark.read.csv(cbiDetailsFilename, header="True") # read CBI information

     zipToCounty = spark.read.csv(zipToCountyFilename, header="True")

     #assume the worst...that each type of file includes claims from different years
     maPenetrationYears = sorted(list(maPenetrationFilenames.keys()))

     #one dictionary for each type of file
     maPenetrationDict={}

     #read all data and put them in dictionary
     for iYear in maPenetrationYears:
         maPenetrationDict[f'{iYear}'] = spark.read.csv(maPenetrationFilenames[f'{iYear}'], header="True")

     # merge all previous years in one dataframe
     maPenetration = maPenetrationDict[maPenetrationYears[0]] #initialize here

     if (len(maPenetrationYears) > 1):
        for iYear in maPenetrationYears[1:]:
            maPenetration = maPenetration.union(maPenetrationDict[f'{iYear}']) #and then do union with the rest     

     medicareHospitalInfo = spark.read.csv(medicareHospitalInfoFilename, header="True")

     pos = spark.read.csv(posFilename, header="True")

     adi = spark.read.csv(adiFilename, header="True")

     aamcHospitals = spark.read.csv(aamcHospitalsFilename, header="True")

     acgmeSites = spark.read.csv(acgmeSitesFilename, header="True")

     acgmePrograms = spark.read.csv(acgmeProgramsFilename, header="True")

     strokeCentersCamargo = spark.read.csv(strokeCentersCamargoFilename, header="True")

     strokeCentersJC = spark.read.csv(strokeCentersJCFilename, header="True")

     return (npiProviders, cbsa, ersPeople, ersJobs, ersIncome, ersRucc, census, gazetteer, cbiHospitals, cbiDetails, 
             hospGme2021, hospCost2018, npiMedicareXw, zipToCounty, maPenetration, medicareHospitalInfo, pos, adi, aamcHospitals,
             acgmeSites, acgmePrograms, strokeCentersCamargo, strokeCentersJC)

def get_data(pathToData, yearInitial, yearFinal, spark):

    (npiFilename, cbsaFilename, shpCountyFilename, geojsonCountyFilename, 
    usdaErsPeopleFilename, usdaErsJobsFilename,usdaErsIncomeFilename, usdaErsRuccFilename,
    census2021Filename, censusGazetteer2020Filename, 
    cbiHospitalsFilename, cbiDetailsFilename,
    hospGme2021Filename, hospCost2018Filename, 
    npiMedicareXwFilename, 
    zipToCountyFilename, 
    maPenetrationFilenames,
    medicareHospitalInfoFilename,
    posFilename,
    adiFilename,
    aamcHospitalsFilename,
    acgmeSitesFilename,
    acgmeProgramsFilename,
    strokeCentersCamargoFilename,
    strokeCentersJCFilename) = get_filename_dicts(pathToData, yearInitial, yearFinal)

    (npiProviders, cbsa, 
    ersPeople, ersJobs, ersIncome, ersRucc, 
    census, gazetteer,
    cbiHospitals, cbiDetails, 
    hospGme2021, hospCost2018, 
    npiMedicareXw, 
    zipToCounty, 
    maPenetration,
    medicareHospitalInfo, 
    pos,
    adi,
    aamcHospitals,
    acgmeSites,
    acgmePrograms,
    strokeCentersCarmago,
    strokeCentersJC) = read_data(spark, npiFilename, cbsaFilename, 
                          usdaErsPeopleFilename, usdaErsJobsFilename,usdaErsIncomeFilename, usdaErsRuccFilename,
                          census2021Filename, censusGazetteer2020Filename,
                          cbiHospitalsFilename, cbiDetailsFilename,
                          hospGme2021Filename, hospCost2018Filename, 
                          npiMedicareXwFilename, 
                          zipToCountyFilename,
                          maPenetrationFilenames,
                          medicareHospitalInfoFilename,
                          posFilename,
                          adiFilename, 
                          aamcHospitalsFilename,
                          acgmeSitesFilename,
                          acgmeProgramsFilename,
                          strokeCentersCamargoFilename,
                          strokeCentersJCFilename)

    with urlopen(geojsonCountyFilename) as response:
        counties = json.load(response)

    return (npiProviders, cbsa, counties,
            ersPeople, ersJobs, ersIncome, ersRucc, 
            census, gazetteer,
            cbiHospitals, cbiDetails, 
            hospGme2021, hospCost2018, 
            npiMedicareXw, 
            zipToCounty, 
            maPenetration,
            medicareHospitalInfo, 
            pos,
            adi,
            aamcHospitals,
            acgmeSites,
            acgmePrograms,
            strokeCentersCarmago,
            strokeCentersJC)

def get_cbus_metro_ssa_counties():

    # definition of columbus metro area counties according to US Census bureau 
    # source: https://obamawhitehouse.archives.gov/sites/default/files/omb/bulletins/2013/b13-01.pdf (page 29)
    # city of Columbus may have a different definition
    return ["36250", "36210", "36230", "36460", "36500", "36660", "36810", "36650", "36600","36380"]

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

    #posDF = posDF.withColumn("providerName", F.col("FAC_NAME"))

    posDF = posDF.withColumn("providerFIPS",F.concat( F.col("FIPS_STATE_CD"),F.col("FIPS_CNTY_CD")))

    posDF = posDF.withColumn("hospital",
                             F.when( F.col("PRVDR_CTGRY_CD")=="01", 1)
                              .otherwise(0))

    posDF = posDF.withColumn("cah",  #critical access hospital
                             F.when( F.col("PRVDR_CTGRY_SBTYP_CD")=="11", 1)
                              .otherwise(0))

    posDF = add_processed_name(posDF,colToProcess="FAC_NAME")

    return posDF

def prep_aamcHospitalsDF(aamcHospitalsDF):

    aamcHospitalsDF = (aamcHospitalsDF.withColumn("teachingStatus",
                                                  F.when( F.col("Teaching Status")=="Teaching", "1")
                                                   .when( F.col("Teaching Status")=="Non-Teaching", "0")
                                                   .otherwise(""))
                                      .withColumn("teachingStatus", F.col("teachingStatus").cast('int')))

    aamcHospitalsDF = (aamcHospitalsDF.withColumn("majorTeachingStatus",
                                                  F.when( F.col("Major Teaching Status")=="Major Teaching", "1")
                                                   .when( F.col("Major Teaching Status")=="Other Teaching", "0")
                                                   .when( F.col("Major Teaching Status")=="Non-Teaching", "0")
                                                   .otherwise(""))
                                      .withColumn("majorTeachingStatus", F.col("majorTeachingStatus").cast('int')))

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

def prep_npiProvidersDF(npiProvidersDF):

    npiProvidersDF = add_primaryTaxonomy(npiProvidersDF)

    return npiProvidersDF

def prep_strokeCentersCamargoDF(strokeCentersCamargoDF):

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


