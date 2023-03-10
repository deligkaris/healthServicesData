
def get_filename_dicts(pathToData):

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
 
    return (npiFilename, cbsaFilename, shpCountyFilename, geojsonCountyFilename, usdaErsPeopleFilename, usdaErsJobsFilename,
            usdaErsIncomeFilename, census2021Filename, censusGazetteer2020Filename)

def read_data(spark, 
              npiFilename, 
              cbsaFilename, 
              usdaErsPeopleFilename, usdaErsJobsFilename,usdaErsIncomeFilename, 
              census2021Filename, censusGazetteer2020Filename):

     npiProviders = spark.read.csv(npiFilename, header="True") # read CMS provider information
     cbsa = spark.read.csv(cbsaFilename, header="True") # read CBSA information

     ersPeople = spark.read.csv(usdaErsPeopleFilename, header="True")
     ersJobs = spark.read.csv(usdaErsJobsFilename, header="True")
     ersIncome = spark.read.csv(usdaErsIncomeFilename, header="True")

     census = spark.read.csv(census2021Filename,header=True)
     gazetteer = (spark.read
                       .option("delimiter","\t")
                       .option("inferSchema", "true")
                       .csv(censusGazetteer2020Filename, header=True))

     return (npiProviders, cbsa, ersPeople, ersJobs, ersIncome, census, gazetteer)

def get_cbus_metro_ssa_counties():

    # definition of columbus metro area counties according to US Census bureau 
    # source: https://obamawhitehouse.archives.gov/sites/default/files/omb/bulletins/2013/b13-01.pdf (page 29)
    # city of Columbus may have a different definition
    return ["36250", "36210", "36230", "36460", "36500", "36660", "36810", "36650", "36600","36380"]
