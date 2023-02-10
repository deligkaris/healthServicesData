
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
    # https://www.census.gov/data/developers/data-sets/acs-1year.html
    # Exact query: 
    # https://api.census.gov/data/2021/acs/acs1/profile?get=NAME,DP03_0062E,DP03_0009PE,DP02_0068PE,DP05_0001E&for=county:*&in=state:39
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
