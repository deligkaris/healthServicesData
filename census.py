import pyspark.sql.functions as F

# rename the census codes to something more understandable
def rename_columns(censusDF): 

    censusDF = (censusDF
                    .withColumnRenamed("DP03_0062E","medianHouseholdIncome")
                    .withColumnRenamed("DP03_0009PE","unemploymentRate")
                    .withColumnRenamed("DP02_0068PE","bsOrHigher")
                    .withColumnRenamed("DP05_0001E","population")
                    .withColumn("fipscounty",
            	                       F.concat(
             	                           F.col("state"),
                                           F.col("county"))))
    return censusDF

# find population density (in 1000 citizens per square mile of land)
def add_populationDensity(censusDF, gazetteerDF):

    # include the county land area in square miles
    censusDF = censusDF.join(
                          gazetteerDF.select(
                                         F.col("GEOID"),F.col("ALAND_SQMI")),
                          on=[F.col("fipscounty")==F.col("GEOID")],
                          how="left")

    censusDF = censusDF.drop(F.col("GEOID"))

    # https://www.census.gov/newsroom/blogs/random-samplings/2015/03/understanding-population-density.html
    censusDF = (censusDF.withColumn("populationDensity",
                                          F.col("population")/1000./F.col("ALAND_SQMI")))

    return censusDF

