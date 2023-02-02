
# rename the census codes to something more understandable
def renameColumns(census): 

	census = (census
          		.withColumnRenamed("DP03_0062E","medianHouseholdIncome")
          		.withColumnRenamed("DP03_0009PE","unemploymentRate")
          		.withColumnRenamed("DP02_0068PE","bsOrHigher")
          		.withColumnRenamed("DP05_0001E","population")
          		.withColumn("fipscounty",
            				F.concat(
                				F.col("state"),
                				F.col("county")
            				)))
 	return census

# find population density (in 1000 citizens per square mile of land)
def getPopulationDensity(census, gazetteer):

	# include the county land area in square miles
	census = census.join(
				gazetteer
                    			.select(
                        			F.col("GEOID"),F.col("ALAND_SQMI")),
                    		on=[F.col("fipscounty")==F.col("GEOID")],
                    		how="left")

	census = census.drop(F.col("GEOID"))

	# https://www.census.gov/newsroom/blogs/random-samplings/2015/03/understanding-population-density.html
	census = (census
          		.withColumn("populationDensity",
                     		F.col("population")/1000./F.col("ALAND_SQMI")))

	return census

