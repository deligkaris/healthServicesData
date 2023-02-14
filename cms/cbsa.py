

def rename_columns(cbsaDF):

    cbsaDF = (cbsaDF.withColumnRenamed("ssacounty","ssaCounty")
                    .withColumnRenamed("fipscounty","fipsCounty")
                    .withColumnRenamed("countyname","countyName"))

    return cbsaDF
