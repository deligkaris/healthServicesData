from pyspark.sql.window import Window
import pyspark.sql.functions as F

def get_claimsDF(baseDF,summaryDF): #assumes I have already summarized the revenue dataframe

    #CLAIMNO resets every year so need to include the THRU_DT as well
    claimsDF = baseDF.join(summaryDF,
                          on=["DSYSRTKY","CLAIMNO","THRU_DT"],
                          # because we collapsed all revenue records to a single summary revenue record, there should be 1-to-1 match
                          how = "left_outer")
    return claimsDF

def add_provider_revenue_info(claimsDF):
    '''Must be called using the revenue summary.'''
    eachProvider = Window.partitionBy(["ORGNPINM","THRU_DT_YEAR"])
    claimsDF = (claimsDF.withColumn("providerEdMean", F.mean( F.col("ed") ).over(eachProvider))
                        .withColumn("providerEdVol", F.sum( F.col("ed") ).over(eachProvider))
                        .withColumn("providerCtMean", F.mean( F.col("ct") ).over(eachProvider))
                        .withColumn("providerCtVol", F.sum( F.col("ct") ).over(eachProvider))
                        .withColumn("providerMriMean", F.mean( F.col("mri") ).over(eachProvider))
                        .withColumn("providerMriVol", F.sum( F.col("mri") ).over(eachProvider)))
    return claimsDF

