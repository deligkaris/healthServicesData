import pyspark.sql.functions as F
from pyspark.sql.window import Window

def getEdClaims(revenueDF):

    # https://resdac.org/articles/how-identify-hospital-claims-emergency-room-visits-medicare-claims-data
    # Claims in the Outpatient and Inpatient files are identified via Revenue Center Code 
    # values of 0450-0459 (Emergency room) or 0981 (Professional fees-Emergency room).

    revenueDF = revenueDF.withColumn("edClaim",
                                     F.when( 
                                        (F.col("REV_CNTR")>= 450) & (F.col("REV_CNTR") <= 459) ,1)
                                      .when( 
                                        F.col("REV_CNTR")==981 ,1)
                                      .otherwise(0))

    # here we collapse all revenue center records that are part of the same claim to a single revenue center 
    # summary record that includes/summarizes the information we need
    # this allows us during join with the single base claim record to have a single combined base claim - revenue record

    revenueDF = (revenueDF
                   .select(
                      F.col("DSYSRTKY"),F.col("CLAIMNO"),F.col("edClaim"))
                   .groupBy(
                      F.col("DSYSRTKY"),F.col("CLAIMNO")) #each DSYSRTKY and CLAIMNO may have multiple edClaim, mriClaim, ctClaim values
                   .agg(
                      #we are only interested if there was a ED visit, MRI scan, or CT scan, hence the use of max()
                      F.max(F.col("edClaim")).alias("edClaim")))  #pyspark renames it to max("edClaim"), hence need to alias)

    return revenueDF

def getMriClaims(revenueDF):

    revenueDF = revenueDF.withColumn("mriClaim",
                                     F.when( (F.col("REV_CNTR")>= 610) & (F.col("REV_CNTR") <= 619) ,1)
                                      .otherwise(0))

    revenueDF = (revenueDF
                   .select(
                      F.col("DSYSRTKY"),F.col("CLAIMNO"),F.col("mriClaim"))
                   .groupBy(
                      F.col("DSYSRTKY"),F.col("CLAIMNO")) #each DSYSRTKY and CLAIMNO may have multiple edClaim, mriClaim, ctClaim values
                   .agg(
                      #we are only interested if there was a ED visit, MRI scan, or CT scan, hence the use of max()
                      F.max(F.col("mriClaim")).alias("mriClaim")))

    return revenueDF

def getCtClaims(revenueDF):

    revenueDF = revenueDF.withColumn("ctClaim",
                                    F.when( (F.col("REV_CNTR")>= 350) & (F.col("REV_CNTR") <= 359) ,1)
                                     .otherwise(0))

    revenueDF = (revenueDF
                   .select(
                      F.col("DSYSRTKY"),F.col("CLAIMNO"),F.col("ctClaim"))
                   .groupBy(
                      F.col("DSYSRTKY"),F.col("CLAIMNO")) #each DSYSRTKY and CLAIMNO may have multiple edClaim, mriClaim, ctClaim values
                   .agg(
                      #we are only interested if there was a ED visit, MRI scan, or CT scan, hence the use of max()
                      F.max(F.col("ctClaim")).alias("ctClaim")))

    return revenueDF

