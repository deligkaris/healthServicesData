import pyspark.sql.functions as F
from pyspark.sql.window import Window
import re

#I think for now I prefer not implementing any filters on revenue records because I am doing an inner join of base with revenue summaries
#maybe I should rethink how to to do the base and revenue summary join
#for now implement filters either on base or on claims

def add_ed(revenueDF, inClaim=False):
    # https://resdac.org/articles/how-identify-hospital-claims-emergency-room-visits-medicare-claims-data
    # Claims in the Outpatient and Inpatient files are identified via Revenue Center Code 
    # values of 0450-0459 (Emergency room) or 0981 (Professional fees-Emergency room).
    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        revenueDF = (revenueDF.withColumn("ed",
                                          F.when( (F.col("REV_CNTR")>= 450) & (F.col("REV_CNTR") <= 459) ,1)
                                           .when( F.col("REV_CNTR")==981 ,1)
                                           .otherwise(0))
                              .withColumn("edInClaim", F.max(F.col("ed")).over(eachClaim)))
    else:
        revenueDF = revenueDF.withColumn("ed",
                                         F.when( (F.col("REV_CNTR")>= 450) & (F.col("REV_CNTR") <= 459) ,1)
                                          .when( F.col("REV_CNTR")==981 ,1)
                                          .otherwise(0))  
    return revenueDF

def add_mri(revenueDF, inClaim=False):
    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        revenueDF = (revenueDF.withColumn("mri", 
                                          F.when( (F.col("REV_CNTR")>= 610) & (F.col("REV_CNTR") <= 619) ,1)
                                           .otherwise(0))
                              .withColumn("mriInClaim", F.max(F.col("mri")).over(eachClaim)))
    else:
        revenueDF = revenueDF.withColumn("mri",
                                         F.when( (F.col("REV_CNTR")>= 610) & (F.col("REV_CNTR") <= 619) ,1)
                                          .otherwise(0))   
    return revenueDF

def add_ct(revenueDF, inClaim=False):
    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"]) 
        revenueDF = (revenueDF.withColumn("ct",
                                          F.when( (F.col("REV_CNTR")>= 350) & (F.col("REV_CNTR") <= 359) ,1)
                                           .otherwise(0))
                              .withColumn("ctInClaim", F.max(F.col("ct")).over(eachClaim)))
    else:
        revenueDF = revenueDF.withColumn("ct",
                                         F.when( (F.col("REV_CNTR")>= 350) & (F.col("REV_CNTR") <= 359) ,1)
                                          .otherwise(0))
    return revenueDF

def add_provider_revenue_info(revenueSummaryDF):
    '''Must be called using the revenue summary.'''
    eachProvider = Window.partitionBy(["ORGNPINM","THRU_DT_YEAR"])
    revenueSummaryDF = (revenueSummaryDF.withColumn("providerEdMean", F.mean( F.col("ed") ).over(eachProvider))
                                        .withColumn("providerEdVol", F.sum( F.col("ed") ).over(eachProvider))
                                        .withColumn("providerCtMean", F.mean( F.col("ct") ).over(eachProvider))
                                        .withColumn("providerCtVol", F.sum( F.col("ct") ).over(eachProvider))
                                        .withColumn("providerMriMean", F.mean( F.col("mri") ).over(eachProvider))
                                        .withColumn("providerMriVol", F.sum( F.col("mri") ).over(eachProvider)))
    return revenueSummaryDF

def add_echo(revenueDF, inClaim=False):
    echoCodes = ["93304", "93306", "93307", "93320", "93321", "93312", "93313", "93314","93315", "93316", "93317"]
    echoCondition = '(F.col("HCPCS_CD").isin(echoCodes))'
    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        revenueDF = (revenueDF.withColumn("echo",
                                          F.when( eval(echoCondition) ,1)
                                           .otherwise(0))
                              .withColumn("echoInClaim", F.max(F.col("echo")).over(eachClaim)))
    else:
        revenueDF = revenueDF.withColumn("echo",
                                          F.when( eval(echoCondition) ,1)
                                           .otherwise(0))
    return revenueDF

def filter_claims(revenueDF, baseDF):
    #CLAIMNO resets every year, so I need CLAIMNO, DSYSRTKY and THRU_DT to uniquely link base and revenue files
    revenueDF = revenueDF.join(baseDF.select(F.col("CLAIMNO"),F.col("DSYSRTKY"),F.col("THRU_DT")),
                               on=["CLAIMNO","DSYSRTKY","THRU_DT"],
                               how="left_semi")
    return revenueDF

def get_revenue_summary(revenueDF):
    #the only summary I think I typically need are the inClaim summaries, and those columns now end with InClaim
    #include a few other columns so that I can link the summary to the base 
    revenueSummaryDF = revenueDF.select("DSYSRTKY", "CLAIMNO", "THRU_DT", revenueDF.colRegex("`^[a-zA-Z]+(InClaim)$`")).distinct()
    #the summary will be joined to base, so the InClaim is no longer needed
    namesWithoutInClaim = [re.sub("InClaim","",x) for x in revenueSummaryDF.columns]
    revenueSummaryDF = revenueSummaryDF.toDF(*namesWithoutInClaim)
    return revenueSummaryDF

def add_revenue_info(revenueDF, inClaim=True):
    revenueDF = add_ed(revenueDF, inClaim=inClaim) 
    revenueDF = add_mri(revenueDF, inClaim=inClaim)
    revenueDF = add_ct(revenueDF, inClaim=inClaim)
    return revenueDF

def get_revenue_info(revenueDF, baseDF, inClaim=True):
    '''inClaim = True will cause the summary to be returned.'''
    revenueDF = filter_claims(revenueDF, baseDF)
    revenueDF = add_revenue_info(revenueDF, inClaim=inClaim)
    if inClaim:
        revenueDF = get_revenue_summary(revenueDF)
        revenueDF = add_provider_revenue_info(revenueDF)
    return revenueDF
