import pyspark.sql.functions as F
from pyspark.sql.window import Window

#I think for now I prefer not implementing any filters on revenue records because I am doing an inner join of base with revenue summaries
#maybe I should rethink how to to do the base and revenue summary join
#for now implement filters either on base or on claims

def add_ed(revenueDF):

    # https://resdac.org/articles/how-identify-hospital-claims-emergency-room-visits-medicare-claims-data
    # Claims in the Outpatient and Inpatient files are identified via Revenue Center Code 
    # values of 0450-0459 (Emergency room) or 0981 (Professional fees-Emergency room).

    revenueDF = revenueDF.withColumn("ed",
                                     F.when( (F.col("REV_CNTR")>= 450) & (F.col("REV_CNTR") <= 459) ,1)
                                      .when( F.col("REV_CNTR")==981 ,1)
                                      .otherwise(0))

    eachDsysrtkyAndClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO"])

    revenueDF = revenueDF.withColumn("ed", F.max(F.col("ed")).over(eachDsysrtkyAndClaim))

    # this is another approach where I summarize the records and then return a summary, for now I prefer to do the summary in the notebook as needed
    # here we collapse all revenue center records that are part of the same claim to a single revenue center 
    # summary record that includes/summarizes the information we need
    # this allows us during join with the single base claim record to have a single combined base claim - revenue record

    #revenueSummaryDF = (revenueDF
    #                      .select(
    #                         F.col("DSYSRTKY"),F.col("CLAIMNO"),F.col("edClaim"))
    #                      .groupBy(
    #                         F.col("DSYSRTKY"),F.col("CLAIMNO")) #each DSYSRTKY and CLAIMNO may have multiple edClaim, mriClaim, ctClaim values
    #                      .agg(
    #                         #we are only interested if there was a ED visit, MRI scan, or CT scan, hence the use of max()
    #                         F.max(F.col("edClaim")).alias("edClaim")))  #pyspark renames it to max("edClaim"), hence need to alias)

    return revenueDF

def add_mri(revenueDF):

    revenueDF = revenueDF.withColumn("mri", 
                                     F.when( (F.col("REV_CNTR")>= 610) & (F.col("REV_CNTR") <= 619) ,1)
                                      .otherwise(0))

    eachDsysrtkyAndClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO"])

    revenueDF = revenueDF.withColumn("mri", 
                                     F.max(F.col("mri")).over(eachDsysrtkyAndClaim))

    #revenueSummaryDF = (revenueDF
    #                      .select(
    #                         F.col("DSYSRTKY"),F.col("CLAIMNO"),F.col("mriClaim"))
    #                      .groupBy(
    #                         F.col("DSYSRTKY"),F.col("CLAIMNO")) #each DSYSRTKY and CLAIMNO may have multiple edClaim, mriClaim, ctClaim values
    #                      .agg(
    #                         #we are only interested if there was a ED visit, MRI scan, or CT scan, hence the use of max()
    #                         F.max(F.col("mriClaim")).alias("mriClaim")))

    return revenueDF

def add_ct(revenueDF):

    revenueDF = revenueDF.withColumn("ct",
                                    F.when( (F.col("REV_CNTR")>= 350) & (F.col("REV_CNTR") <= 359) ,1)
                                     .otherwise(0))
    eachDsysrtkyAndClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO"])

    revenueDF = revenueDF.withColumn("ct",
                                     F.max(F.col("ct")).over(eachDsysrtkyAndClaim))

    #revenueSummaryDF = (revenueDF
    #                      .select(
    #                         F.col("DSYSRTKY"),F.col("CLAIMNO"),F.col("ctClaim"))
    #                      .groupBy(
    #                         F.col("DSYSRTKY"),F.col("CLAIMNO")) #each DSYSRTKY and CLAIMNO may have multiple edClaim, mriClaim, ctClaim values
    #                      .agg(
    #                         #we are only interested if there was a ED visit, MRI scan, or CT scan, hence the use of max()
    #                         F.max(F.col("ctClaim")).alias("ctClaim")))

    return revenueDF

def add_echo(revenueDF):

    echoCodes = ["93304", "93306", "93307", "93320", "93321", "93312", "93313", "93314","93315", "93316", "93317"]

    echoCondition = '(F.col("HCPCS_CD").isin(echoCodes))'

    revenueDF = revenueDF.withColumn("echo",
                                    F.when( eval(echoCondition) ,1)
                                     .otherwise(0))

    eachDsysrtkyAndClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO"])

    revenueDF = revenueDF.withColumn("echo",
                                     F.max(F.col("echo")).over(eachDsysrtkyAndClaim))

    return revenueDF

def filter_claims(revenueDF, baseDF):

    #CLAIMNO resets every year, so I need CLAIMNO, DSYSRTKY and THRU_DT to uniquely link base and revenue files
    revenueDF = revenueDF.join(baseDF.select(F.col("CLAIMNO"),F.col("DSYSRTKY"),F.col("THRU_DT")),
                               on=["CLAIMNO","DSYSRTKY","THRU_DT"],
                               how="left_semi")
    return revenueDF


