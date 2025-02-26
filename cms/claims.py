from pyspark.sql.window import Window
import pyspark.sql.functions as F
import cms.revenue as revenueF

def get_claims(baseDF,summaryDF): #assumes I have already summarized the revenue dataframe
    #CLAIMNO resets every year so need to include the THRU_DT as well
    claimsDF = baseDF.join(summaryDF,
                          on=["DSYSRTKY","CLAIMNO","THRU_DT"],
                          # because we collapsed all revenue records to a single summary revenue record, there should be 1-to-1 match
                          how = "left_outer")
    return claimsDF

def update_nonPPS_revenue_info(ipClaimsDF, opBaseDF, opRevenueDF):
    #non-PPS hospitals (PPS_IND == null), eg CAH, do not need to bundle the outpatient ED visit with the inpatient stay
    #so for non-PPS hospitals I need to search in the outpatient file...most of the non-PPS claims are in MD (PRSTATE==21)
    #as a test, doing this for the PPS hospitals (PPS_IND==2) should yield exactly zero
    eachStay = Window.partitionBy(["ORGNPINM","THRU_DT_DAY","DSYSRTKY"])
    opRevenueDFSummary = revenueF.get_revenue_info(opRevenueDF, inClaim=True)
    opClaimsDF = get_claims(opBaseDF,opRevenueDFSummary).filter(F.col("ed")==1)
    ipClaimsDF = (ipClaimsDF.join(opClaimsDF #now bring back to the ip claims the updated information about the non-PPS hospitals (but PPS hospitals also)
                                   .select(F.col("ORGNPINM"),F.col("DSYSRTKY"),
                                           F.col("THRU_DT_DAY").alias("ADMSN_DT_DAY"),
                                           F.max( F.col("ed") ).over(eachStay).alias("oped"),
                                           F.max( F.col("mri") ).over(eachStay).alias("opmri"),
                                           F.max( F.col("ct") ).over(eachStay).alias("opct"))
                                   .distinct(),
                                  on=["ORGNPINM","DSYSRTKY","ADMSN_DT_DAY"],
                                  how="left_outer")
                   .fillna(0, subset=["oped","opmri","opct"])
                   .withColumn("ed", F.when( F.col("PPS_IND").isNull(), ((F.col("ed").cast("boolean"))|(F.col("oped").cast("boolean"))).cast('int'))
                                      .otherwise( F.col("ed") )) #leave PPS hospitals as they were
                   .withColumn("mri", F.when( F.col("PPS_IND").isNull(), ((F.col("mri").cast("boolean"))|(F.col("opmri").cast("boolean"))).cast('int'))
                                       .otherwise( F.col("mri") ))
                   .withColumn("ct", F.when( F.col("PPS_IND").isNull(), ((F.col("ct").cast("boolean")) | ((F.col("opct").cast("boolean")))).cast('int'))
                                      .otherwise( F.col("ct") ))
                   .drop("oped","opmri","opct"))
    return ipClaimsDF
