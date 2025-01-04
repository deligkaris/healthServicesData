from pyspark.sql.window import Window
import pyspark.sql.functions as F
import .revenue as revenueF

def get_claims(baseDF,summaryDF): #assumes I have already summarized the revenue dataframe
    #CLAIMNO resets every year so need to include the THRU_DT as well
    claimsDF = baseDF.join(summaryDF,
                          on=["DSYSRTKY","CLAIMNO","THRU_DT"],
                          # because we collapsed all revenue records to a single summary revenue record, there should be 1-to-1 match
                          how = "left_outer")
    return claimsDF

def add_provider_revenue_info(staysDF):
    '''Must be called using the revenue summary.'''
    eachProvider = Window.partitionBy(["ORGNPINM","THRU_DT_YEAR"])
    staysDF = (staysDF.withColumn("providerEdMean", F.mean( F.col("ed") ).over(eachProvider))
                      .withColumn("providerEdVol", F.sum( F.col("ed") ).over(eachProvider))
                      .withColumn("providerCtMean", F.mean( F.col("ct") ).over(eachProvider))
                      .withColumn("providerCtVol", F.sum( F.col("ct") ).over(eachProvider))
                      .withColumn("providerMriMean", F.mean( F.col("mri") ).over(eachProvider))
                      .withColumn("providerMriVol", F.sum( F.col("mri") ).over(eachProvider)))
    return staysDF

def propagate_stay_info(claimsDF, claimType="op"):
    '''Assumes claimType either op or ip.
    This function has not been tested for use with snf, hha, or hosp claims.'''
    eachIpStay = Window.partitionBy("DSYSRTKY","PROVIDER", "ORGNPINM", "ADMSN_DT_DAY", "DSCHRGDT_DAY")
    eachOpStay = Window.partitionBy("DSYSRTKY","PROVIDER", "ORGNPINM", "THRU_DT_DAY")
    eachStay = eachIpStay if claimType=="ip" else eachOpStay
    #most columns are 0/1 but nihss is not
    #however, when more than 1 claims are the same stay, they have the same nihss, for 2016 and 17 at least, so this works for nihss too
    columnsToPropagate = ["ishStrokeDgns", "ishStrokeDrg", "ishStroke", "tpaPrcdr", "tpaDgns", "tpaDrg", "tpa", "ccvPrcdr", "evtDrg",
                        "evtPrcdr", "evt", "ed", "mri", "ct", "nihss", "nihssGroup"]
    for col in columnsToPropagate:
        if col in claimsDF.columns: #use in order to apply all claim types
            claimsDF = claimsDF.withColumn(col, F.max(F.col(col)).over(eachStay))
    return claimsDF

def get_unique_stays(claimsDF, claimType="op"):
    '''A facility stay might be broken in more than one claim.
    This function propagates information from all claims that are probably part of the same facility stay
    and returns a single row that represents more accurately the facility stay.'''
    claimsDF = propagate_stay_info(claimsDF, claimType=claimType)
    eachIpStay = Window.partitionBy("DSYSRTKY","PROVIDER", "ORGNPINM", "ADMSN_DT_DAY", "DSCHRGDT_DAY")
    eachOpStay = Window.partitionBy("DSYSRTKY","PROVIDER", "ORGNPINM", "THRU_DT_DAY")
    eachStay = eachIpStay if claimType=="ip" else eachOpStay
    claimsDF = (claimsDF.withColumn("minClaimnoForStay", F.min(F.col("CLAIMNO")).over(eachStay) )
                        .filter( F.col("minClaimnoForStay") == F.col("CLAIMNO") )
                        .drop("minClaimnoForStay"))
    return claimsDF

def get_stays(baseDF, summaryDF, claimType="op", opBase=None, opRevenue=None): 
    '''Inputs are the claims, base and revenue summary, whereas outputs are the stays/visits.
    Each stay/visit might include more then a single claim, so this function attempts to create the dataframe of stay/visits from the claim information.'''
    claimsDF = get_claims(baseDF, summaryDF)
    if (opBase is not None) & (opRevenue is not None):
        claimsDF = update_nonPPS_revenue_info(claimsDF, opBase, opRevenue)
    staysDF = get_unique_stays(claimsDF, claimType=claimType)
    return staysDF

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
