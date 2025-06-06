from pyspark.sql.window import Window
import pyspark.sql.functions as F
import cms.revenue as revenueF
import cms.claims as claimsF

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
    #some claims may have same dsysrtky, provider, npi, admission date, and one of the 2 or more claims will have a null discharge date,
    #perhaps because when the claim was created they did not know the discharge date
    eachIpStay = Window.partitionBy("DSYSRTKY","PROVIDER", "ORGNPINM", "ADMSN_DT_DAY") # "DSCHRGDT_DAY")
    eachOpStay = Window.partitionBy("DSYSRTKY","PROVIDER", "ORGNPINM", "THRU_DT_DAY")
    eachStay = eachIpStay if claimType=="ip" else eachOpStay
    #most columns are 0/1 but nihss is not
    #however, when more than 1 claims are the same stay, they have the same nihss, for 2016 and 17 at least, so this works for nihss too
    columnsToPropagate = ["ishStrokeDgns", "ishStrokeDrg", "ishStroke", "tpaPrcdr", "tpaDgns", "tpaDrg", "tpa", "ccvPrcdr", "evtDrg",
                        "evtPrcdr", "evt", "ed", "mri", "ct", "nihss", "nihssGroup"]
    for col in columnsToPropagate:
        if col in claimsDF.columns: #use in order to apply all claim types
            print(f"Note: Column {col} will be aggregated over all claims that comprise of a single stay/visit.")
            claimsDF = claimsDF.withColumn(col, F.max(F.col(col)).over(eachStay))
    return claimsDF

def get_unique_stays(claimsDF, claimType="op"):
    '''A facility stay might be broken in more than one claim.
    This function propagates information from all claims that are probably part of the same facility stay
    and returns a single row that represents more accurately the facility stay.'''
    claimsDF = propagate_stay_info(claimsDF, claimType=claimType)
    #some claims may have same dsysrtky, provider, npi, admission date, and one of the 2 or more claims will have a null discharge date,
    #perhaps because when the claim was created they did not know the discharge date
    eachIpStay = Window.partitionBy("DSYSRTKY","PROVIDER", "ORGNPINM", "ADMSN_DT_DAY") #, "DSCHRGDT_DAY")
    eachOpStay = Window.partitionBy("DSYSRTKY","PROVIDER", "ORGNPINM", "THRU_DT_DAY")
    eachStay = eachIpStay if claimType=="ip" else eachOpStay
    #since claim numbers reset every year, there is a small probability that two claims will have the same number, I am ok with this
    staysDF = (claimsDF.withColumn("minClaimnoForStay", F.min(F.col("CLAIMNO")).over(eachStay) )
                        .filter( F.col("minClaimnoForStay") == F.col("CLAIMNO") )
                        .drop("minClaimnoForStay"))
    return staysDF

def get_stays(baseDF, summaryDF, claimType="op", opBase=None, opRevenue=None): 
    '''Inputs are the claims, base and revenue summary, whereas outputs are the stays/visits.
    Each stay/visit might include more then a single claim, so this function attempts to create the dataframe of stay/visits from the claim information.'''
    claimsDF = claimsF.get_claims(baseDF, summaryDF)
    if (opBase is not None) & (opRevenue is not None):
        claimsDF = claimsF.update_nonPPS_revenue_info(claimsDF, opBase, opRevenue)
    staysDF = get_unique_stays(claimsDF, claimType=claimType)
    return staysDF

def add_onDayOfFirstStay(staysDF):
    eachDsysrtky = Window.partitionBy("DSYSRTKY")
    staysDF = staysDF.withColumn("onDayOfFirstStay", ( F.col("ADMSN_DT_DAY")==F.min(F.col("ADMSN_DT_DAY")).over(eachDsysrtky) ).cast('int'))
    return staysDF

def add_onDayOfFirstStaySum(staysDF):
    eachDsysrtky=Window.partitionBy("DSYSRTKY")
    staysDF = (staysDF.withColumn("onDayOfFirstStaySum", F.sum(F.col("onDayOfFirstStay")).over(eachDsysrtky)))
    return staysDF

def add_first_stay_info(staysDF):
    '''Column firstStay marks as 1 the stays that were definitely the first stay for that beneficiary and as 0 all other stays,
    including the ones for which we cannot know with this data.'''
    eachDay = Window.partitionBy(["DSYSRTKY","ADMSN_DT"])
    staysDF = add_onDayOfFirstStay(staysDF)
    staysDF = add_onDayOfFirstStaySum(staysDF)
    staysDF = (staysDF.withColumn( "singleDayStay", (F.col("los")==1).cast('int') )
                      .withColumn( "singleDayStaySum", F.sum( F.col("singleDayStay") ).over(eachDay) ) #how many admissions with los=1 on that day
                      .withColumn( "firstStay", F.when( (F.col("onDayOfFirstStay")==1) & (F.col("onDayOfFirstStaySum")==1), 1 ) #definitely first stay
                                                 .when( (F.col("onDayOfFirstStay")==1) & (F.col("onDayOfFirstStaySum")>1) &  #definitely first stay
                                                        (F.col("singleDayStay")==1) & (F.col("singleDayStaySum")==1), 1 ) #since discharged from los=1 first
                                                 .otherwise(0) )) #the value of 0 includes both true 'not first admissions' but also 'impossible to know if first admission'
    return staysDF
                                    

