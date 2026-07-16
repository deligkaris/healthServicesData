import pyspark.sql.functions as F
from pyspark.sql.window import Window
import re

#Column naming convention (holds codebase-wide, see CLAUDE.md): every raw CMS field is UPPERCASE
#(REV_CNTR, HCPCS_CD, DSYSRTKY -- see cms/schemas.py) and every column we add starts with a lowercase
#character (ed, icu, mri, ct, echo). The case is what marks a column as ours, so the summary below can
#find the flags to collapse to the claim level by regex instead of a hand-maintained list -- a new
#add_* flag then flows into the summary with no other edit. Keep new columns lowercase-initial.

#I think for now I prefer not implementing any filters on revenue records because I am doing an inner join of base with revenue summaries
#maybe I should rethink how to to do the base and revenue summary join
#for now implement filters either on base or on claims

def add_icu(revenueDF, inClaim=False):
    '''
    https://resdac.org/cms-data/variables/intensive-care-unit-icu-indicator-code
    https://resdac.org/cms-data/variables/intensive-care-day-count
    https://www.ncbi.nlm.nih.gov/books/NBK273991/table/sb185.t4/
    https://www.jstor.org/stable/27798363 #they used codes 174 and 175 for ICU also but those might be for neonatal ICU...
    '''
    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        revenueDF = (revenueDF.withColumn("icu",
                                          F.when( (F.col("REV_CNTR")>= 200) & (F.col("REV_CNTR") <= 209) & (F.col("REV_CNTR") != 205) ,1)
                                           .otherwise(0))
                              .withColumn("icuInClaim", F.max(F.col("icu")).over(eachClaim)))
    else:
        revenueDF = revenueDF.withColumn("icu",
                                         F.when( (F.col("REV_CNTR")>= 200) & (F.col("REV_CNTR") <= 209) & (F.col("REV_CNTR") != 205) ,1)
                                          .otherwise(0))
    return revenueDF

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
    '''Keeps only the revenue lines whose claim is present in baseDF.

    Worth doing against a base already cut down to the claims a project actually uses (a diagnosis/provider-type
    filter, say): it drops most revenue lines before they are ever aggregated, which shrinks the aggregation
    shuffle, the summary that gets persisted, and the base-to-summary join downstream. Against a barely-filtered
    base it eliminates almost nothing and only adds work.'''
    #CLAIMNO resets every year, so I need CLAIMNO, DSYSRTKY and THRU_DT to uniquely link base and revenue files
    revenueDF = revenueDF.join(baseDF.select(F.col("CLAIMNO"),F.col("DSYSRTKY"),F.col("THRU_DT")),
                               on=["CLAIMNO","DSYSRTKY","THRU_DT"],
                               how="left_semi")
    return revenueDF

def get_revenue_summary(revenueDF):
    '''Collapses the revenue lines to one row per claim, keyed on DSYSRTKY/CLAIMNO/THRU_DT (CLAIMNO resets
    every year, so THRU_DT is needed to link a claim back to the base file). A claim's flag is 1 when any
    of its lines carries it -- max() over the claim, which is the value the xInClaim columns hold.

    Takes the per-line flags (add_revenue_info with inClaim=False), NOT the xInClaim window columns. The
    two are equivalent, but the groupBy partially aggregates on each executor and shuffles one row per
    claim, whereas the window it replaces had to sort every revenue line of the file and carry the whole
    line-level dataframe through to a distinct() that then threw all but one line per claim away. On the
    national ip revenue file that sort was the most expensive step of the summary.

    The flags to aggregate are found by case, not by a list: every raw CMS field is uppercase and every
    column we add is lowercase-initial (see the convention at the top of this module), so a new add_*
    flag flows into the summary with no edit here. xInClaim columns are excluded -- a caller that already
    ran with inClaim=True also has the per-line flags, so it still summarizes to the same columns.'''
    flags = [c for c in revenueDF.columns
             if re.fullmatch("[a-z][a-zA-Z]*", c) and not c.endswith("InClaim")]
    return (revenueDF.groupBy("DSYSRTKY", "CLAIMNO", "THRU_DT")
                     .agg(*[F.max(F.col(c)).alias(c) for c in flags]))

def add_revenue_info(revenueDF, inClaim=True):
    revenueDF = add_ed(revenueDF, inClaim=inClaim)
    revenueDF = add_mri(revenueDF, inClaim=inClaim)
    revenueDF = add_ct(revenueDF, inClaim=inClaim)
    revenueDF = add_icu(revenueDF, inClaim=inClaim)
    return revenueDF

def get_revenue_info(revenueDF, inClaim=True):
    '''inClaim = True will cause the claim summary to be returned, which is what I need almost always...

    Summarizes every revenue line. To restrict the lines to a subset of claims first, filter the revenue
    file with filter_claims before calling this.'''
    #the summary aggregates the per-line flags to the claim itself, so the xInClaim windows are not computed for it
    revenueDF = add_revenue_info(revenueDF, inClaim=False)
    if inClaim:
        revenueDF = get_revenue_summary(revenueDF)
    return revenueDF
