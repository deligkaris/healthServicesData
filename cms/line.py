import pyspark.sql.functions as F
from pyspark.sql.window import Window
import re

#Notes from a RESDAC tutorial: https://youtu.be/dwWdZPnSNB4?si=5ChQr4fFMZ9yh_UE&t=2989 and from communication with RESDAC
#
#provider specialty codes may or may not align with board certification of the provider
#there is no requirement to pick the most specific specialty (eg can choose IM for a gastroenterologist)
#Unfortunately, there’s no gold standard for provider specialty as far as Medicare goes. 
#Provider specialty codes are self-reported codes that the provider gives when applying to Medicare - we know these codes can and do change.
#You could explore purchasing the AMA database - I believe that has provider specialty. Although I’m not certain the accuracy of that database.
#The MAC provider specialty should match the NPPES data.
#the provider specialty is self-reported when the physician applies for an NPI via NPPES. 
#The NPPES database does allow for physicians to update their taxonomy, but I don’t believe it’s required 
#and thus it’s not clear how often physicians do.
#The PRVDR_SPCLTY variable is populated by the Medicare Administrative Contractor (MAC) based on the corresponding 
#provider identification number (performing NPI or UPIN), which is pulled from NPPES.
#
#about 20% of carrier claims include at least one line item that CMS denied
#denied codes can be found here: https://www.cms.gov/priorities/innovation/files/x/bundled-payments-for-care-improvement-carrier-file.pdf
#
#RESDAC recommends using both DSYSRTKY and CLAIMNO to identify unique claims, and since CLAIMNO resets every year I also need
#the THRU_DT (RESDAC videos refer to the use of CMS RIF not the LDS ones)
#this is why every window over each claim I make, I include "DSYSRTKY","CLAIMNO","THRU_DT"

def add_level1HCPCS_CD(lineDF):

    #level 1 HCPCS codes do not include any characters, only numbers
    lineDF = lineDF.withColumn("level1HCPCS_CD",
                   F.when( (F.regexp_extract( F.col("HCPCS_CD"), '^[\d]{5}$',0) !=''), F.col("HCPCS_CD").cast('int'))
                    .otherwise( F.lit(None) ))

    return lineDF

def add_pcp(lineDF, inClaim=False):

    pcpCond = 'F.col("HCFASPCL").isin(["01","08","11"])'

    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        lineDF = (lineDF.withColumn("pcp", F.when( eval(pcpCond), 1).otherwise(0))
                        .withColumn("pcpInClaim", F.max(F.col("pcp")).over(eachClaim)))
    else:
        lineDF = lineDF.withColumn("pcp", F.when( eval(pcpCond), 1).otherwise(0))

    return lineDF

def add_pcpFromTaxonomy(lineDF, inClaim=False):

    pcpCond = 'F.col("primaryTaxonomy").isin(["207Q00000X","208D00000X","207R00000X"])'

    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        lineDF = (lineDF.withColumn("pcpFromTaxonomy", F.when( eval(pcpCond), 1).otherwise(0))
                        .withColumn("pcpFromTaxonomyInClaim", F.max(F.col("pcpFromTaxonomy")).over(eachClaim)))
    else:
        lineDF = lineDF.withColumn("pcpFromTaxonomy", F.when( eval(pcpCond), 1).otherwise(0))

    return lineDF

def add_alfVisit(lineDF, inClaim=False): #alf: assisted living facility

    alfVisitCond = '''( ((F.col("level1HCPCS_CD")>=99324)&(F.col("level1HCPCS_CD")<=99328)) |
                        ((F.col("level1HCPCS_CD")>=99334)&(F.col("level1HCPCS_CD")<=99338)) )'''

    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        lineDF = (lineDF.withColumn("alfVisit", F.when( eval(alfVisitCond), 1).otherwise(0))
                        .withColumn("alfVisitInClaim", F.max(F.col("alfVisit")).over(eachClaim)))
    else:
        lineDF = lineDF.withColumn("alfVisit", F.when( eval(alfVisitCond), 1).otherwise(0))

    return lineDF

def add_opVisit(lineDF, inClaim=False):

    opVisitCond = '''( ((F.col("level1HCPCS_CD")>=99201)&(F.col("level1HCPCS_CD")<=99205)) |
                     ((F.col("level1HCPCS_CD")>=99211)&(F.col("level1HCPCS_CD")<=99215)) )'''

    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        lineDF = (lineDF.withColumn("opVisit", F.when( eval(opVisitCond), 1).otherwise(0))
                        .withColumn("opVisitInClaim", F.max(F.col("opVisit")).over(eachClaim)))
    else:
        lineDF = lineDF.withColumn("opVisit", F.when( eval(opVisitCond), 1).otherwise(0))

    return lineDF

def add_neurology(lineDF, inClaim=False):

    neurologyCond = 'F.col("HCFASPCL")=="13"'

    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        lineDF = (lineDF.withColumn("neurology", F.when( eval(neurologyCond), 1).otherwise(0))
                        .withColumn("neurologyInClaim", F.max(F.col("neurology")).over(eachClaim)))
    else:
        lineDF = lineDF.withColumn("neurology", F.when( eval(neurologyCond), 1).otherwise(0))

    return lineDF

def add_neurologyFromTaxonomy(lineDF, inClaim=False):

    neurologyCond = 'F.col("primaryTaxonomy")=="2084N0400X"'

    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        lineDF = (lineDF.withColumn("neurologyFromTaxonomy", F.when( eval(neurologyCond), 1).otherwise(0))
                        .withColumn("neurologyFromTaxonomyInClaim", F.max(F.col("neurologyFromTaxonomy")).over(eachClaim)))
    else:
        lineDF = lineDF.withColumn("neurologyFromTaxonomy", F.when( eval(neurologyCond), 1).otherwise(0))
    
    return lineDF

def add_neuropsychiatry(lineDF, inClaim=False):

    neuropsychiatryCond = 'F.col("HCFASPCL").isin(["62","68","86"])'

    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        lineDF = (lineDF.withColumn("neuropsychiatry", F.when( eval(neuropsychiatryCond), 1).otherwise(0))
                        .withColumn("neuropsychiatryInClaim", F.max(F.col("neuropsychiatry")).over(eachClaim)))
    else:
        lineDF = lineDF.withColumn("neuropsychiatry", F.when( eval(neuropsychiatryCond), 1).otherwise(0))

    return lineDF

def add_neuropsychiatryFromTaxonomy(lineDF, inClaim=False):

    neuropsychiatryCond = 'F.col("primaryTaxonomy")=="2084B0040X"'

    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        lineDF = (lineDF.withColumn("neuropsychiatryFromTaxonomy", F.when( eval(neuropsychiatryCond), 1).otherwise(0))
                        .withColumn("neuropsychiatryFromTaxonomyInClaim", F.max(F.col("neuropsychiatryFromTaxonomy")).over(eachClaim)))
    else:
        lineDF = lineDF.withColumn("neuropsychiatryFromTaxonomy", F.when( eval(neuropsychiatryCond), 1).otherwise(0))

    return lineDF

def add_geriatric(lineDF, inClaim=False):

    geriatricCond = 'F.col("HCFASPCL")=="38"'

    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        lineDF = (lineDF.withColumn("geriatric", F.when( eval(geriatricCond), 1).otherwise(0))
                        .withColumn("geriatricInClaim", F.max(F.col("geriatric")).over(eachClaim)))
    else:
        lineDF = lineDF.withColumn("geriatric", F.when( eval(geriatricCond), 1).otherwise(0))

    return lineDF

def add_geriatricFromTaxonomy(lineDF, inClaim=False):

    geriatricCond = 'F.col("primaryTaxonomy").isin(["207QG0300X","207RG0300X"])'

    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        lineDF = (lineDF.withColumn("geriatricFromTaxonomy", F.when( eval(geriatricCond), 1).otherwise(0))
                        .withColumn("geriatricFromTaxonomyInClaim", F.max(F.col("geriatricFromTaxonomy")).over(eachClaim)))
    else:
        lineDF = lineDF.withColumn("geriatricFromTaxonomy", F.when( eval(geriatricCond), 1).otherwise(0))

    return lineDF

def add_neurologyOpVisit(lineDF, inClaim=False):

    lineDF = add_neurology(lineDF, inClaim=False)
    lineDF = add_opVisit(lineDF, inClaim=False)

    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        lineDF = (lineDF.withColumn("neurologyOpVisit", F.col("neurology")*F.col("opVisit"))
                        .withColumn("neurologyOpVisitInClaim", F.max(F.col("neurologyOpVisit")).over(eachClaim)))
    else:
        lineDF = lineDF.withColumn("neurologyOpVisit", F.col("neurology")*F.col("opVisit"))
                                
    return lineDF

def add_pcpOpVisit(lineDF, inClaim=False):

    lineDF = add_pcp(lineDF)
    lineDF = add_opVisit(lineDF)

    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        lineDF = (lineDF.withColumn("pcpOpVisit", F.col("pcp")*F.col("opVisit"))
                        .withColumn("pcpOpVisitInClaim", F.max(F.col("pcpOpVisit")).over(eachClaim))) 
    else:
        lineDF = lineDF.withColumn("pcpOpVisit", F.col("pcp")*F.col("opVisit"))

    return lineDF

def add_ct(lineDF, inClaim=False):

    ctCond = 'F.col("level1HCPCS_CD").isin([70450,70460,70470])'

    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        lineDF = (lineDF.withColumn("ct", F.when( eval(ctCond), 1).otherwise(0))
                        .withColumn("ctInClaim", F.max(F.col("ct")).over(eachClaim)))
    else:
        lineDF = lineDF.withColumn("ct", F.when( eval(ctCond), 1).otherwise(0))

    return lineDF

def add_mri(lineDF, inClaim=False):

    mriCond = 'F.col("level1HCPCS_CD").isin([70551,70553])'

    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        lineDF = (lineDF.withColumn("mri", F.when( eval(mriCond), 1).otherwise(0))
                        .withColumn("mriInClaim", F.max(F.col("mri")).over(eachClaim)))
    else:
        lineDF = lineDF.withColumn("mri", F.when( eval(mriCond), 1).otherwise(0))

    return lineDF

def add_homeVisit(lineDF, inClaim=False):

    homeVisitCond = '((F.col("level1HCPCS_CD")>=99342)&(F.col("level1HCPCS_CD")<=99350))' 

    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        lineDF = (lineDF.withColumn("homeVisit", F.when( eval(homeVisitCond), 1).otherwise(0))
                        .withColumn("homeVisitInClaim", F.max(F.col("homeVisit")).over(eachClaim)))
    else:
        lineDF = lineDF.withColumn("homeVisit", F.when( eval(homeVisitCond), 1).otherwise(0))

    return lineDF

def add_pcpHomeVisit(lineDF, inClaim=False):

    lineDF = add_pcp(lineDF, inClaim=False)
    lineDF = add_homeVisit(lineDF, inClaim=False)

    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        lineDF = (lineDF.withColumn("pcpHomeVisit", F.col("pcp")*F.col("homeVisit"))
                        .withColumn("pcpHomeVisitInClaim", F.max(F.col("pcpHomeVisit")).over(eachClaim)))
    else:
        lineDF = lineDF.withColumn("pcpHomeVisit", F.col("pcp")*F.col("homeVisit"))

    return lineDF

def add_pcpAlfVisit(lineDF, inClaim=False):

    lineDF = add_pcp(lineDF, inClaim=False)
    lineDF = add_alfVisit(lineDF, inClaim=False)

    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        lineDF = (lineDF.withColumn("pcpAlfVisit", F.col("pcp")*F.col("alfVisit"))
                        .withColumn("pcpAlfVisitInClaim", F.max(F.col("pcpAlfVisit")).over(eachClaim)))
    else:
        lineDF = lineDF.withColumn("pcpAlfVisit", F.col("pcp")*F.col("alfVisit"))

    return lineDF

def add_geriatricOpVisit(lineDF, inClaim=False):

    lineDF = add_geriatric(lineDF, inClaim=False)
    lineDF = add_opVisit(lineDF, inClaim=False)

    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        lineDF = (lineDF.withColumn("geriatricOpVisit", F.col("geriatric")*F.col("opVisit"))
                        .withColumn("geriatricOpVisitInClaim", F.max(F.col("geriatricOpVisit")).over(eachClaim)))
    else:
        lineDF = lineDF.withColumn("geriatricOpVisit", F.col("geriatric")*F.col("opVisit"))

    return lineDF

def add_neuropsychiatryOpVisit(lineDF, inClaim=False):

    lineDF = add_neuropsychiatry(lineDF, inClaim=False)
    lineDF = add_opVisit(lineDF, inClaim=False)

    if (inClaim):
        eachClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO","THRU_DT"])
        lineDF = (lineDF.withColumn("neuropsychiatryOpVisit", F.col("neuropsychiatry")*F.col("opVisit"))
                        .withColumn("neuropsychiatryOpVisitInClaim", F.max(F.col("neuropsychiatryOpVisit")).over(eachClaim)))
    else:
        lineDF = lineDF.withColumn("neuropsychiatryOpVisit", F.col("neuropsychiatry")*F.col("opVisit"))

    return lineDF

def filter_claims(lineDF, baseDF):

    #CLAIMNO resets every year, so I need CLAIMNO, DSYSRTKY and THRU_DT to uniquely link base and line files
    lineDF = lineDF.join(baseDF.select(F.col("CLAIMNO"),F.col("DSYSRTKY"),F.col("THRU_DT")),
                         on=["CLAIMNO","DSYSRTKY","THRU_DT"],
                         how="left_semi")

    return lineDF

def add_allowed(lineDF):

    allowedCond = '(F.col("PRCNGIND")=="A")'
     
    lineDF = lineDF.withColumn("allowed", F.when( eval(allowedCond), 1).otherwise(0))

    return lineDF

def add_primaryTaxonomy(lineDF,npiProvidersDF):

    lineDF = lineDF.join(npiProvidersDF.select(F.col("NPI").alias("PRF_NPI"),F.col("primaryTaxonomy")),
                         on=["PRF_NPI"],
                         how="left_outer")

    return lineDF

def get_line_summary(lineDF):

    #the only summary I think I typically need are the inClaim summaries, and those columns now end with InClaim
    #include a few other columns so that I can link the summary to the base 
    lineSummaryDF = lineDF.select("DSYSRTKY", "CLAIMNO", "THRU_DT", lineDF.colRegex("`^[a-zA-Z]+(InClaim)$`")).distinct()

    #the summary will be joined to base, so the InClaim is no longer needed
    namesWithoutInClaim = [re.sub("InClaim","",x) for x in lineSummaryDF.columns]

    lineSummaryDF = lineSummaryDF.toDF(*namesWithoutInClaim)

    return lineSummaryDF


