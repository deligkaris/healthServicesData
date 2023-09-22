from cms.SCHEMAS.car_schema import carLineSchema, carLineLongToShortXW
import pyspark.sql.functions as F
from pyspark.sql.window import Window

def prep_lineDF(lineDF, claim="car"):

    lineDF = clean_line(lineDF, claim=claim)
    lineDF = enforce_schema(lineDF, claim=claim)
    lineDF = add_level1HCPCS_CD(lineDF)

    return lineDF

def clean_line(lineDF, claim="car"):

    #mbsf, op, ip files were cleaned and this line was removed in them, but the rest of the files still include the first row
    #that essentially repeats the column names
    if ( (claim=="car") ):
        lineDF = lineDF.filter(~(F.col("DESY_SORT_KEY")=="DESY_SORT_KEY"))

    return lineDF

def enforce_schema(lineDF, claim="car"):

    if claim=="car":
        lineDF = lineDF.select([ (F.col(field.name).cast(field.dataType)).alias(carLineLongToShortXW[field.name]) for field in carLineSchema.fields])

    return lineDF

def add_level1HCPCS_CD(lineDF):

    lineDF = lineDF.withColumn("level1HCPCS_CD",
                   F.when( (F.regexp_extract( F.col("HCPCS_CD"), '^[\d]{5}$',0) !=''), F.col("HCPCS_CD").cast('int'))
                    .otherwise( F.lit(None) ))

    return lineDF

def add_pcp(lineDF):

    pcpCond = 'F.col("HCFASPCL").isin([1,8,11])'

    eachDsysrtkyAndClaim = Window.partitionBy(["DSYSRTKY","CLAIMNO"])

    lineDF = lineDF.withColumn("pcp", 
                               F.when( eval(pcpCond), 1)
                                .otherwise(0))
    return lineDF

def add_alfVisit(lineDF): #alf: assisted living facility

    alfVisitCond = '''( ((F.col("level1HCPCS_CD")>=99324)&(F.col("level1HCPCS_CD")<=99328)) |
                        ((F.col("level1HCPCS_CD")>=99334)&(F.col("level1HCPCS_CD")<=99338)) )'''

    lineDF = lineDF.withColumn("alfVisit",
                               F.when( eval(alfVisitCond), 1)
                                .otherwise(0))
    return lineDF

def add_opVisit(lineDF):

    opVisitCond = '''( ((F.col("level1HCPCS_CD")>=99201)&(F.col("level1HCPCS_CD")<=99205)) |
                     ((F.col("level1HCPCS_CD")>=99211)&(F.col("level1HCPCS_CD")<=99215)) )'''

    lineDF = lineDF.withColumn("opVisit",
                               F.when( eval(opVisitCond), 1)
                                .otherwise(0))
    return lineDF

def add_neurology(lineDF):

    neurologyCond = 'F.col("HCFASPCL")==13'

    lineDF = lineDF.withColumn("neurology",
                               F.when( eval(neurologyCond), 1)
                                .otherwise(0))
    return lineDF

def add_neuropsychiatry(lineDF):

    neuropsychiatryCond = 'F.col("HCFASPCL")==86'

    lineDF = lineDF.withColumn("neuropsychiatry",
                               F.when( eval(neuropsychiatryCond), 1)
                                .otherwise(0))
    return lineDF

def add_geriatric(lineDF):

    geriatricCond = 'F.col("HCFASPCL")==38'

    lineDF = lineDF.withColumn("geriatric",
                               F.when( eval(geriatricCond), 1)
                                .otherwise(0))
    return lineDF

def add_neurologyOpVisit(lineDF):

    lineDF = add_neurology(lineDF)
    lineDF = add_opVisit(lineDF)

    lineDF = lineDF.withColumn("neurologyOpVisit", F.col("neurology")*F.col("opVisit"))

    return lineDF

def add_pcpOpVisit(lineDF):

    lineDF = add_pcp(lineDF)
    lineDF = add_opVisit(lineDF)

    lineDF = lineDF.withColumn("pcpOpVisit", F.col("pcp")*F.col("opVisit"))

    return lineDF

def add_ct(lineDF):

    ctCond = 'F.col("level1HCPCS_CD").isin([70450,70460,70470])'

    lineDF = lineDF.withColumn("ct",
                                F.when( eval(ctCond), 1)
                                 .otherwise(0))
    return lineDF

def add_mri(lineDF):

    mriCond = 'F.col("level1HCPCS_CD").isin([70551,70553])'

    lineDF = lineDF.withColumn("mri",
                               F.when( eval(mriCond), 1)
                                .otherwise(0))
    return lineDF

def add_homeVisit(lineDF):

    homeVisitCond = '((F.col("level1HCPCS_CD")>=99342)&(F.col("level1HCPCS_CD")<=99350))' 

    lineDF = lineDF.withColumn("homeVisit",
                               F.when( eval(homeVisitCond), 1)
                                .otherwise(0))
    return lineDF

def add_pcpHomeVisit(lineDF):

    lineDF = add_pcp(lineDF)
    lineDF = add_homeVisit(lineDF)

    lineDF = lineDF.withColumn("pcpHomeVisit", F.col("pcp")*F.col("homeVisit"))

    return lineDF

def add_pcpAlfVisit(lineDF):

    lineDF = add_pcp(lineDF)
    lineDF = add_alfVisit(lineDF)

    lineDF = lineDF.withColumn("pcpAlfVisit", F.col("pcp")*F.col("alfVisit"))

    return lineDF

def add_geriatricOpVisit(lineDF):

    lineDF = add_geriatric(lineDF)
    lineDF = add_opVisit(lineDF)

    lineDF = lineDF.withColumn("geriatricOpVisit", F.col("geriatric")*F.col("opVisit"))

    return lineDF

def add_neuropsychiatryOpVisit(lineDF):

    lineDF = add_neuropsychiatry(lineDF)
    lineDF = add_opVisit(lineDF)

    lineDF = lineDF.withColumn("neuropsychiatryOpVisit", F.col("neuropsychiatry")*F.col("opVisit"))

    return lineDF




