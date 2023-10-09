from .mbsf import prep_mbsfDF
from cms.base import prep_baseDF
from cms.line import prep_lineDF
from cms.SCHEMAS.mbsf_schema import mbsfSchema
from cms.SCHEMAS.ip_schema import ipBaseSchema, ipRevenueSchema
from cms.SCHEMAS.op_schema import opBaseSchema, opRevenueSchema
from cms.SCHEMAS.snf_schema import snfBaseSchema, snfBaseLongToShortXW
from cms.SCHEMAS.hha_schema import hhaBaseSchema, hhaBaseLongToShortXW
from cms.SCHEMAS.hosp_schema import hospBaseSchema, hospBaseLongToShortXW
from cms.SCHEMAS.car_schema import carBaseSchema, carBaseLongToShortXW
from cms.revenue import prep_revenueDF
from cms.schemas import schemas
from cms.longToShortXW import longToShortXW

import re
from functools import reduce

yearJKTransition = 2016 #year CMS switched from J to K format

#inputs: path to the central CMS directory, yearInitial, yearFinal
#outputs: full path filenames that need to be read by spark
def get_filenames(pathCMS, yearI, yearF):

    paths = {"ip": pathCMS + '/INP',
             "op": pathCMS + '/OUT',
             "mbsf": pathCMS + '/Denom',
             "snf": pathCMS + '/SAF/SNF',
             "hha": pathCMS + '/SAF/HHA',
             "hosp": pathCMS + '/SAF/HOSP',
             "car": pathCMS + '/SAF/CAR'}

    # j format lists will be empty if yearInitial > yearJKTransition
    # k format lists include max function in case yearInitial > yearJKTransitio 
    filenames = {"opBase": [paths["op"]+f"/out_claimsj_{year}.parquet" for year in range(yearI, yearJKTransition)] + 
                           [paths["op"]+f"/out_claimsk_{year}.parquet" for year in range(max(yearJKTransition,yearI), yearF+1)],
                 "opRevenue": [paths["op"]+f"/out_revenuej_{year}.parquet" for year in range(yearI, yearJKTransition)] +
                              [paths["op"]+f"/out_revenuek_{year}.parquet" for year in range(max(yearJKTransition,yearI), yearF+1)],      
                 "ipBase": [paths["ip"]+f"/inp_claimsj_{year}.parquet" for year in range(yearI, yearJKTransition)] +
                           [paths["ip"]+f"/inp_claimsk_{year}.parquet" for year in range(max(yearJKTransition,yearI), yearF+1)],           
                 "ipRevenue": [paths["ip"]+f"/inp_revenuej_{year}.parquet" for year in range(yearI, yearJKTransition)] +
                              [paths["ip"]+f"/inp_revenuek_{year}.parquet" for year in range(max(yearJKTransition,yearI), yearF+1)],
                 "mbsf": [paths["mbsf"]+f"/mbsf_{year}.parquet" for year in range(yearI, yearF+1)], 
                 "snfBase": [paths["snf"]+f"/snf_claimsj_{year}.parquet" for year in range(yearI, yearJKTransition)] +
                            [paths["snf"]+f"/snf_claimsk_{year}.parquet" for year in range(max(yearJKTransition,yearI), yearF+1)],
                 "snfRevenue": [paths["snf"]+f"/snf_revenuej_{year}.parquet" for year in range(yearI, yearJKTransition)] +
                               [paths["snf"]+f"/snf_revenuek_{year}.parquet" for year in range(max(yearJKTransition,yearI), yearF+1)],
                 "hhaBase": [paths["hha"]+f"/hha_claimsj_{year}.parquet" for year in range(yearI, yearJKTransition)] +
                            [paths["hha"]+f"/hha_claimsk_{year}.parquet" for year in range(max(yearJKTransition,yearI), yearF+1)],
                 "hhaRevenue": [paths["hha"]+f"/hha_revenuej_{year}.parquet" for year in range(yearI, yearJKTransition)] +
                               [paths["hha"]+f"/hha_revenuek_{year}.parquet" for year in range(max(yearJKTransition,yearI), yearF+1)],
                 "hospBase": [paths["hosp"]+f"/hosp_claimsj_{year}.parquet" for year in range(yearI, yearJKTransition)] +
                             [paths["hosp"]+f"/hosp_claimsk_{year}.parquet" for year in range(max(yearJKTransition,yearI), yearF+1)],
                 "hospRevenue": [paths["hosp"]+f"/hosp_revenuej_{year}.parquet" for year in range(yearI, yearJKTransition)] +
                               [paths["hosp"]+f"/hosp_revenuek_{year}.parquet" for year in range(max(yearJKTransition,yearI), yearF+1)],
                 "carBase": [paths["car"]+f"/car_claimsj_{year}.parquet" for year in range(yearI, yearJKTransition)] +
                            [paths["car"]+f"/car_claimsk_{year}.parquet" for year in range(max(yearJKTransition,yearI), yearF+1)],
                 "carLine": [paths["car"]+f"/car_linej_{year}.parquet" for year in range(yearI, yearJKTransition)] +
                            [paths["car"]+f"/car_linek_{year}.parquet" for year in range(max(yearJKTransition,yearI), yearF+1)]}

    return filenames

def read_data(spark, filenames, yearI, yearF):

    #these columns are the ones that were added during the transition from the J format to the K format
    dropColumns = {"opBase": [],
                   "opRevenue": [],
                   "ipBase": [],
                   "ipRevenue": [],
                   "mbsf": [],
                   "snfBase": [],
                   "snfRevenue": [],
                   "hospBase": [],
                   "hospRevenue": [],
                   "hhaBase": [],
                   "hhaRevenue": [],
                   "carBase": ['CLM_BENE_PD_AMT', 'CPO_PRVDR_NUM', 'CPO_ORG_NPI_NUM', 'CARR_CLM_BLG_NPI_NUM', 'ACO_ID_NUM'],
                   "carLine": ['CARR_LINE_CL_CHRG_AMT', 'LINE_OTHR_APLD_IND_CD1', 'LINE_OTHR_APLD_IND_CD2', 'LINE_OTHR_APLD_IND_CD3', 
                               'LINE_OTHR_APLD_IND_CD4', 'LINE_OTHR_APLD_IND_CD5', 'LINE_OTHR_APLD_IND_CD6', 'LINE_OTHR_APLD_IND_CD7',
                               'LINE_OTHR_APLD_AMT1', 'LINE_OTHR_APLD_AMT2', 'LINE_OTHR_APLD_AMT3', 'LINE_OTHR_APLD_AMT4',
                               'LINE_OTHR_APLD_AMT5', 'LINE_OTHR_APLD_AMT6', 'LINE_OTHR_APLD_AMT7', 'THRPY_CAP_IND_CD1',
                               'THRPY_CAP_IND_CD2', 'THRPY_CAP_IND_CD3', 'THRPY_CAP_IND_CD4', 'THRPY_CAP_IND_CD5', 'CLM_NEXT_GNRTN_ACO_IND_CD1',
                               'CLM_NEXT_GNRTN_ACO_IND_CD2', 'CLM_NEXT_GNRTN_ACO_IND_CD3', 'CLM_NEXT_GNRTN_ACO_IND_CD4', 'CLM_NEXT_GNRTN_ACO_IND_CD5']}

    #claimType refers to ip, op, snf, etc...claimPart refers to base, revenue, line...claimTypePart includes both of these, opBase, carLine, etc
    dataframes = dict()
    for claimTypePart in list(filenames.keys()):
        #the drop function could be used to drop the difference between formats J and K, otherwise the union cannot take place
        #an alternative to the drop, would be to keep all columns, enforce the same schema on the dfs and then do 
        #unionByName with allowMissingColumns=True to fill in with nulls
        #dataframes[claimTypePart] = map(lambda x: spark.read.parquet(x).drop(*dropColumns[claimSubtype]), filenames[claimTypePart])
        dataframes[claimTypePart] = map(lambda x: read_dataframe(x,claimTypePart, spark), filenames[claimTypePart])

        #using reduce might utilize more memory than necessary since it is creating a dataframe with every single union
        #could find a way to do all unions (eg make the string with all unions and then eval) and then at the end return the result
        dataframes[claimTypePart] = reduce(lambda x,y: x.union(y), dataframes[claimTypePart])

    return dataframes

def read_dataframe(filename, claimTypePart, spark):

    claimType = re.match(r'^[a-z]+', claimTypePart).group()
    claimPart = re.match(r'(^[a-z]+)([A-Z][a-z]*)',claimTypePart).group(2)
    df = spark.read.parquet(filename)
    aliasFlag = True if ("DESY_SORT_KEY" in df.columns) else False
    if claimPart == "Base":
        df = enforce_schema_on_base(df, claimType=claimType, aliasFlag=aliasFlag)
    elif claimPart == "Revenue":
        df = enforce_schema_on_revenue(df, claimType=claimType, aliasFlag=aliasFlag)
    elif claimPart == "Line":
        df = enforce_schema_on_line(df, claimType=claimType, aliasFlag=aliasFlag)

    return df

def enforce_schema_on_base(baseDF, claimType, aliasFlag):

    #some columns are read as double or int but they are strings and include leading zeros, so fix this
    #baseDF = cast_columns_as_string(baseDF,claim=claim)
    #exec(f"schema = {claimType}BaseSchema")
    schema = schemas[f"{claimType}Base"]

    if (aliasFlag):
        #exec(f"xw = {claimType}BaseLongToShortXW")
        xw = longToShortXW[f"{claimType}Base"]
        baseDF = baseDF.select([(F.col(field.name).cast(field.dataType)).alias(xw[field.name]) for field in schema.fields])
    else:
        baseDF = baseDF.select([baseDF[field.name].cast(field.dataType) for field in schema.fields])

    #now enforce the schema set for base df
    #if claimType=="ip":
    #    baseDF = baseDF.select([baseDF[field.name].cast(field.dataType) for field in ipBaseSchema.fields])
    #elif claimType=="op":
    #    baseDF = baseDF.select([baseDF[field.name].cast(field.dataType) for field in opBaseSchema.fields])
    #elif claimType=="snf":
    #    baseDF = baseDF.select([(F.col(field.name).cast(field.dataType)).alias(snfBaseLongToShortXW[field.name]) for field in snfBaseSchema.fields])
    #elif claimType=="hha":
    #    baseDF = baseDF.select([(F.col(field.name).cast(field.dataType)).alias(hhaBaseLongToShortXW[field.name]) for field in hhaBaseSchema.fields])
    #elif claimType=="hosp":
    #    baseDF = baseDF.select([(F.col(field.name).cast(field.dataType)).alias(hospBaseLongToShortXW[field.name]) for field in hospBaseSchema.fields])
    #elif claimType=="car":
    #    baseDF = baseDF.select([(F.col(field.name).cast(field.dataType)).alias(carBaseLongToShortXW[field.name]) for field in carBaseSchema.fields])

    return baseDF

def enforce_schema_on_revenue(revenueDF, claimType, aliasFlag):

    if claimType=="ip":
        if (aliasFlag):
            revenueDF = revenueDF.select([(F.col(field.name).cast(field.dataType)).alias(ipRevenueLongToShortXW[field.name]) for field in ipRevenueSchema.fields])
        else:
            revenueDF = revenueDF.select([revenueDF[field.name].cast(field.dataType) for field in ipRevenueSchema.fields])
    elif claimType=="op":
        if (aliasFlag):
            revenueDF = revenueDF.select([(F.col(field.name).cast(field.dataType)).alias(opRevenueLongToShortXW[field.name]) for field in opRevenueSchema.fields])
        else:
            revenueDF = revenueDF.select([revenueDF[field.name].cast(field.dataType) for field in opRevenueSchema.fields])

    return revenueDF

def enforce_schema_on_line(lineDF, claimType, aliasFlag):

    if (claimType=="car"):
        if (aliasFlag):   
            lineDF = lineDF.select([ (F.col(field.name).cast(field.dataType)).alias(carLineLongToShortXW[field.name]) for field in carLineSchema.fields])
        else:
            lineDF = lineDF.select([ (F.col(field.name).cast(field.dataType)) for field in carLineSchema.fields])

    return lineDF

def prep_dfs(dataframes):

    #claimSubtype eg opBase, opRevenue, ipBase....claimType eg op, ip, car....
    for claimSubtype in list(dataframes.keys()):
        claimType = re.match(r'^[a-z]+', claimSubtype).group()
        if (re.match(r'Base$', claimSubtype)): 
            dataframes[claimSubtype] = prep_baseDF(dataframes[claimSubtype], claim=claimType)
        elif (re.match(r'Revenue$', claimSubtype)):
            dataframes[claimSubtype] = prep_revenueDF(dataframes[claimSubtype], claim=claimType)  
        elif (re.match(r'Line$', claimSubtype)):
            dataframes[claimSubtype] = prep_lineDF(dataframes[claimSubtype], claim=claimType)

    return dataframes

def get_data(pathCMS, yearI, yearF, spark):

    filenames = get_filenames(pathCMS, yearI, yearF)
    dataframes = read_data(spark, filenames, yearI, yearF)
    #dataframes = prep_dfs(dataframes)

    return dataframes

