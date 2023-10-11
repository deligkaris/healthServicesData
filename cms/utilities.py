from .mbsf import prep_mbsfDF
from cms.base import prep_baseDF
from cms.line import prep_lineDF
from cms.revenue import prep_revenueDF
from cms.schemas import schemas
from cms.longToShortXW import longToShortXW

import pyspark.sql.functions as F
from pyspark.sql.types import StructType
import re
from functools import reduce

yearJKTransition = 2016 #year CMS switched from J to K format

#inputs: path to the central CMS directory, yearInitial, yearFinal
#outputs: full path filenames that will be read by spark, a dictionary
def get_filenames(pathCMS, yearI, yearF):

    paths = {"ip": pathCMS + '/INP',
             "op": pathCMS + '/OUT',
             "mbsf": pathCMS + '/Denom',
             "snf": pathCMS + '/SAF/SNF',
             "hha": pathCMS + '/SAF/HHA',
             "hosp": pathCMS + '/SAF/HOSP',
             "car": pathCMS + '/SAF/CAR'}

    # j format lists will be empty if yearInitial > yearJKTransition
    # k format lists include max function in case yearInitial > yearJKTransition 
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

#inputs: spark instance, filenames dictionary, initial year, final year
#outputs: dataframes dictionary, each value is a spark dataframe that holds rows from yearI to yearF
def read_data(spark, filenames, yearI, yearF):

    dataframes = dict()
    #claimType refers to ip, op, snf, etc...claimPart refers to Base, Revenue, Line...claimTypePart includes both of these, opBase, carLine, etc
    for claimTypePart in list(filenames.keys()):

        #idea is to enforce the same schema on the dfs (so same column names for the same type of data) 
        #and then do unionByName with allowMissingColumns=True to fill in with nulls
        dataframes[claimTypePart] = map(lambda x: read_and_prep_dataframe(x,claimTypePart, spark), filenames[claimTypePart])

        #using reduce might utilize more memory than necessary since it is creating a dataframe with every single union
        #could find a way to do all unions (eg make the string with all unions and then eval) and then at the end return the result
        dataframes[claimTypePart] = reduce(lambda x,y: x.unionByName(y,allowMissingColumns=True), dataframes[claimTypePart])

    return dataframes

#inputs: single filename of specific claim type and part (eg opBase), and spark instance
#outputs: single dataframe with consistent schema and the short column names
def read_and_prep_dataframe(filename, claimTypePart, spark):

    claimType = re.match(r'^[a-z]+', claimTypePart).group() #claimType refers to ip, op, snf, etc
    #claimPart refers to Base, Revenue, Line (does not apply to mbsf claimTypes)
    claimPart = re.match(r'(^[a-z]+)([A-Z][a-z]*)',claimTypePart).group(2) if (claimType!="mbsf") else None
    df = spark.read.parquet(filename)
    #if DESY_SORT_KEY exists in columns names then mark that as a df that is using the long column names
    if ("DESY_SORT_KEY" in df.columns):
        df = enforce_short_names(df, claimType=claimType, claimPart=claimPart)
    #in some dataframes the first row is a copy of the header, I need to remove it, otherwise enforce_schema will introduce nulls (eg cast string to int) 
    df = df.filter(~( (F.col("DSYSRTKY")=="DESY_SORT_KEY") | (F.col("DSYSRTKY")=="DSYSRTKY") ))
    #enforce the schema now, dataframes need to have the same schema before doing the unions
    df = enforce_schema(df, claimType=claimType, claimPart=claimPart)

    return df

#inputs: dataframe that uses long column names
#outputs: dataframe with short column names
def enforce_short_names(df, claimType, claimPart):

    #get the right xw, the xws are a superset of all column names from both versions J and K
    xw = longToShortXW[f"{claimType}{claimPart}"] if (claimType!="mbsf") else longToShortXW["mbsf"]
    #rename
    df = df.select( [(F.col(c).alias(xw[c])) for c in df.columns] )
    return df 

#inputs: dataframe with inferred schema by spark
#outputs: dataframe with schema defined in schema.py
def enforce_schema(df, claimType, claimPart):

    #get the schema, this schema is a superset of the schemas for versions J and K 
    schema = schemas[f"{claimType}{claimPart}"] if (claimType!="mbsf") else schemas["mbsf"]
    #the schema for this df is a subset of the one I just loaded, so adjust it
    schema = StructType( [field for field in schema.fields if field.name in df.columns] )
    #cast right schema
    df = df.select([df[field.name].cast(field.dataType) for field in schema.fields])

    return df

#inputs: original CMS dataframes
#outputs: input dataframes with additional columns appended that are needed almost always
def add_preliminary_info(dataframes):

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
    #dataframes = add_preliminary_info(dataframes)

    return dataframes

