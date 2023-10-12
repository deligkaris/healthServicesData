from cms.schemas import schemas
from cms.longToShortXW import longToShortXW
import cms.line as lineF
import cms.base as baseF
import cms.revenue as revenueF
import cms.mbsf as mbsfF
import pyspark.sql.functions as F
from pyspark.sql.types import StructType
import re
from functools import reduce

yearJKTransition = 2016 #year CMS switched from J to K format

#inputs: claimTypePart eg opBase, snfRevenue etc
#outputs: claimType eg op, snf, etc and claimPart eg Base, Revenue, Line
def get_claimType_claimPart(claimTypePart):

    #claimType refers to ip, op, snf, etc
    claimType = re.match(r'^[a-z]+', claimTypePart).group()

    #claimPart refers to Base, Revenue, Line (does not apply to mbsf claimTypes)
    claimPart = re.match(r'(^[a-z]+)([A-Z][a-z]*)',claimTypePart).group(2) if (claimType!="mbsf") else None

    return (claimType, claimPart)

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

    (claimType, claimPart) = get_claimType_claimPart(claimTypePart)

    df = spark.read.parquet(filename)
    #if DESY_SORT_KEY exists in columns names then mark that as a df that is using the long column names
    if ("DESY_SORT_KEY" in df.columns):
        df = enforce_short_names(df, claimType=claimType, claimPart=claimPart)
    #some columns need to have zeros padded (state and county codes)
    #df = pad_zeros(df, claimType=claimType, claimPart=claimPart)
    #enforce the schema now, dataframes need to have the same schema before doing the unions
    #in some dataframes the first row is a copy of the header, enforce_schema has made the "DSYSRTKY" string of that first row a null value
    #so I need to remove that row, assumes that the DSYSRTKY col is cast to an int in schema.py
    #df = enforce_schema(df, claimType=claimType, claimPart=claimPart).filter(~(F.col("DSYSRTKY").isNull()))
    
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
#outputs: dataframe with state and county codes properly formatted
def pad_zeros(df, claimType, claimPart):

    #note: it is not clear to me why I used a different approach for mbsf and the rest of the files

    #logic for op, ip, snf, hha, hosp, car   
    if (claimPart=="Base"):
        df = (df.withColumn("STATE_CD", F.lpad(F.col("STATE_CD").cast("string"),2,'0'))
                .withColumn("CNTY_CD", F.lpad(F.col("CNTY_CD").cast("string"),3,'0')))
        if (claimType in ["op","ip","snf","hha","hosp"]):
            df = df.withColumn("PRSTATE", F.lpad(F.col("PRSTATE").cast("string"),2,'0'))

    #logic for mbsf
    #some columns need to be converted to ints first (the ones that are now double and that will be converted to strings at the end)
    if (claimType=="mbsf"):
        stCntCols = [f"STATE_CNTY_FIPS_CD_{x:02d}" for x in range(1,13)]
        castToIntCols = stCntCols + ["STATE_CD"]
        df = (df.select([F.col(c).cast('int') if c in castToIntCols else F.col(c) for c in df.columns])
                .withColumn("STATE_CD", 
                            F.when( F.col("STATE_CD").isNull(), F.col("STATE_CD") )
                             .otherwise( F.format_string("%02d",F.col("STATE_CD"))))
                .withColumn("CNTY_CD", 
                            F.when( F.col("CNTY_CD").isNull(), F.col("CNTY_CD") )
                             .otherwise( F.format_string("%03d",F.col("CNTY_CD"))))
                .select([ F.when( ~F.col(c).isNull(), F.format_string("%05d",F.col(c))).alias(c)  if c in stCntCols else F.col(c) for c in df.columns ]))
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

    #logic for op, ip, snf, hha, hosp, car
    for claimTypePart in list(dataframes.keys()):
        (claimType, claimPart) = get_claimType_claimPart(claimTypePart)

        if (claimPart=="Base"):
            dataframes[claimTypePart] = baseF.add_through_date_info(dataframes[claimTypePart], claimType=claimType)
            dataframes[claimTypePart] = baseF.add_ssaCounty(dataframes[claimTypePart])
            if (claimType in ["ip","snf","hosp","hha"]):
                dataframes[claimTypePart] = baseF.add_admission_date_info(dataframes[claimTypePart],claimType=claimType)
            if (claimType=="ip"):
                dataframes[claimTypePart] = baseF.add_discharge_date_info(dataframes[claimTypePart],claimType=claimType)
            if (claimType=="car"):
                 dataframes[claimTypePart] = baseF.add_denied(dataframes[claimTypePart])
        elif (claimPart=="Line"): 
            dataframes[claimTypePart] = lineF.add_level1HCPCS_CD(dataframes[claimTypePart])
            dataframes[claimTypePart] = lineF.add_allowed(dataframes[claimTypePart])
        elif (claimPart=="Revenue"):
            pass

    #logic for mbsf
    dataframes["mbsf"] = mbsfF.add_death_date_info(dataframes["mbsf"])
    dataframes["mbsf"] = mbsfF.add_ssaCounty(dataframes["mbsf"])

    return dataframes

def get_data(pathCMS, yearI, yearF, spark):

    filenames = get_filenames(pathCMS, yearI, yearF)
    dataframes = read_data(spark, filenames, yearI, yearF)
    #dataframes = add_preliminary_info(dataframes)

    return dataframes

