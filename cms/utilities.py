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
from utilities import yearMin, yearMax, daysInYearsPrior, monthsInYearsPrior

yearJKTransition = 2016 #year CMS switched from J to K format
yearMinWithCMSData = 2012
yearMaxWithCMSData = 2022

def years_within_code_limits(yearI, yearF):
    '''inputs: year initial and final requested
       outputs: boolean if years requested are within limits that the code was designed to operate'''
    return True if ( (yearI>=yearMin) & (yearF<=yearMax) ) else False

def years_within_cms_data_limits(yearI, yearF):
    '''inputs: year initial and final requested
       outputs: boolean if years requested CMS data exists'''
    return True if ( (yearI>=yearMinWithCMSData) & (yearF<=yearMaxWithCMSData) ) else False

def get_claimType_claimPart(claimTypePart):
    '''inputs: claimTypePart eg opBase, snfRevenue etc
       outputs: claimType eg op, snf, etc and claimPart eg Base, Revenue, Line'''
    claimType = re.match(r'^[a-z]+', claimTypePart).group() #claimType refers to ip, op, snf, etc
    claimPart = re.match(r'(^[a-z]+)([A-Z][a-z]*)',claimTypePart).group(2) if (claimType!="mbsf") else None #Base, Revenue, Line (not for mbsf claimTypes)
    return (claimType, claimPart)

def get_filenames(pathCMS, yearI, yearF):
    '''inputs: path to the central CMS directory, yearInitial, yearFinal
       outputs: full path filenames that will be read by spark, a dictionary'''
    paths = {"ip": pathCMS + '/INP',
             "op": pathCMS + '/OUT',
             "mbsf": pathCMS + '/MBSF',
             "snf": pathCMS + '/SNF',
             "hha": pathCMS + '/HHA',
             "hosp": pathCMS + '/HOSP',
             "car": pathCMS + '/CAR'}

    # j format lists will be empty if yearInitial > yearJKTransition
    # k format lists include max function in case yearInitial > yearJKTransition 
    #also without specifying an order of the dict keys, I want mbsf to be the first key for the add_preliminary_info function later on...
    filenames = {"mbsf": [paths["mbsf"]+f"/mbsf_{year}.parquet" for year in range(yearI, yearF+1)],
                 "opBase": [paths["op"]+f"/out_claimsj_{year}.parquet" for year in range(yearI, yearJKTransition)] + 
                           [paths["op"]+f"/out_claimsk_{year}.parquet" for year in range(max(yearJKTransition,yearI), yearF+1)],
                 "opRevenue": [paths["op"]+f"/out_revenuej_{year}.parquet" for year in range(yearI, yearJKTransition)] +
                              [paths["op"]+f"/out_revenuek_{year}.parquet" for year in range(max(yearJKTransition,yearI), yearF+1)],      
                 "ipBase": [paths["ip"]+f"/inp_claimsj_{year}.parquet" for year in range(yearI, yearJKTransition)] +
                           [paths["ip"]+f"/inp_claimsk_{year}.parquet" for year in range(max(yearJKTransition,yearI), yearF+1)],           
                 "ipRevenue": [paths["ip"]+f"/inp_revenuej_{year}.parquet" for year in range(yearI, yearJKTransition)] +
                              [paths["ip"]+f"/inp_revenuek_{year}.parquet" for year in range(max(yearJKTransition,yearI), yearF+1)],
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
                 "carBase": [paths["car"]+f"/car_clmsj_{year}.parquet" for year in range(yearI, yearJKTransition)] +
                            [paths["car"]+f"/car_clmsk_{year}.parquet" for year in range(max(yearJKTransition,yearI), yearF+1)],
                 "carLine": [paths["car"]+f"/car_linej_{year}.parquet" for year in range(yearI, yearJKTransition)] +
                            [paths["car"]+f"/car_linek_{year}.parquet" for year in range(max(yearJKTransition,yearI), yearF+1)]}
    return filenames

def read_data(spark, filenames, yearI, yearF):
    '''inputs: spark instance, filenames dictionary, initial year, final year
       outputs: dataframes dictionary, each value is a spark dataframe that holds rows from yearI to yearF'''
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

def read_and_prep_dataframe(filename, claimTypePart, spark):
    '''inputs: single filename of specific claim type and part (eg opBase), and spark instance
       outputs: single dataframe with consistent schema and the short column names'''
    (claimType, claimPart) = get_claimType_claimPart(claimTypePart)

    df = spark.read.parquet(filename)
    #if DESY_SORT_KEY exists in columns names then mark that as a df that is using the long column names
    if ("DESY_SORT_KEY" in df.columns):
        df = enforce_short_names(df, claimType=claimType, claimPart=claimPart)
    #some columns need to have zeros padded (state and county codes)
    df = pad_zeros(df, claimType=claimType, claimPart=claimPart)
    #mbsf files 2015 and prior have only 2 digits for the RFRNC_YR
    df = fix_year(df, claimType=claimType, claimPart=claimPart)
    #enforce the schema now, dataframes need to have the same schema before doing the unions
    #in some dataframes the first row is a copy of the header, enforce_schema has made the "DSYSRTKY" string of that first row a null value
    #so I need to remove that row, assumes that the DSYSRTKY col is cast to an int in schema.py
    #update 11/11/24: as far as I can tell this is now fixed so I am commenting out the filter
    df = enforce_schema(df, claimType=claimType, claimPart=claimPart)#.filter(~(F.col("DSYSRTKY").isNull()))
    return df

def enforce_short_names(df, claimType, claimPart):
    '''inputs: dataframe that uses long column names
       outputs: dataframe with short column names'''
    #get the right xw, the xws are a superset of all column names from both versions J and K
    xw = longToShortXW[f"{claimType}{claimPart}"] if (claimType!="mbsf") else longToShortXW["mbsf"]
    #rename
    df = df.select( [(F.col(c).alias(xw[c])) for c in df.columns] )
    return df 

def pad_zeros(df, claimType, claimPart):
    '''inputs: dataframe with inferred schema by spark
       outputs: dataframe with state and county codes properly formatted'''
    #some times the state, county columns are inferred as double (eg 33.0), or even worse as double cast as strings ("33.0")
    #so I need to cast those columns as ints first, to be on the safe side, and then as strings to be processed by lpad 

    if (claimPart=="Base"): #logic for op, ip, snf, hha, hosp, car 
        df = (df.withColumn("STATE_CD", F.lpad(F.col("STATE_CD").cast('int').cast("string"),2,'0'))
                .withColumn("CNTY_CD", F.lpad(F.col("CNTY_CD").cast('int').cast("string"),3,'0')))
        if (claimType in ["op","ip","snf","hha","hosp"]):
            df = df.withColumn("PRSTATE", F.lpad(F.col("PRSTATE").cast('int').cast("string"),2,'0'))

    if (claimType=="mbsf"): #logic for mbsf
        stCntCols = [f"STATE_CNTY_FIPS_CD_{x:02d}" for x in range(1,13)]
        df = (df.withColumn("STATE_CD", F.lpad(F.col("STATE_CD").cast('int').cast("string"),2,'0'))
                .withColumn("CNTY_CD", F.lpad(F.col("CNTY_CD").cast('int').cast("string"),3,'0'))
                .select( [F.lpad(F.col(c).cast('int').cast("string"),5,'0').alias(c) if c in stCntCols else F.col(c) for c in df.columns] ))
                #old approach, using format_string, I doubt when-otherwise was needed, I think I was trying to be explicit....
                #.withColumn("STATE_CD", 
                #            F.when( F.col("STATE_CD").isNull(), F.col("STATE_CD") )
                #             .otherwise( F.format_string("%02d",F.col("STATE_CD"))))
                #.withColumn("CNTY_CD", 
                #            F.when( F.col("CNTY_CD").isNull(), F.col("CNTY_CD") )
                #             .otherwise( F.format_string("%03d",F.col("CNTY_CD"))))
                #.select([ F.when( ~F.col(c).isNull(), F.format_string("%05d",F.col(c))).alias(c)  if c in stCntCols else F.col(c) for c in df.columns ]))
    return df  

def fix_year(df, claimType, claimPart):
    '''inputs: df, mbsf, with incomplete RFRNC_YR (eg 15)
       outputs: df, mbsf, with RFRNC_YR complete (eg 2015)'''
    if (claimType=="mbsf"):
        df = (df.withColumn("lengthRFRNC_YR", F.length(F.col("RFRNC_YR").cast('string')))
                .withColumn("RFRNC_YR",
                            F.when( F.col("lengthRFRNC_YR")==2, F.concat(F.lit("20"), F.col("RFRNC_YR").cast('string')))
                             .otherwise(F.col("RFRNC_YR").cast('string')))
                .withColumn("RFRNC_YR", F.col("RFRNC_YR").cast('int')))
    return df

def enforce_schema(df, claimType, claimPart):
    '''inputs: dataframe with inferred schema by spark
       outputs: dataframe with schema defined in schema.py'''
    #get the schema, this schema is a superset of the schemas for versions J and K 
    schema = schemas[f"{claimType}{claimPart}"] if (claimType!="mbsf") else schemas["mbsf"]
    #the schema for this df is a subset of the one I just loaded, so adjust it
    schema = StructType( [field for field in schema.fields if field.name in df.columns] )
    #cast right schema
    df = df.select([df[field.name].cast(field.dataType) for field in schema.fields])
    return df

def add_preliminary_info(dataframes, data):
    '''inputs: original CMS dataframes
       outputs: input dataframes with additional columns appended that are needed almost always'''
    for claimTypePart in list(dataframes.keys()):
        (claimType, claimPart) = get_claimType_claimPart(claimTypePart)
        if (claimPart=="Base"):
            dataframes[claimTypePart] = add_through_date_info(dataframes[claimTypePart])
            dataframes[claimTypePart] = baseF.add_ssaCounty(dataframes[claimTypePart])
            if (claimType in ["ip","snf","hosp","hha"]):
                dataframes[claimTypePart] = baseF.add_admission_date_info(dataframes[claimTypePart],claimType=claimType)
            if (claimType=="ip"):
                dataframes[claimTypePart] = baseF.add_discharge_date_info(dataframes[claimTypePart],claimType=claimType)
                dataframes[claimTypePart] = baseF.add_prcdrCodeAll(dataframes[claimTypePart])
                dataframes[claimTypePart] = baseF.add_dgnsCodeAll(dataframes[claimTypePart])
                dataframes[claimTypePart] = baseF.add_provider_info(dataframes[claimTypePart], data)
                dataframes[claimTypePart] = baseF.add_los(dataframes[claimTypePart])
                #assumes that mbsf has already been processed here
                dataframes[claimTypePart] = baseF.add_beneficiary_info(dataframes[claimTypePart], dataframes["mbsf"], data, claimType="ip")
            if (claimType=="car"):
                 dataframes[claimTypePart] = baseF.add_denied(dataframes[claimTypePart])
            if (claimType=="op"):
                 dataframes[claimTypePart] = baseF.add_prcdrCodeAll(dataframes[claimTypePart])
                 dataframes[claimTypePart] = baseF.add_dgnsCodeAll(dataframes[claimTypePart])
                 dataframes[claimTypePart] = baseF.add_provider_info(dataframes[claimTypePart], data)
        elif (claimPart=="Line"): 
            dataframes[claimTypePart] = lineF.add_level1HCPCS_CD(dataframes[claimTypePart])
            dataframes[claimTypePart] = lineF.add_allowed(dataframes[claimTypePart])
            dataframes[claimTypePart] = add_through_date_info(dataframes[claimTypePart])
        elif (claimPart=="Revenue"):
            pass
        elif (claimPart is None):
            if (claimType=="mbsf"):
                dataframes[claimType] = mbsfF.add_death_date_info(dataframes[claimType])
                dataframes[claimType] = mbsfF.add_ssaCounty(dataframes[claimType])
                dataframes[claimType] = mbsfF.add_enrollment_info(dataframes[claimType])
                dataframes[claimType] = mbsfF.add_willDie(dataframes[claimType])
        else:
            pass
    #this needs to be done when all dfs have been processed
    dataframes["mbsf"] = mbsfF.add_probablyDead(dataframes["mbsf"], dataframes["ipBase"], dataframes["opBase"], dataframes["snfBase"],
                                                dataframes["hospBase"], dataframes["hhaBase"])
    return dataframes

def get_cms_data(pathCMS, yearI, yearF, spark, data, FFS=True, cleanMbsf=True, runTests=False, cleanThroughDates=True):
    """The FFS flag is here to prevent unintended errors.
    If the project requires FFS data, then all dataframes must be filtered for FFS.
    data is a dictionary of spark dataframes with supporting information, eg provider of services, NPI, etc"""
    if ( years_within_code_limits(yearI, yearF)==False ):
        raise ValueError("code was not designed to operate for the years provided")
    elif( years_within_cms_data_limits(yearI, yearF)==False ):
        raise ValueError("CMS data not available for the years requested")
    else:
        filenames = get_filenames(pathCMS, yearI, yearF)
        dataframes = read_data(spark, filenames, yearI, yearF)
        if cleanMbsf: #need to clean mbsf prior to using it to add information to base claims in add_preliminary_info
            dataframes["mbsf"] = mbsfF.prep_mbsf(dataframes["mbsf"])
        dataframes = add_preliminary_info(dataframes, data)
        if FFS: 
            dataframes = filter_FFS(dataframes)
        dataframes["mbsf"] = mbsfF.drop_unused_columns(dataframes["mbsf"])
        dataframes = repartition_dfs(dataframes)
        if runTests:
           run_cms_data_tests(dataframes)
        if cleanThroughDates:
           dataframes = clean_through_dates(dataframes)
    return dataframes

def filter_FFS(dataframes):
    """Filter for FFS, continuous FFS and continuous RFNRC_YR the mbsf dataframe, and then according to that
    filter all base dataframes, and finally use the base dataframes to filter revenue and line dataframes."""
    dataframes["mbsf"]=mbsfF.filter_FFS(dataframes["mbsf"])
    dataframes["mbsf"]=mbsfF.filter_continuousFfs(dataframes["mbsf"])
    dataframes["mbsf"]=mbsfF.filter_continuousRfrncYr(dataframes["mbsf"])
    dataframes["opBase"]=baseF.filter_beneficiaries(dataframes["opBase"], dataframes["mbsf"])
    dataframes["ipBase"]=baseF.filter_beneficiaries(dataframes["ipBase"], dataframes["mbsf"])
    dataframes["hhaBase"]=baseF.filter_beneficiaries(dataframes["hhaBase"], dataframes["mbsf"])
    dataframes["hospBase"]=baseF.filter_beneficiaries(dataframes["hospBase"], dataframes["mbsf"])
    dataframes["snfBase"]=baseF.filter_beneficiaries(dataframes["snfBase"], dataframes["mbsf"])
    dataframes["carBase"]=baseF.filter_beneficiaries(dataframes["carBase"], dataframes["mbsf"])
    dataframes["opRevenue"]=revenueF.filter_claims(dataframes["opRevenue"], dataframes["opBase"])
    dataframes["ipRevenue"]=revenueF.filter_claims(dataframes["ipRevenue"], dataframes["ipBase"])
    dataframes["hhaRevenue"]=revenueF.filter_claims(dataframes["hhaRevenue"], dataframes["hhaBase"])
    dataframes["hospRevenue"]=revenueF.filter_claims(dataframes["hospRevenue"], dataframes["hospBase"])
    dataframes["snfRevenue"]=revenueF.filter_claims(dataframes["snfRevenue"], dataframes["snfBase"])
    dataframes["carLine"]=lineF.filter_claims(dataframes["carLine"], dataframes["carBase"])
    return dataframes

def clean_through_dates(dataframes):
    for claimTypePart in list(dataframes.keys()):
        (claimType, claimPart) = get_claimType_claimPart(claimTypePart)
        if (claimPart=="Base"):
            if claimType=="ip":   #for now I am cleaning just the ip claims
                dataframes[claimTypePart] = baseF.get_clean_through_dates(dataframes[claimTypePart])
    return dataframes    

def add_through_date_info(df):
    df = (df.withColumn("THRU_DT_DAYOFYEAR",
                        F.date_format(
                            #THRU_DT was read as bigint, need to convert it to string that can be understood by date_format
                            F.concat_ws('-',F.col("THRU_DT").substr(1,4),F.col("THRU_DT").substr(5,2),F.col("THRU_DT").substr(7,2)),
                                        "D") #get the day of the year
                         .cast('int'))
                         #keep the claim through year too
                         .withColumn("THRU_DT_YEAR", F.col("THRU_DT").substr(1,4).cast('int'))
                         .withColumn("THRU_DT_MONTHSINYEARSPRIOR", monthsInYearsPrior[F.col("THRU_DT_YEAR")])
                         .withColumn("THRU_DT_MONTHOFYEAR", F.col("THRU_DT").substr(5,2).cast('int'))
                         .withColumn("THRU_DT_MONTH", F.col("THRU_DT_MONTHOFYEAR") + F.col("THRU_DT_MONTHSINYEARSPRIOR"))
                         #find number of days from yearStart-1 to year of admission -1
                         .withColumn("THRU_DT_DAYSINYEARSPRIOR", daysInYearsPrior[F.col("THRU_DT_YEAR")]) 
                         #assign a day number starting at day 1 of yearStart-1, 
                         # days in years prior to admission + days in year of admission = day number
                         .withColumn("THRU_DT_DAY", (F.col("THRU_DT_DAYSINYEARSPRIOR") + F.col("THRU_DT_DAYOFYEAR")).cast('int'))
                         .drop("THRU_DT_DAYSINYEARSPRIOR" , "THRU_DT_DAYOFYEAR", "THRU_DT_MONTHOFYEAR", "THRU_DT_MONTHSINYEARSPRIOR"))
    return df

def repartition_dfs(dataframes):
    for key in dataframes.keys():
        dataframes[key] = dataframes[key].repartition(80,"DSYSRTKY") 
    return dataframes

def run_cms_data_tests(dataframes):
    for claimTypePart in list(dataframes.keys()):
        (claimType, claimPart) = get_claimType_claimPart(claimTypePart)
        if (claimPart=="Base"):
            countsPerNoYear = dataframes[claimTypePart].groupBy("CLAIMNO","THRU_DT_YEAR").count().filter(F.col("count")>1).count()
            assert 0 == countsPerNoYear, f"{claimTypePart} includes more than 1 claims with same CLAIMNO and THRU_DT_YEAR"
        elif (claimPart is None):
            if (claimType=="mbsf"):   
                countsPerIdYear = dataframes[claimTypePart].groupBy("DSYSRTKY","RFRNC_YR").count().filter(F.col("count")>1).count()
                assert 0 == countsPerIdYear, f"{claimTypePart} includes more than 1 beneficiaries with same DSYSRTKY and RFRNC_YR"






