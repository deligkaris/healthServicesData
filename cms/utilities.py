from .mbsf import prep_mbsfDF
from cms.base import prep_baseDF
from cms.line import prep_lineDF
from cms.SCHEMAS.mbsf_schema import mbsfSchema
from cms.revenue import prep_revenueDF

def get_filename_dicts(pathCMS, yearInitial, yearFinal):

    #assume all data is organized with folders INP, OUT, Denom, SNF, HHA, HOSP within the CMS folder
    pathIp = pathCMS +'/INP' #inpatient data folder
    pathOp = pathCMS + '/OUT' #outpatient data folder
    pathDenom = pathCMS + '/Denom' #demographic and enrollment data folder
    pathSNF = pathCMS + '/SAF/SNF' #skilled nursing facilities
    pathHHA = pathCMS + '/SAF/HHA' #home health agency
    pathHOSP = pathCMS + '/SAF/HOSP' #hospice care
    pathCAR = pathCMS + '/SAF/CAR' #carrier 

    #because there are several CMS files, put them in a dictionary
    opBaseFilenames = {}
    opRevenueFilenames = {}
    ipBaseFilenames = {}
    ipRevenueFilenames = {}
    mbsfFilenames = {}
    snfBaseFilenames = {}
    snfRevenueFilenames = {}
    hhaBaseFilenames = {}
    hhaRevenueFilenames = {}
    hospBaseFilenames = {}
    hospRevenueFilenames = {}
    carBaseFilenames = {}
    carLineFilenames = {}

    #all filenames will include their absolute paths
    for iYear in range(yearInitial,yearFinal+1): #remember range does not include the last point
        opBaseFilenames[f'{iYear}'] = pathOp + f"/out_claimsk_{iYear}.parquet"
        opRevenueFilenames[f'{iYear}'] = pathOp + f"/out_revenuek_{iYear}.parquet"
        ipBaseFilenames[f'{iYear}'] = pathIp + f"/inp_claimsk_{iYear}.parquet" 
        ipRevenueFilenames[f'{iYear}'] = pathIp + f"/inp_revenuek_{iYear}.parquet"
        mbsfFilenames[f'{iYear}'] = pathDenom + f"/mbsf_{iYear}.parquet"       
        snfBaseFilenames[f'{iYear}'] = pathSNF + f"/snf_claimsk_{iYear}.parquet"      
        snfRevenueFilenames[f'{iYear}'] = pathSNF + f"/snf_revenuek_{iYear}.parquet"
        hhaBaseFilenames[f'{iYear}'] = pathHHA + f"/hha_claimsk_{iYear}.parquet"
        hhaRevenueFilenames[f'{iYear}'] = pathHHA + f"/hha_revenuek_{iYear}.parquet"
        hospBaseFilenames[f'{iYear}'] = pathHOSP + f"/hosp_claimsk_{iYear}.parquet"
        hospRevenueFilenames[f'{iYear}'] = pathHOSP + f"/hosp_revenuek_{iYear}.parquet"
        carBaseFilenames[f'{iYear}'] = pathCAR + f"/car_claimsk_{iYear}.parquet"
        carLineFilenames[f'{iYear}'] = pathCAR + f"/car_linek_{iYear}.parquet"

    return (mbsfFilenames, opBaseFilenames, opRevenueFilenames, ipBaseFilenames, ipRevenueFilenames, snfBaseFilenames, snfRevenueFilenames,
           hhaBaseFilenames, hhaRevenueFilenames, hospBaseFilenames, hospRevenueFilenames, carBaseFilenames, carLineFilenames)

def read_data(spark, mbsfFilenames, opBaseFilenames, opRevenueFilenames, ipBaseFilenames, ipRevenueFilenames,
             snfBaseFilenames, snfRevenueFilenames, hhaBaseFilenames, hhaRevenueFilenames, hospBaseFilenames, hospRevenueFilenames,
             carBaseFilenames, carLineFilenames):

    #assume the worst...that each type of file includes claims from different years
    opBaseYears = sorted(list(opBaseFilenames.keys()))
    opRevenueYears = sorted(list(opRevenueFilenames.keys()))
    ipBaseYears = sorted(list(ipBaseFilenames.keys()))
    ipRevenueYears = sorted(list(ipRevenueFilenames.keys()))
    mbsfYears = sorted(list(mbsfFilenames.keys()))
    snfBaseYears = sorted(list(snfBaseFilenames.keys()))
    snfRevenueYears = sorted(list(snfRevenueFilenames.keys()))
    hhaBaseYears = sorted(list(hhaBaseFilenames.keys()))
    hhaRevenueYears = sorted(list(hhaRevenueFilenames.keys()))
    hospBaseYears = sorted(list(hospBaseFilenames.keys()))
    hospRevenueYears = sorted(list(hospRevenueFilenames.keys()))
    carBaseYears = sorted(list(carBaseFilenames.keys()))
    carLineYears = sorted(list(carLineFilenames.keys()))

    # PySpark defaults to reading and writing in the Parquet format
    # spark.read.parquet maps to spark.read.format('parquet').load()
    
    #one dictionary for each type of file
    opBaseDict={}
    opRevenueDict={}
    ipBaseDict={}
    ipRevenueDict={}
    mbsfDict={}
    snfBaseDict={}
    snfRevenueDict={}
    hhaBaseDict={}
    hhaRevenueDict={}
    hospBaseDict={}
    hospRevenueDict={}
    carBaseDict={}
    carLineDict={}

    #read all data and put them in dictionary, 
    #parquet files have a built-in schema so I do not need to infer schema when reading parquet files
    for iYear in opBaseYears:
        opBaseDict[f'{iYear}'] = spark.read.parquet(opBaseFilenames[f'{iYear}'])

    for iYear in opRevenueYears:
        opRevenueDict[f'{iYear}'] = spark.read.parquet(opRevenueFilenames[f'{iYear}'])

    for iYear in ipBaseYears:
        ipBaseDict[f'{iYear}'] = spark.read.parquet(ipBaseFilenames[f'{iYear}'])

    for iYear in ipRevenueYears:
        ipRevenueDict[f'{iYear}'] = spark.read.parquet(ipRevenueFilenames[f'{iYear}'])

    for iYear in mbsfYears:
        #ideally the schema will be enforced when the reading of the file takes place, run into issues doing that though
        #so for now the schema will be enforced during the dataframe prep functions
        #mbsfDict[f'{iYear}'] = spark.read.schema(mbsfSchema).parquet(mbsfFilenames[f'{iYear}'])
        mbsfDict[f'{iYear}'] = spark.read.parquet(mbsfFilenames[f'{iYear}'])

    for iYear in snfBaseYears:
        snfBaseDict[f'{iYear}'] = spark.read.parquet(snfBaseFilenames[f'{iYear}'])

    for iYear in snfRevenueYears:
        snfRevenueDict[f'{iYear}'] = spark.read.parquet(snfRevenueFilenames[f'{iYear}'])

    for iYear in hhaBaseYears:
        hhaBaseDict[f'{iYear}'] = spark.read.parquet(hhaBaseFilenames[f'{iYear}'])

    for iYear in hhaRevenueYears:
        hhaRevenueDict[f'{iYear}'] = spark.read.parquet(hhaRevenueFilenames[f'{iYear}'])

    for iYear in hospBaseYears:
        hospBaseDict[f'{iYear}'] = spark.read.parquet(hospBaseFilenames[f'{iYear}'])

    for iYear in hospRevenueYears:
        hospRevenueDict[f'{iYear}'] = spark.read.parquet(hospRevenueFilenames[f'{iYear}'])
    
    for iYear in carBaseYears:
        carBaseDict[f'{iYear}'] = spark.read.parquet(carBaseFilenames[f'{iYear}'])

    for iYear in carLineYears:
        carLineDict[f'{iYear}'] = spark.read.parquet(carLineFilenames[f'{iYear}'])

    # merge all previous years in one dataframe
    opBase = opBaseDict[opBaseYears[0]] #initialize here
    opRevenue = opRevenueDict[opRevenueYears[0]]
    ipBase = ipBaseDict[ipBaseYears[0]]
    ipRevenue = ipRevenueDict[ipRevenueYears[0]]
    mbsf = mbsfDict[mbsfYears[0]]
    snfBase = snfBaseDict[snfBaseYears[0]]
    snfRevenue = snfRevenueDict[snfRevenueYears[0]]
    hhaBase = hhaBaseDict[hhaBaseYears[0]]
    hhaRevenue = hhaRevenueDict[hhaRevenueYears[0]]
    hospBase = hospBaseDict[hospBaseYears[0]]
    hospRevenue = hospRevenueDict[hospRevenueYears[0]]
    carBase = carBaseDict[carBaseYears[0]]
    carLine = carLineDict[carLineYears[0]]

    if (len(opBaseYears) > 1): 
        for iYear in opBaseYears[1:]: 
            opBase = opBase.union(opBaseDict[f'{iYear}']) #and then do union with the rest

    if (len(opRevenueYears) > 1):
        for iYear in opRevenueYears[1:]: 
            opRevenue = opRevenue.union(opRevenueDict[f'{iYear}']) #and then do union with the rest

    if (len(ipBaseYears) > 1):
        for iYear in ipBaseYears[1:]: 
            ipBase = ipBase.union(ipBaseDict[f'{iYear}']) #and then do union with the rest

    if (len(ipRevenueYears) > 1):
       for iYear in ipRevenueYears[1:]:
           ipRevenue = ipRevenue.union(ipRevenueDict[f'{iYear}']) #and then do union with the rest

    if (len(mbsfYears) > 1):
       for iYear in mbsfYears[1:]:
           mbsf = mbsf.union(mbsfDict[f'{iYear}']) #and then do union with the rest

    if (len(snfBaseYears) > 1):
        for iYear in snfBaseYears[1:]:
            snfBase = snfBase.union(snfBaseDict[f'{iYear}']) #and then do union with the rest

    if (len(snfRevenueYears) > 1):
       for iYear in snfRevenueYears[1:]:
           snfRevenue = snfRevenue.union(snfRevenueDict[f'{iYear}']) #and then do union with the rest

    if (len(hhaBaseYears) > 1):
        for iYear in hhaBaseYears[1:]:
            hhaBase = hhaBase.union(hhaBaseDict[f'{iYear}']) #and then do union with the rest

    if (len(hhaRevenueYears) > 1):
       for iYear in hhaRevenueYears[1:]:
            hhaRevenue = hhaRevenue.union(hhaRevenueDict[f'{iYear}']) #and then do union with the rest

    if (len(hospBaseYears) > 1):
       for iYear in hospBaseYears[1:]:
           hospBase = hospBase.union(hospBaseDict[f'{iYear}']) #and then do union with the rest

    if (len(hospRevenueYears) > 1):
       for iYear in hospRevenueYears[1:]:
           hospRevenue = hospRevenue.union(hospRevenueDict[f'{iYear}']) #and then do union with the rest

    if (len(carBaseYears) > 1):
       for iYear in carBaseYears[1:]:
           carBase = carBase.union(carBaseDict[f'{iYear}']) #and then do union with the rest

    if (len(carLineYears) > 1):
       for iYear in carLineYears[1:]:
           carLine = carLine.union(carLineDict[f'{iYear}']) #and then do union with the rest

    return(mbsf, opBase, opRevenue, ipBase, ipRevenue, 
           snfBase, snfRevenue, hhaBase, hhaRevenue, hospBase, hospRevenue, 
           carBase, carLine)

def get_data(pathCMS, yearInitial, yearFinal, spark):

    (mbsfFilenames, opBaseFilenames, opRevenueFilenames, ipBaseFilenames, ipRevenueFilenames,
    snfBaseFilenames, snfRevenueFilenames, hhaBaseFilenames, hhaRevenueFilenames, 
    hospBaseFilenames, hospRevenueFilenames,
    carBaseFilenames, carLineFilenames) = get_filename_dicts(pathCMS, yearInitial, yearFinal)

    (mbsf, opBase, opRevenue, ipBase, ipRevenue, 
    snfBase, snfRevenue, 
    hhaBase, hhaRevenue, 
    hospBase, hospRevenue,
    carBase, carLine) = read_data(spark, mbsfFilenames, opBaseFilenames, opRevenueFilenames, ipBaseFilenames, ipRevenueFilenames,
                                                     snfBaseFilenames, snfRevenueFilenames, 
                                                     hhaBaseFilenames, hhaRevenueFilenames, 
                                                     hospBaseFilenames, hospRevenueFilenames,
                                                     carBaseFilenames, carLineFilenames) 

    (mbsf, opBase, opRevenue, ipBase, ipRevenue, snfBase, snfRevenue, hhaBase, hhaRevenue, hospBase, hospRevenue,
    carBase, carRevenue) = prep_dfs(mbsf, opBase, opRevenue, ipBase, ipRevenue, snfBase, snfRevenue, hhaBase, hhaRevenue,
                                    hospBase, hospRevenue, carBase, carLine) 

    return (mbsf, opBase, opRevenue, ipBase, ipRevenue, snfBase, snfRevenue,hhaBase, hhaRevenue, hospBase, hospRevenue,
            carBase, carLine)

def prep_dfs(mbsf, opBase, opRevenue, ipBase, ipRevenue, snfBase, snfRevenue, hhaBase, hhaRevenue, hospBase, hospRevenue, carBase, carLine):

    ipBase = prep_baseDF(ipBase,claim="inpatient")
    opBase = prep_baseDF(opBase,claim="outpatient")
    mbsf = prep_mbsfDF(mbsf, ipBase, opBase)
    snfBase = prep_baseDF(snfBase,claim="snf")
    hospBase = prep_baseDF(hospBase,claim="hosp")
    hhaBase = prep_baseDF(hhaBase,claim="hha")

    ipRevenue = prep_revenueDF(ipRevenue,claim="inpatient")
    opRevenue = prep_revenueDF(opRevenue,claim="outpatient")

    carBase = prep_baseDF(carBase, claim="car")
    carLine = prep_lineDF(carLine, claim="car")

    return (mbsf, opBase, opRevenue, ipBase, ipRevenue, snfBase, snfRevenue, hhaBase, hhaRevenue, hospBase, hospRevenue, carBase, carLine) 


