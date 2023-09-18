from .mbsf import prep_mbsfDF
from cms.base import prep_baseDF
from cms.SCHEMAS.mbsf_schema import mbsfSchema

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
    carRevenueFilenames = {}

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
        carRevenueFilenames[f'{iYear}'] = pathCAR + f"/car_linek_{iYear}.parquet"

    return (mbsfFilenames, opBaseFilenames, opRevenueFilenames, ipBaseFilenames, ipRevenueFilenames, snfBaseFilenames, snfRevenueFilenames,
           hhaBaseFilenames, hhaRevenueFilenames, hospBaseFilenames, hospRevenueFilenames, carBaseFilenames, carRevenueFilenames)

def read_data(spark, mbsfFilenames, opBaseFilenames, opRevenueFilenames, ipBaseFilenames, ipRevenueFilenames,
             snfBaseFilenames, snfRevenueFilenames, hhaBaseFilenames, hhaRevenueFilenames, hospBaseFilenames, hospRevenueFilenames,
             carBaseFilenames, carRevenueFilenames):

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
    carRevenueYears = sorted(list(carRevenueFilenames.keys()))

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
    carRevenueDict={}

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

    #for iYear in snfClaimsYears:
    #    snfClaimsDict[f'{iYear}'] = spark.read.parquet(snfClaimsFilenames[f'{iYear}'])

    #for iYear in snfRevenueYears:
    #    snfRevenueDict[f'{iYear}'] = spark.read.parquet(snfRevenueFilenames[f'{iYear}'])

    #for iYear in hhaClaimsYears:
    #    hhaClaimsDict[f'{iYear}'] = spark.read.parquet(hhaClaimsFilenames[f'{iYear}'])

    #for iYear in hhaRevenueYears:
    #    hhaRevenueDict[f'{iYear}'] = spark.read.parquet(hhaRevenueFilenames[f'{iYear}'])

    #for iYear in hospClaimsYears:
    #    hospClaimsDict[f'{iYear}'] = spark.read.parquet(hospClaimsFilenames[f'{iYear}'])

    #for iYear in hospRevenueYears:
    #    hospRevenueDict[f'{iYear}'] = spark.read.parquet(hospRevenueFilenames[f'{iYear}'])
    
    #for iYear in carClaimsYears:
    #    carClaimsDict[f'{iYear}'] = spark.read.parquet(carClaimsFilenames[f'{iYear}'])

    #for iYear in hospRevenueYears:
    #    carRevenueDict[f'{iYear}'] = spark.read.parquet(carRevenueFilenames[f'{iYear}'])

    # merge all previous years in one dataframe
    opBase = opBaseDict[opBaseYears[0]] #initialize here
    opRevenue = opRevenueDict[opRevenueYears[0]]
    ipBase = ipBaseDict[ipClaimsYears[0]]
    ipRevenue = ipRevenueDict[ipRevenueYears[0]]
    mbsf = mbsfDict[mbsfYears[0]]
    #snfClaims = snfClaimsDict[snfClaimsYears[0]]
    #snfRevenue = snfRevenueDict[snfRevenueYears[0]]
    #hhaClaims = hhaClaimsDict[hhaClaimsYears[0]]
    #hhaRevenue = hhaRevenueDict[hhaRevenueYears[0]]
    #hospClaims = hospClaimsDict[hospClaimsYears[0]]
    #hospRevenue = hospRevenueDict[hospRevenueYears[0]]
    #carClaims = carClaimsDict[carClaimsYears[0]]
    #carRevenue = carRevenueDict[carRevenueYears[0]]

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

    #if (len(snfClaimsYears) > 1):
    #    for iYear in snfClaimsYears[1:]:
    #        snfClaims = snfClaims.union(snfClaimsDict[f'{iYear}']) #and then do union with the rest

    #if (len(snfRevenueYears) > 1):
    #   for iYear in snfRevenueYears[1:]:
    #       snfRevenue = snfRevenue.union(snfRevenueDict[f'{iYear}']) #and then do union with the rest

    #if (len(hhaClaimsYears) > 1):
    #    for iYear in hhaClaimsYears[1:]:
    #        hhaClaims = hhaClaims.union(hhaClaimsDict[f'{iYear}']) #and then do union with the rest

    #if (len(hhaRevenueYears) > 1):
    #   for iYear in hhaRevenueYears[1:]:
    #        hhaRevenue = hhaRevenue.union(hhaRevenueDict[f'{iYear}']) #and then do union with the rest

    #if (len(hospClaimsYears) > 1):
    #   for iYear in hospClaimsYears[1:]:
    #       hospClaims = hospClaims.union(hospClaimsDict[f'{iYear}']) #and then do union with the rest

    #if (len(hospRevenueYears) > 1):
    #   for iYear in hospRevenueYears[1:]:
    #       hospRevenue = hospRevenue.union(hospRevenueDict[f'{iYear}']) #and then do union with the rest

    #if (len(carClaimsYears) > 1):
    #   for iYear in carClaimsYears[1:]:
    #       carClaims = carClaims.union(carClaimsDict[f'{iYear}']) #and then do union with the rest

    #if (len(carRevenueYears) > 1):
    #   for iYear in carRevenueYears[1:]:
    #       carRevenue = carRevenue.union(carRevenueDict[f'{iYear}']) #and then do union with the rest
    snfRevenue=1
    hospRevenue=1
    hhaRevenue=1
    carRevenue=1
    carBase=1    
    snfBase=1
    hhaBase=1
    hospBase=1

    return(mbsf, opBase, opRevenue, ipBase, ipRevenue, 
           snfBase, snfRevenue, hhaBase, hhaRevenue, hospBase, hospRevenue, 
           carBase, carRevenue)

    #return(mbsf, outClaims, outRevenue, inClaims, inRevenue,
    #       snfClaims, hhaClaims, hospClaims)

def get_data(pathCMS, yearInitial, yearFinal, spark):

    (mbsfFilenames, opBaseFilenames, opRevenueFilenames, ipBaseFilenames, ipRevenueFilenames,
    snfBaseFilenames, snfRevenueFilenames, hhaBaseFilenames, hhaRevenueFilenames, 
    hospBaseFilenames, hospRevenueFilenames,
    carBaseFilenames, carRevenueFilenames) = get_filename_dicts(pathCMS, yearInitial, yearFinal)

    (mbsf, opBase, opRevenue, ipBase, ipRevenue, 
    snfBase, snfRevenue, 
    hhaBase, hhaRevenue, 
    hospBase, hospRevenue,
    carBase, carRevenue) = read_data(spark, mbsfFilenames, opBaseFilenames, opRevenueFilenames, ipBaseFilenames, ipRevenueFilenames,
                                                     snfBaseFilenames, snfRevenueFilenames, 
                                                     hhaBaseFilenames, hhaRevenueFilenames, 
                                                     hospBaseFilenames, hospRevenueFilenames,
                                                     carBaseFilenames, carRevenueFilenames) 

    (mbsf, opBase, opRevenue, ipBase, ipRevenue, snfBase, snfRevenue, hhaBase, hhaRevenue, hospBase, hospRevenue,
    carBase, carRevenue) = prep_dfs(mbsf, opBase, opRevenue, ipBase, ipRevenue, snfBase, snfRevenue, hhaBase, hhaRevenue,
                                    hospBase, hospRevenue, carBase, carRevenue) 

    return (mbsf, opBase, opRevenue, ipBase, ipRevenue, snfBase, snfRevenue,hhaBase, hhaRevenue, hospBase, hospRevenue,
            carBase, carRevenue)

def prep_dfs(mbsf, opBase, opRevenue, ipBase, ipRevenue, snfBase, snfRevenue, hhaBase, hhaRevenue, hospBase, hospRevenue, carBase, carRevenue):

    ipBase = prep_baseDF(ipBase,claim="inpatient")
    opBase = prep_baseDF(opBase,claim="outpatient")
    mbsf = prep_mbsfDF(mbsf, ipBase, opBase)
    #snfBase = prep_baseDF(snfBase,claim="snf")
    #hospBase = prep_baseDF(hospBase,claim="hosp")
    #hhaBase = prep_baseDF(hhaBase,claim="hha")

    return (mbsf, opBase, opRevenue, ipBase, ipRevenue, snfBase, snfRevenue, hhaBase, hhaRevenue, hospBase, hospRevenue, carBase, carRevenue) 


