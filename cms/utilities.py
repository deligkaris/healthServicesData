from .mbsf import prep_mbsfDF
from cms.base import prep_baseDF
from cms.SCHEMAS.mbsf_schema import mbsfSchema

def get_filename_dicts(pathCMS, yearInitial, yearFinal):

    #assume all data is organized with folders INP, OUT, Denom, SNF, HHA, HOSP within the CMS folder
    pathIn = pathCMS +'/INP' #inpatient data folder
    pathOut = pathCMS + '/OUT' #outpatient data folder
    pathDenom = pathCMS + '/Denom' #demographic and enrollment data folder
    pathSNF = pathCMS + '/SAF/SNF' #skilled nursing facilities
    pathHHA = pathCMS + '/SAF/HHA' #home health agency
    pathHOSP = pathCMS + '/SAF/HOSP' #hospice care
    pathCAR = pathCMS + '/SAF/CAR' #carrier 

    #because there are several CMS files, put them in a dictionary
    outClaimsFilenames = {}
    outRevenueFilenames = {}
    inClaimsFilenames = {}
    inRevenueFilenames = {}
    mbsfFilenames = {}
    snfClaimsFilenames = {}
    snfRevenueFilenames = {}
    hhaClaimsFilenames = {}
    hhaRevenueFilenames = {}
    hospClaimsFilenames = {}
    hospRevenueFilenames = {}
    carClaimsFilenames = {}
    carRevenueFilenames = {}

    #all filenames will include their absolute paths
    for iYear in range(yearInitial,yearFinal+1): #remember range does not include the last point
        outClaimsFilenames[f'{iYear}'] = pathOut + f"/out_claimsk_{iYear}.parquet"
        outRevenueFilenames[f'{iYear}'] = pathOut + f"/out_revenuek_{iYear}.parquet"
        inClaimsFilenames[f'{iYear}'] = pathIn + f"/inp_claimsk_{iYear}.parquet" 
        inRevenueFilenames[f'{iYear}'] = pathIn + f"/inp_revenuek_{iYear}.parquet"
        mbsfFilenames[f'{iYear}'] = pathDenom + f"/mbsf_{iYear}.parquet"       
        snfClaimsFilenames[f'{iYear}'] = pathSNF + f"/snf_claimsk_{iYear}.parquet"      
        snfRevenueFilenames[f'{iYear}'] = pathSNF + f"/snf_revenuek_{iYear}.parquet"
        hhaClaimsFilenames[f'{iYear}'] = pathHHA + f"/hha_claimsk_{iYear}.parquet"
        hhaRevenueFilenames[f'{iYear}'] = pathHHA + f"/hha_revenuek_{iYear}.parquet"
        hospClaimsFilenames[f'{iYear}'] = pathHOSP + f"/hosp_claimsk_{iYear}.parquet"
        hospRevenueFilenames[f'{iYear}'] = pathHOSP + f"/hosp_revenuek_{iYear}.parquet"
        carClaimsFilenames[f'{iYear}'] = pathCAR + f"/car_claimsk_{iYear}.parquet"
        carRevenueFilenames[f'{iYear}'] = pathCAR + f"/car_linek_{iYear}.parquet"

    return (mbsfFilenames, outClaimsFilenames, outRevenueFilenames, inClaimsFilenames, inRevenueFilenames, snfClaimsFilenames, snfRevenueFilenames,
           hhaClaimsFilenames, hhaRevenueFilenames, hospClaimsFilenames, hospRevenueFilenames, carClaimsFilenames, carRevenueFilenames)

def read_data(spark, mbsfFilenames, outClaimsFilenames, outRevenueFilenames, inClaimsFilenames, inRevenueFilenames,
             snfClaimsFilenames, snfRevenueFilenames, hhaClaimsFilenames, hhaRevenueFilenames, hospClaimsFilenames, hospRevenueFilenames,
             carClaimsFilenames, carRevenueFilenames):

    #assume the worst...that each type of file includes claims from different years
    outClaimsYears = sorted(list(outClaimsFilenames.keys()))
    outRevenueYears = sorted(list(outRevenueFilenames.keys()))
    inClaimsYears = sorted(list(inClaimsFilenames.keys()))
    inRevenueYears = sorted(list(inRevenueFilenames.keys()))
    mbsfYears = sorted(list(mbsfFilenames.keys()))
    snfClaimsYears = sorted(list(snfClaimsFilenames.keys()))
    snfRevenueYears = sorted(list(snfRevenueFilenames.keys()))
    hhaClaimsYears = sorted(list(hhaClaimsFilenames.keys()))
    hhaRevenueYears = sorted(list(hhaRevenueFilenames.keys()))
    hospClaimsYears = sorted(list(hospClaimsFilenames.keys()))
    hospRevenueYears = sorted(list(hospRevenueFilenames.keys()))
    carClaimsYears = sorted(list(carClaimsFilenames.keys()))
    carRevenueYears = sorted(list(carRevenueFilenames.keys()))

    # PySpark defaults to reading and writing in the Parquet format
    # spark.read.parquet maps to spark.read.format('parquet').load()
    
    #one dictionary for each type of file
    outClaimsDict={}
    outRevenueDict={}
    inClaimsDict={}
    inRevenueDict={}
    mbsfDict={}
    snfClaimsDict={}
    snfRevenueDict={}
    hhaClaimsDict={}
    hhaRevenueDict={}
    hospClaimsDict={}
    hospRevenueDict={}
    carClaimsDict={}
    carRevenueDict={}

    #read all data and put them in dictionary, 
    #parquet files have a built-in schema so I do not need to infer schema when reading parquet files
    for iYear in outClaimsYears:
        outClaimsDict[f'{iYear}'] = spark.read.parquet(outClaimsFilenames[f'{iYear}'])

    for iYear in outRevenueYears:
        outRevenueDict[f'{iYear}'] = spark.read.parquet(outRevenueFilenames[f'{iYear}'])

    for iYear in inClaimsYears:
        inClaimsDict[f'{iYear}'] = spark.read.parquet(inClaimsFilenames[f'{iYear}'])

    for iYear in inRevenueYears:
        inRevenueDict[f'{iYear}'] = spark.read.parquet(inRevenueFilenames[f'{iYear}'])

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
    outClaims = outClaimsDict[outClaimsYears[0]] #initialize here
    outRevenue = outRevenueDict[outRevenueYears[0]]
    inClaims = inClaimsDict[inClaimsYears[0]]
    inRevenue = inRevenueDict[inRevenueYears[0]]
    mbsf = mbsfDict[mbsfYears[0]]
    #snfClaims = snfClaimsDict[snfClaimsYears[0]]
    #snfRevenue = snfRevenueDict[snfRevenueYears[0]]
    #hhaClaims = hhaClaimsDict[hhaClaimsYears[0]]
    #hhaRevenue = hhaRevenueDict[hhaRevenueYears[0]]
    #hospClaims = hospClaimsDict[hospClaimsYears[0]]
    #hospRevenue = hospRevenueDict[hospRevenueYears[0]]
    #carClaims = carClaimsDict[carClaimsYears[0]]
    #carRevenue = carRevenueDict[carRevenueYears[0]]

    if (len(outClaimsYears) > 1): 
        for iYear in outClaimsYears[1:]: 
            outClaims = outClaims.union(outClaimsDict[f'{iYear}']) #and then do union with the rest

    if (len(outRevenueYears) > 1):
        for iYear in outRevenueYears[1:]: 
            outRevenue = outRevenue.union(outRevenueDict[f'{iYear}']) #and then do union with the rest

    if (len(inClaimsYears) > 1):
        for iYear in inClaimsYears[1:]: 
            inClaims = inClaims.union(inClaimsDict[f'{iYear}']) #and then do union with the rest

    if (len(inRevenueYears) > 1):
       for iYear in inRevenueYears[1:]:
           inRevenue = inRevenue.union(inRevenueDict[f'{iYear}']) #and then do union with the rest

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
    carClaims=1    
    snfClaims=1
    hhaClaims=1
    hospClaims=1

    return(mbsf, outClaims, outRevenue, inClaims, inRevenue, 
           snfClaims, snfRevenue, hhaClaims, hhaRevenue, hospClaims, hospRevenue, 
           carClaims, carRevenue)

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


