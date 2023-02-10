

def get_filename_dicts(pathDenom, pathOut, pathIn, yearInitial, yearFinal):

    #because there are several CMS files, put them in a dictionary
    outClaimsFilenames = {}
    outRevenueFilenames = {}
    inClaimsFilenames = {}
    inRevenueFilenames = {}
    mbsfFilenames = {}

    #all filenames will include their absolute paths
    for iYear in range(yearInitial,yearFinal+1): #remember range does not include the last point
        outClaimsFilenames[f'{iYear}'] = pathOut + f"/out_claimsk_{iYear}.parquet"
        outRevenueFilenames[f'{iYear}'] = pathOut + f"/out_revenuek_{iYear}.parquet"
        inClaimsFilenames[f'{iYear}'] = pathIn + f"/inp_claimsk_{iYear}.parquet" 
        inRevenueFilenames[f'{iYear}'] = pathIn + f"/inp_revenuek_{iYear}.parquet"
        mbsfFilenames[f'{iYear}'] = pathDenom + f"/mbsf_{iYear}.parquet"       

    return (mbsfFilenames, outClaimsFilenames, outRevenueFilenames, inClaimsFilenames, inRevenueFilenames)

def read_data(spark, mbsfFilenames, outClaimsFilenames, outRevenueFilenames, inClaimsFilenames, inRevenueFilenames):

    #assume the worst...that each type of file includes claims from different years
    outClaimsYears = sorted(list(outClaimsFilenames.keys()))
    outRevenueYears = sorted(list(outRevenueFilenames.keys()))
    inClaimsYears = sorted(list(inClaimsFilenames.keys()))
    inRevenueYears = sorted(list(inRevenueFilenames.keys()))
    mbsfYears = sorted(list(mbsfFilenames.keys()))

    # PySpark defaults to reading and writing in the Parquet format
    # spark.read.parquet maps to spark.read.format('parquet').load()
    
    #one dictionary for each type of file
    outClaimsDict={}
    outRevenueDict={}
    inClaimsDict={}
    inRevenueDict={}
    mbsfDict={}

    print(outClaimsYears, outClaimsFilenames.keys())

    for iYear in outClaimsYears:
        outClaimsDict[f'{iYear}'] = spark.read.parquet(outClaimsFilenames[f'{iYear}'])

    for iYear in outRevenueYears:
        outRevenueDict[f'{iYear}'] = spark.read.parquet(outRevenueFilenames[f'{iYear}'])

    for iYear in inClaimsYears:
        inClaimsDict[f'{iYear}'] = spark.read.parquet(inClaimsFilenames[f'{iYear}'])

    for iYear in inRevenueYears:
        inRevenueDict[f'{iYear}'] = spark.read.parquet(inRevenueFilenames[f'{iYear}'])

    for iYear in mbsfYears:
        mbsfDict[f'{iYear}'] = spark.read.parquet(mbsfFilenames[f'{iYear}'])
    
    # merge all previous years in one dataframe
    outClaims = outClaimsDict[outClaimsYears[0]] #initialize here
    outRevenue = outRevenueDict[outRevenueYears[0]]
    inClaims = inClaimsDict[inClaimsYears[0]]
    inRevenue = inRevenueDict[inRevenueYears[0]]
    mbsf = mbsfDict[mbsfYears[0]]

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

    return(mbsf, outClaims, outRevenue, inClaims, inRevenue)
