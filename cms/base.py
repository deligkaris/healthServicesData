import pyspark.sql.functions as F
from pyspark.sql.window import Window
from .mbsf import add_ohResident
from utilities import add_primaryTaxonomy, add_acgmeSitesInZip, add_acgmeProgramsInZip
from cms.SCHEMAS.ip_base_schema import ipBaseSchema

def cast_columns_as_int(baseDF, claim="outpatient"): #date fields in the dataset must be interpreted as integers (and not as floats)

    if (claim=="outpatient"):
       columns = ["THRU_DT", "DSYSRTKY"]
    elif (claim=="inpatient"):
       columns = ["THRU_DT", "DSCHRGDT", "ADMSN_DT", "DSYSRTKY"]
    # SNF: DSCHRG DT is either NULL (quite frequently) or the same as THRU_DT, so for SNF claims use the THRU_DT when you need DSCHRG_DT
    elif ( (claim=="snf") | (claim=="hha") ):
       columns = ["CLM_THRU_DT", "NCH_BENE_DSCHRG_DT", "CLM_ADMSN_DT", "DESY_SORT_KEY"]
    elif ( (claim=="hosp") ):
       columns = ["CLM_THRU_DT", "CLM_HOSPC_START_DT_ID", "DESY_SORT_KEY"]

    for iColumns in columns:
        baseDF = baseDF.withColumn( iColumns, F.col(iColumns).cast('int'))

    return baseDF

def cast_columns_as_string(baseDF, claim="outpatient"):

    if ((claim=="outpatient") | (claim=="inpatient")):
        baseDF = (baseDF.withColumn("PRSTATE", F.lpad(F.col("PRSTATE").cast("string"),2,'0'))
                        .withColumn("STATE_CD", F.lpad(F.col("STATE_CD").cast("string"),2,'0'))
                        .withColumn("CNTY_CD", F.lpad(F.col("CNTY_CD").cast("string"),3,'0')))
    elif ((claim=="hha") | (claim=="hosp") | (claim=="snf")):
        baseDF = (baseDF.withColumn("PRVDR_STATE_CD", F.lpad(F.col("PRVDR_STATE_CD").cast("string"),2,'0'))
                        .withColumn("BENE_STATE_CD", F.lpad(F.col("BENE_STATE_CD").cast("string"),2,'0'))
                        .withColumn("BENE_CNTY_CD", F.lpad(F.col("BENE_CNTY_CD").cast("string"),3,'0')))
        
    return baseDF

def add_admission_date_info(baseDF, claim="outpatient"):

    #leapYears=[2016,2020,2024,2028]
   
    #unfortunately, SNF claims have a different column name for admission date
    #admissionColName = "CLM_ADMSN_DT" if claim=="snf" else "ADMSN_DT"
    if ( (claim=="snf") | (claim=="hha") ):
        baseDF = baseDF.withColumn( "ADMSN_DT", F.col("CLM_ADMSN_DT"))
    elif ( (claim=="hosp") ):
        baseDF = baseDF.withColumn( "ADMSN_DT", F.col("CLM_HOSPC_START_DT_ID") )

    baseDF = baseDF.withColumn( "ADMSN_DT_DAYOFYEAR", 
                                F.date_format(
                                     #ADMSN_DT was read as bigint, need to convert it to string that can be understood by date_format
                                     F.concat_ws('-',F.col("ADMSN_DT").substr(1,4),F.col("ADMSN_DT").substr(5,2),F.col("ADMSN_DT").substr(7,2)), 
                                     "D" #get the day of the year
                                ).cast('int'))

    # keep the year too
    baseDF = baseDF.withColumn( "ADMSN_DT_YEAR", F.col("ADMSN_DT").substr(1,4).cast('int'))

    # find number of days from yearStart-1 to year of admission -1
    baseDF = baseDF.withColumn( "ADMSN_DT_DAYSINYEARSPRIOR", 
                                #some admissions have started in yearStart-1
                                F.when(F.col("ADMSN_DT_YEAR")==2015 ,0)  #this should be yearStart-1
                                 .when(F.col("ADMSN_DT_YEAR")==2016 ,365) 
                                 .when(F.col("ADMSN_DT_YEAR")==2017 ,366+365) #set them to 366 for leap years
                                 .when(F.col("ADMSN_DT_YEAR")==2018 ,366+365*2)
                                 .when(F.col("ADMSN_DT_YEAR")==2019 ,366+365*3)
                                 .when(F.col("ADMSN_DT_YEAR")==2020 ,366+365*4)
                                 .when(F.col("ADMSN_DT_YEAR")==2021 ,366*2+365*4)
                                 .otherwise(365)) #otherwise 365

    # assign a day number starting at day 1 of yearStart-1
    baseDF = baseDF.withColumn("ADMSN_DT_DAY", 
                               # days in years prior to admission + days in year of admission = day nunber
                               (F.col("ADMSN_DT_DAYSINYEARSPRIOR") + F.col("ADMSN_DT_DAYOFYEAR")).cast('int'))

    return baseDF

def add_through_date_info(baseDF, claim="outpatient"):

    #unfortunately, SNF claims have a different column name for claim through date
    if ( (claim=="snf") | (claim=="hha") | (claim=="hosp") ):
        baseDF = baseDF.withColumn( "THRU_DT", F.col("CLM_THRU_DT"))

    baseDF = baseDF.withColumn( "THRU_DT_DAYOFYEAR", 
                                F.date_format(
                                    #THRU_DT was read as bigint, need to convert it to string that can be understood by date_format
                                    F.concat_ws('-',F.col("THRU_DT").substr(1,4),F.col("THRU_DT").substr(5,2),F.col("THRU_DT").substr(7,2)), 
                                    "D" #get the day of the year
                                ).cast('int'))

    # keep the claim through year too
    baseDF = baseDF.withColumn( "THRU_DT_YEAR", F.col("THRU_DT").substr(1,4).cast('int'))

    # find number of days from yearStart-1 to year of admission -1
    baseDF = baseDF.withColumn( "THRU_DT_DAYSINYEARSPRIOR", 
                                #some admissions have started in yearStart-1
                                F.when(F.col("THRU_DT_YEAR")==2015 ,0)  #this should be yearStart-1
                                 .when(F.col("THRU_DT_YEAR")==2016 ,365) 
                                 .when(F.col("THRU_DT_YEAR")==2017 ,366+365) #set them to 366 for leap years
                                 .when(F.col("THRU_DT_YEAR")==2018 ,366+365*2)
                                 .when(F.col("THRU_DT_YEAR")==2019 ,366+365*3)
                                 .when(F.col("THRU_DT_YEAR")==2020 ,366+365*4)
                                 .when(F.col("THRU_DT_YEAR")==2021 ,366*2+365*4)
                                 .otherwise(365)) #otherwise 365

    # assign a day number starting at day 1 of yearStart-1
    baseDF = baseDF.withColumn( "THRU_DT_DAY", 
                                 # days in years prior to admission + days in year of admission = day nunber
                                 (F.col("THRU_DT_DAYSINYEARSPRIOR") + F.col("THRU_DT_DAYOFYEAR")).cast('int'))

    return baseDF

def add_discharge_date_info(baseDF, claim="outpatient"):

    #unfortunately, SNF claims have a different column name for discharge date
    if (claim=="snf"):         
        baseDF = baseDF.withColumn( "DSCHRGDT", F.col("NCH_BENE_DSCHRG_DT"))

    baseDF = baseDF.withColumn( "DSCHRGDT_DAYOFYEAR",
                                F.date_format(
                                    #THRU_DT was read as bigint, need to convert it to string that can be understood by date_format
                                    F.concat_ws('-',F.col("DSCHRGDT").substr(1,4),F.col("DSCHRGDT").substr(5,2),F.col("DSCHRGDT").substr(7,2)),
                                    "D" #get the day of the year
                                ).cast('int'))

    # keep the claim through year too
    baseDF = baseDF.withColumn( "DSCHRGDT_YEAR", F.col("DSCHRGDT").substr(1,4).cast('int'))

    # find number of days from yearStart-1 to year of admission -1
    baseDF = baseDF.withColumn( "DSCHRGDT_DAYSINYEARSPRIOR",
                                #some admissions have started in yearStart-1
                                F.when(F.col("DSCHRGDT_YEAR")==2015 ,0)  #this should be yearStart-1
                                 .when(F.col("DSCHRGDT_YEAR")==2016 ,365)
                                 .when(F.col("DSCHRGDT_YEAR")==2017 ,366+365) #set them to 366 for leap years
                                 .when(F.col("DSCHRGDT_YEAR")==2018 ,366+365*2)
                                 .when(F.col("DSCHRGDT_YEAR")==2019 ,366+365*3)
                                 .when(F.col("DSCHRGDT_YEAR")==2020 ,366+365*4)
                                 .when(F.col("DSCHRGDT_YEAR")==2021 ,366*2+365*4)
                                 .otherwise(365)) #otherwise 365

    # assign a day number starting at day 1 of yearStart-1
    baseDF = baseDF.withColumn( "DSCHRGDT_DAY",
                                 # days in years prior to admission + days in year of admission = day nunber
                                 (F.col("DSCHRGDT_DAYSINYEARSPRIOR") + F.col("DSCHRGDT_DAYOFYEAR")).cast('int'))

    return baseDF

def add_XDaysFromYDAY(baseDF, YDAY="ADMSN_DT_DAY", X=90):
    
    baseDF = baseDF.withColumn(f"{X}DaysFrom{YDAY}", F.col(YDAY)+X )
    
    return baseDF

def add_ishStroke(baseDF):

    # PRNCPAL_DGNS_CD: diagnosis, condition problem or other reason for the admission/encounter/visit to 
    # be chiefly responsible for the services, redundantly stored as ICD_DGNS_CD1
    # ADMTG_DGNS_CD: initial diagnosis at admission, may not be confirmed after evaluation, 
    # may be different than the eventual diagnosis as in ICD_DGNS_CD1-25
    # which suggests that the ICD_DGNS_CDs are after evaluation, therefore ICD_DGNS_CDs are definitely not rule-out 
    # JB: well, you can never be certain that they are not rule-out, but the principal diagnostic code for stroke has been validated

    baseDF = baseDF.withColumn("ishStroke",
                              # ^I63[\d]: beginning of string I63 matches 0 or more digit characters 0-9
                              # I63 cerebral infraction, I64 
                              F.when(
                                      (F.regexp_extract( F.col("PRNCPAL_DGNS_CD"), '^I63[\d]*',0) !='') |
                                      (F.regexp_extract( F.col("PRNCPAL_DGNS_CD"), '^I64[\d]*',0) !=''), 1)
                               .otherwise(0)) 

    return baseDF

def add_ichStroke(baseDF):

    baseDF = baseDF.withColumn("ichStroke",
                              # ^I61[\d]: beginning of string I61 matches 0 or more digit characters 0-9
                              F.when(
                                      (F.regexp_extract( F.col("PRNCPAL_DGNS_CD"), '^I61[\d]*',0) !=''), 1)
                               .otherwise(0)) 
    return baseDF

def add_tiaStroke(baseDF):

    baseDF = baseDF.withColumn("tiaStroke",
                              # ^G45[\d]: beginning of string I61 matches 0 or more digit characters 0-9
                              F.when(
                                      (F.regexp_extract( F.col("PRNCPAL_DGNS_CD"), '^G45[\d]*',0) !=''), 1)
                               .otherwise(0)) 
    return baseDF

def add_anyStroke(baseDF):

    baseDF = baseDF.withColumn("anyStroke",
                               F.when( (F.col("ishStroke")==1) | (F.col("ichStroke")==1) | (F.col("tiaStroke")==1), 1)
                                .otherwise(0))

    return baseDF

def add_parkinsonsPrncpalDgns(baseDF):

    baseDF = baseDF.withColumn("parkinsons",
                              F.when((F.regexp_extract( F.trim(F.col("PRNCPAL_DGNS_CD")), '^G20[\d]*',0) !=''), 1)
                               .otherwise(0))

    return baseDF

def add_parkinsons(baseDF):

    dgnsColumnList = [f"ICD_DGNS_CD{x}" for x in range(1,26)] #all 25 DGNS columns

    baseDF = (baseDF.withColumn("dgnsList", #add an array of all dgns codes found in their claims
                                F.array(dgnsColumnList))
                    .withColumn("parkinsonsList", #keeps codes that match the regexp pattern
                                F.expr(f'filter(dgnsList, x -> x rlike "G20[0-9]?")')))

    baseDF = baseDF.withColumn("parkinsons",
                               F.when( F.size(F.col("parkinsonsList"))>0, 1)
                                .otherwise(0))

    return baseDF

def add_dbsPrcdr(baseDF): # dbs: deep brain stimulation

    #this function tries to find dbs in procedure codes only, and those are found in inpatient claims

    dbsPrcdrCodes = ("00H00MZ", "00H03MZ")
    
    prcdrCodeColumns = [f"ICD_PRCDR_CD{x}" for x in range(1,26)]

    #find dbs procedure codes in claims
    baseDF = (baseDF.withColumn("prcdrCodeAll",
                               F.array(prcdrCodeColumns))
                    .withColumn("dbsPrcdrCodes",
                               F.expr( f"filter(prcdrCodeAll, x -> x in {dbsPrcdrCodes})")))

    #if dbs prcdr codes are found, then dbs was performed
    baseDF = baseDF.withColumn("dbsPrcdr",
                               F.when( F.size(F.col("dbsPrcdrCodes"))>0,     1)
                                .otherwise(0))

    return baseDF

def add_dbsCpt(baseDF):

    #this function tries to find dbs in current procedural terminology, cpt, codes, and those are found in carrier files

    dbsCptCodes = ("61855", "61862", "61863", "61865", "61867")

    eachClaim = Window.partitionBy("CLAIMNO")

    #find dbs cpt codes in claims
    baseDF = (baseDF.withColumn("hcpcsCodeAll",
                                F.collect_set(F.col("HCPCS_CD")).over(eachClaim))
                    .withColumn("dbsCptCodes",
                                F.expr(f"filter(hcpcsCodeAll, x - > x in {dbsCptCodes})")))

    #if dbs cpt codes are found, then dbsCpt was performed
    baseDF = baseDF.withColumn("dbsCpt",
                               F.when( F.size(F.col("dbsCptCodes"))>0,     1)
                                .otherwise(0))

    return baseDF

def add_ohProvider(baseDF):

    # keep providers in OH (PRSTATE)
    # ohio is code 36, SSA code, https://resdac.org/cms-data/variables/state-code-claim-ssa
    ohProviderCondition = '(F.col("PRSTATE")=="36")'

    baseDF = baseDF.withColumn("ohProvider",
                               F.when(eval(ohProviderCondition), 1)
                                .otherwise(0))

    return baseDF
            
def add_firstClaim(baseDF):

    eachDsysrtky=Window.partitionBy("DSYSRTKY")

    baseDF = baseDF.withColumn("firstADMSN_DT_DAY", # find the first claims for each beneficiary
                                F.min(F.col("ADMSN_DT_DAY")).over(eachDsysrtky))

    baseDF = baseDF.withColumn("firstClaim", #and mark it/them (could be more than 1)
                                F.when(F.col("ADMSN_DT_DAY")==F.col("firstADMSN_DT_DAY"),1)
                                 .otherwise(0))

    return baseDF

def add_firstClaimSum(baseDF):

    eachDsysrtky=Window.partitionBy("DSYSRTKY")

    baseDF = (baseDF.withColumn("firstClaimSum",
                                F.sum(F.col("firstClaim")).over(eachDsysrtky)))

    return baseDF

def add_lastClaim(baseDF):

    eachDsysrtky=Window.partitionBy("DSYSRTKY")

    baseDF = baseDF.withColumn("lastTHRU_DT_DAY",
                                F.max(F.col("THRU_DT_DAY")).over(eachDsysrtky))

    baseDF = baseDF.withColumn("lastClaim", #and mark it/them (could be more than 1)
                                F.when(F.col("THRU_DT_DAY")==F.col("lastTHRU_DT_DAY"),1)
                                 .otherwise(0))

    return baseDF

def add_lastClaimSum(baseDF):

    eachDsysrtky=Window.partitionBy("DSYSRTKY")

    baseDF = (baseDF.withColumn("lastClaimSum",
                                F.sum(F.col("lastClaim")).over(eachDsysrtky)))

    return baseDF

def add_sameStay(baseDF):

    # claims with same beneficiary, organization, and admission date as defined as one hospitalization stay
    eachStay = Window.partitionBy(["DSYSRTKY","ORGNPINM","ADMSN_DT"])

    baseDF = baseDF.withColumn("sameStay",
                                F.when(  F.count(F.col("DSYSRTKY")).over(eachStay) > 1, 1)
                                 .otherwise(0))

    return baseDF

#def add_inToInTransfer(baseDF):

    #inpatient -> inpatient transfer is defined as the same beneficiary having claims from two different organizations on the same day
#    eachAdmissionDate = Window.partitionBy(["DSYSRTKY","ADMSN_DT"])

#    baseDF = baseDF.withColumn("inToInTransfer",
#                                F.when(  F.size(F.collect_set(F.col("ORGNPINM")).over(eachAdmissionDate)) > 1, 1)
#                                 .otherwise(0))

#    return baseDF

def add_numberOfXOverY(baseDF, X="ORGNPINM", Y=["DSYSRTKY","ADMSN_DT"]):

    #eachBeneficiaryAdmissionDate = Window.partitionBy(["DSYSRTKY","ADMSN_DT"])
    eachY = Window.partitionBy(Y)

    YasString = ''.join(Y)

    baseDF = baseDF.withColumn(f"numberOf{X}Over{YasString}",
                                F.size(F.collect_set(F.col(X)).over(eachY)) )

    return baseDF                            

def add_providerName(baseDF, npiProviderDF):

    baseDF = baseDF.join(
                      npiProviderDF.select(
                          F.col("NPI"),F.col("Provider Organization Name (Legal Business Name)").alias("providerName")),
                      on = [F.col("ORGNPINM") == F.col("NPI")],
                      how = "left_outer")

    # drop the NPI column that was just added
    baseDF = baseDF.drop(F.col("NPI"))

    return baseDF

def add_providerOtherName(baseDF, npiProviderDF):

    baseDF = baseDF.join(
                      npiProviderDF.select(
                          F.col("NPI"),F.col("Provider Other Organization Name").alias("providerOtherName")),
                      on = [F.col("ORGNPINM") == F.col("NPI")],
                      how = "left_outer")

    # drop the NPI column that was just added
    baseDF = baseDF.drop(F.col("NPI"))

    return baseDF

def add_providerAddress(baseDF, npiProviderDF):

    baseDF = baseDF.join(
                         npiProviderDF.select(
                                          F.col("NPI"),
                                          F.concat_ws(",",
                                              F.col("Provider First Line Business Practice Location Address"),
                                              F.col("Provider Second Line Business Practice Location Address"),
                                              F.col("Provider Business Practice Location Address City Name"),
                                              F.col("Provider Business Practice Location Address State Name"),
                                              F.col("Provider Business Practice Location Address Postal Code").substr(1,5))
                                              .alias("providerAddress")),
                         on = [F.col("ORGNPINM")==F.col("NPI")],
                         how = "left_outer")

    baseDF = baseDF.drop(F.col("NPI"))

    return baseDF

def add_providerZip(baseDF,npiProviderDF):

    baseDF = baseDF.join(
                         npiProviderDF.select(
                                          F.col("NPI"),
                                          F.col("Provider Business Practice Location Address Postal Code").substr(1,5).alias("providerZip")),
                         on = [F.col("ORGNPINM")==F.col("NPI")],
                         how = "left_outer")

    baseDF = baseDF.drop(F.col("NPI"))

    return baseDF

def add_providerState(baseDF,npiProviderDF):

    baseDF = baseDF.join(
                         npiProviderDF.select(
                                          F.col("NPI"),
                                          F.col("Provider Business Practice Location Address State Name").alias("providerState")),
                         on = [F.col("ORGNPINM")==F.col("NPI")],
                         how = "left_outer")

    baseDF = baseDF.drop(F.col("NPI"))

    return baseDF

def add_providerCountyName(baseDF,cbsaDF): #assumes providerFIPS

    #if ever run into problems, eg a lot of nulls, the CMS hospital cost report (hospCost2018) also have the county of providers

    #medicareHospitalInfo works well for inpatient claims, but is less complete for outpatient claims
    #baseDF = baseDF.join(medicareHospitalInfoDF
    #                          .select(
    #                                 F.col("Facility ID"),  
    #                                 F.lower(F.trim(F.col("County Name"))).alias("providerCounty")),
    #                     on=[ F.col("Facility ID")==F.col("PROVIDER") ],
    #                     how="left_outer")
    #baseDF = baseDF.drop("Facility ID")

    #if you know the providerFIPS, this is the best way to find the county name
    #but...in the cbsaDF there are two rows for the Los Angeles county...because for this county, and only this county,
    #there is a single FIPS code but two SSA codes and exactly the same county name
    #each of the two SSA codes apparently includes two different parts of the LA county, different zip codes
    #search this document for 05200 to read a bit: https://www.reginfo.gov/public/do/DownloadDocument?objectID=69093000
    baseDF = (baseDF.join(
                        cbsaDF
                            .select(
                                F.col("countyname").alias("providerCountyName"),
                                F.col("fipscounty"))
                            .distinct(),
                        on=[F.col("fipscounty")==F.col("providerFIPS")],
                        how="left_outer")
                    .drop("fipscounty"))

    return baseDF

def add_providerFIPS(baseDF,posDF): #assumes add_providerCounty

    #I found that CBSA will duplicate some of my baseDF rows, I did not look into why because I found that posDF can be used as well
    #baseDF = baseDF.join(cbsaDF
    #                         .select(
    #                            F.lower(F.trim(F.col("countyname"))).alias("countyname"),
    #                            F.upper(F.trim(F.col("state"))).alias("state"),
    #                            F.col("fipscounty").alias("providerFips")),
    #                     on=[ (F.col("countyname")==F.col("providerCounty")) & (F.col("state")==F.col("providerState")) ],
    #                     #on=[F.col("countyname").contains(F.col("providerCounty"))], #in 1 test gave identical results as above
    #                     how="left_outer")

    #baseDF = baseDF.drop("countyname","state")

    #the posDF will give me ~99.9%of the providerFIPS codes
    baseDF = baseDF.join(posDF
                             .select( F.col("PRVDR_NUM"),F.col("providerFIPS")),
                         on=[F.col("PRVDR_NUM")==F.col("PROVIDER")],
                         how="left_outer")

    baseDF = baseDF.drop("PRVDR_NUM")

    return baseDF

def add_providerStateFIPS(baseDF, posDF):

    baseDF = baseDF.join(posDF
                            .select( F.col("PRVDR_NUM"), F.col("providerStateFIPS") ),
                         on=[F.col("PRVDR_NUM")==F.col("PROVIDER")],
                         how="left_outer")

    baseDF = baseDF.drop("PRVDR_NUM")

    return baseDF


def add_providerRegion(baseDF):

    westCodes = ["04", "08", "16", "35", "30", "49", "32", "56", "02", "06", "15", "41", "53"] #state fips codes
    southCodes = ["10", "11", "12", "13", "24", "37", "45", "51", "54", "01", "21", "28", "47", "05", "22", "40", "48"]
    midwestCodes = ["18", "17", "26", "39", "55", "19", "20", "27", "29", "31", "38", "46"]
    northeastCodes = ["09", "23", "25", "33", "44", "50", "34", "36", "42"]

    westCondition = '(F.col("providerStateFIPS").isin(westCodes))' 
    southCondition = '(F.col("providerStateFIPS").isin(southCodes))'
    midwestCondition = '(F.col("providerStateFIPS").isin(midwestCodes))'
    northeastCondition = '(F.col("providerStateFIPS").isin(northeastCodes))'

    baseDF = baseDF.withColumn("providerRegion",
                               F.when( eval(westCondition), 4)
                                .when( eval(southCondition), 3)
                                .when( eval(midwestCondition), 2)
                                .when( eval(northeastCondition), 1) 
                                .otherwise(F.lit(None)))

    return baseDF

def add_providerFIPSToNulls(baseDF,posDF,zipToCountyDF): #assumes add_providerCounty

    #for the rest 0.1% I will use a probabilistic method to get the fips county, but even with this method there will still be some nulls

    #baseDF = baseDF.join(zipToCountyDF
    #                               .filter(F.col("countyForZip")==1) 
    #                               .select(F.col("zip"),F.col("county").alias("providerFIPSzipToCounty")),
    #          on= [  (zipToCountyDF["zip"]==baseDF["providerZip"]) ],
    #          how="left_outer")

    #baseDF = baseDF.drop("zip")

    #baseDF = baseDF.withColumn("providerFIPS",
    #                           F.when( F.col("providerFIPS").isNull(), F.col("providerFIPSzipToCounty"))
    #                            .otherwise( F.col("providerFIPS") ))

    #baseDF = baseDF.drop("providerFIPSzipToCounty")

    return baseDF

def add_provider_npi_info(baseDF, npiProvidersDF):

    baseDF = add_providerName(baseDF,npiProvidersDF)
    baseDF = add_providerOtherName(baseDF,npiProvidersDF)
    baseDF = add_providerAddress(baseDF,npiProvidersDF)
    baseDF = add_providerZip(baseDF,npiProvidersDF)
    baseDF = add_providerState(baseDF,npiProvidersDF)

    return baseDF

def add_provider_pos_info(baseDF, posDF):

    baseDF = add_providerStateFIPS(baseDF, posDF)
    baseDF = add_providerFIPS(baseDF, posDF)
    baseDF = add_providerOwner(baseDF, posDF)
    baseDF = add_providerIsCah(baseDF, posDF)

    return baseDF

def add_provider_info(baseDF, npiProvidersDF, cbsaDF, posDF, ersRuccDF, maPenetrationDF, costReportDF):

    baseDF = add_provider_npi_info(baseDF, npiProvidersDF)
    baseDF = add_provider_pos_info(baseDF, posDF)
    baseDF = add_providerRegion(baseDF)
    baseDF = add_providerCountyName(baseDF, cbsaDF)
    baseDF = add_providerRucc(baseDF, ersRuccDF)
    baseDF = add_providerMaPenetration(baseDF, maPenetrationDF)
    #right now I prefer cost report data because they seem to be about 99.5% complete, vs 80% complete for cbi
    #baseDF = add_cbi_info(baseDF, cbiDF)
    baseDF = add_provider_cost_report_info(baseDF, costReportDF)

    return baseDF

def add_osu(baseDF):

    osuNpi = ["1447359997"]  # set the NPI(s) I will use for OSU

    osuCondition = '(F.col("ORGNPINM").isin(osuNpi))' # do NOT forget the parenthesis!!

    # add a column to indicate which claims were at OSU
    baseDF = baseDF.withColumn("osu", 
                               F.when(eval(osuCondition) ,1) #set them to true
                                .otherwise(0)) #otherwise false

    return baseDF

def add_evtDrg(baseDF):

    evtDrgCodes=[23,24]

    evtDrgCondition = '(F.col("DRG_CD").isin(evtDrgCodes))'

    baseDF = baseDF.withColumn("evtDrg",
                               F.when( eval(evtDrgCondition), 1)
                                .otherwise(0))

    return baseDF

def add_evtPrcdr(baseDF):

    evtPrcdrCodes=("03CG3ZZ","03CH3ZZ","03CJ3ZZ","03CK3ZZ","03CL3ZZ","03CM3ZZ","03CN3ZZ","03CP3ZZ","03CQ3ZZ")

    prcdrCodeColumns = [f"ICD_PRCDR_CD{x}" for x in range(1,26)]

    baseDF = (baseDF.withColumn("prcdrCodeAll",
                                F.array(prcdrCodeColumns))
                    .withColumn("evtPrcdrCodes",
                                F.expr( f"filter(prcdrCodeAll, x -> x in {evtPrcdrCodes})"))
                    .withColumn("evtPrcdr",
                                F.when( F.size(F.col("evtPrcdrCodes"))>0,     1)
                                .otherwise(0)))

    baseDF = baseDF.drop("evtPrcdrCodes")

    #a different approach, not sure if this is slower/faster
    #evtPrcdrCodes=["03CG3ZZ","03CH3ZZ","03CJ3ZZ","03CK3ZZ","03CL3ZZ","03CM3ZZ","03CN3ZZ","03CP3ZZ","03CQ3ZZ"]
    #evtPrcdrCondition = '(' + '|'.join('(F.col(' + f'"ICD_PRCDR_CD{x}"' + ').isin(evtPrcdrCodes))' for x in range(1,26)) +')'
    #evtCondition = '( (F.col("evtDrg")==1) | (F.col("evtPrcdr")==1) )' # do NOT forget the parenthesis!!!
    #baseDF = baseDF.withColumn("evt", 
    #                           F.when(eval(evtCondition) ,1) #set them to true
    #                            .otherwise(0)) #otherwise false

    return baseDF

def add_evt(baseDF):

    # EVT takes place only in inpatient settings, EVT events are found on base claim file, not in the revenue center
    baseDF = add_evtDrg(baseDF)
    baseDF = add_evtPrcdr(baseDF)

    evtCondition = '( (F.col("evtDrg")==1) | (F.col("evtPrcdr")==1) )' # do NOT forget the parenthesis!!!

    baseDF = baseDF.withColumn("evt", 
                               F.when(eval(evtCondition) ,1) #set them to true
                                .otherwise(0)) #otherwise false

    return baseDF

def add_evtOsu(baseDF):

     return baseDF.withColumn("evtOsu", F.col("osu")*F.col("evt"))

def add_tpaDrg(baseDF):

    tpaDrgCodes = [61,62,63,65]

    tpaDrgCondition = '(F.col("DRG_CD").isin(tpaDrgCodes))'

    baseDF = baseDF.withColumn("tpaDrg",
                               F.when( eval(tpaDrgCondition), 1)
                                .otherwise(0))
    return baseDF

def add_tpaPrcdr(baseDF):

    tpaPrcdrCodes = ("3E03317", "3E03317")
                               
    prcdrCodeColumns = [f"ICD_PRCDR_CD{x}" for x in range(1,26)]

    baseDF = (baseDF.withColumn("prcdrCodeAll",
                                F.array(prcdrCodeColumns))
                    .withColumn("tpaPrcdrCodes",
                                F.expr( f"filter(prcdrCodeAll, x -> x in {tpaPrcdrCodes})"))
                    .withColumn("tpaPrcdr",
                                F.when( F.size(F.col("tpaPrcdrCodes"))>0,     1)
                                .otherwise(0)))

    baseDF = baseDF.drop("tpaPrcdrCodes")

    return baseDF

def add_tpaDgns(baseDF):

    #this diagnostic code is: Status post administration of tPA (rtPA) in a different facility within the last 24 
    #hours prior to admission to current facility, so this code should not be used to identify tpa performed at the provider of the claim (I think)
    tpaDgnsCodes = ("Z9282", "Z9282")
  
    dgnsCodeColumns = [f"ICD_DGNS_CD{x}" for x in range(1,26)]
 
    baseDF = (baseDF.withColumn("dgnsCodeAll",
                                F.array(dgnsCodeColumns))
                    .withColumn("tpaDgnsCodes",
                                F.expr( f"filter(dgnsCodeAll, x -> x in {tpaDgnsCodes})"))
                    .withColumn("tpaDgns",
                                F.when( F.size(F.col("tpaDgnsCodes"))>0,     1)
                                .otherwise(0)))

    baseDF = baseDF.drop("tpaDgnsCodes")
   
    return baseDF

def add_tpaCpt(baseDF):

    #CPT codes from: https://svn.bmj.com/content/6/2/194
    tpaCptCodes = ("37195", "37201", "37202")

    eachClaim = Window.partitionBy("CLAIMNO")

    #find tpa cpt codes in claims
    baseDF = (baseDF.withColumn("hcpcsCodeAll",
                                F.collect_set(F.col("HCPCS_CD")).over(eachClaim))
                    .withColumn("tpaCptCodes",
                                F.expr(f"filter(hcpcsCodeAll, x -> x in {tpaCptCodes})")))

    #if tpa cpt codes are found, then tpaCpt was performed
    baseDF = baseDF.withColumn("tpaCpt",
                               F.when( F.size(F.col("tpaCptCodes"))>0,     1)
                                .otherwise(0))

    baseDF = baseDF.drop("tpaCptCodes")

    return baseDF

def add_tpa(baseDF, inpatient=True):

    # tPA can take place in either outpatient or inpatient setting
    # however, in an efficient health care world, tPA would be administered at the outpatient setting
    # perhaps with the help of telemedicine and the bigger hub's guidance, and then the patient would be transferred to the bigger hospital
    # the diagnostic code from IP should be consistent with the procedure code from outpatient but check this
    # tpa can be found in DRG, DGNS and PRCDR codes

    #a different approach, not sure if this is faster/slower
    #tpaDgnsCodes = ["Z9282"]
    #tpaPrcdrCodes = ["3E03317"]
    #tpaPrcdrCondition = '(' + '|'.join('(F.col(' + f'"ICD_PRCDR_CD{x}"' + ').isin(tpaPrcdrCodes))' for x in range(1,26)) +')'
    #tpaDgnsCondition = '(' + '|'.join('(F.col(' + f'"ICD_DGNS_CD{x}"' + ').isin(tpaDgnsCodes))' for x in range(1,26)) +')'
    #if (inpatient):
    #    tpaCondition = '(' + tpaDrgCondition + '|' + tpaPrcdrCondition + '|' + tpaDgnsCondition + ')' # inpatient condition
    #else:
    #    tpaCondition = tpaPrcdrCondition # outpatient condition
    
    baseDF = add_tpaPrcdr(baseDF) #common for both inpatient and outpatient

    if (inpatient):
        baseDF = add_tpaDrg(baseDF) #used only in inpatient
        baseDF = add_tpaDgns(baseDF) #used only in inpatient
        tpaCondition = '( (F.col("tpaDrg")==1) | (F.col("tpaPrcdr")==1) | (F.col("tpaDgns")==1) )' # do NOT forget the parenthesis!!!
    else:
        tpaCondition = '( (F.col("tpaPrcdr")==1) )'

    baseDF = baseDF.withColumn("tpa",
                               F.when(eval(tpaCondition),1) # 1 if tpa was done during visit
                                .otherwise(0))
    return baseDF

def add_tpaOsu(baseDF):

    return baseDF.withColumn("tpaOsu", F.col("osu")*F.col("tpa"))

#def add_beneficiary_info(baseDF,mbsfDF): #assumes add_ssaCounty

    # assumes baseDF includes columns from add_admission_date_info and add_through_date_info
    # county codes can be an issue because MBSF includes a county code for mailing address and 12 county codes 
    # for each month, need to decide at the beginning which county code to use for each patient

    #baseDF = baseDF.join( 
    #                     mbsfDF
    #                         .select(
    #                            F.col("DSYSRTKY"),F.col("SEX"),F.col("RACE"),F.col("AGE"),F.col("RFRNC_YR"),F.col("ssaCounty")),
    #                     on = [ baseDF["DSYSRTKY"]==mbsfDF["DSYSRTKY"],
    #                            F.col("ADMSN_DT_YEAR")==F.col("RFRNC_YR")],
    #                     how = "inner")

    #baseDF=baseDF.drop(mbsfDF["DSYSRTKY"]).drop(mbsfDF["RFRNC_YR"]) #no longer need these

    #return baseDF

def add_beneficiary_info(baseDF, mbsfDF, cbsaDF, ersRuccDF):

    baseDF = add_age(baseDF, mbsfDF)
    baseDF = add_death_date_info(baseDF,mbsfDF)
    baseDF = add_daysDeadAfterVisit(baseDF)
    baseDF = add_90DaysDead(baseDF)
    baseDF = add_365DaysDead(baseDF)
 
    baseDF = add_fips_info(baseDF, cbsaDF) 
    baseDF = add_rucc(baseDF, ersRuccDF)
    baseDF = add_region(baseDF)

    return baseDF

def add_age(baseDF,mbsfDF):

    baseDF = baseDF.join(
                         mbsfDF
                             .select(
                                F.col("DSYSRTKY"),F.col("AGE"),F.col("RFRNC_YR")),
                         on = [ baseDF["DSYSRTKY"]==mbsfDF["DSYSRTKY"],
                                F.col("THRU_DT_YEAR")==F.col("RFRNC_YR")],
                         how = "left_outer")

    baseDF=baseDF.drop(mbsfDF["DSYSRTKY"]).drop(mbsfDF["RFRNC_YR"]) #no longer need these

    return baseDF

def add_ssaCounty(baseDF):

    baseDF = baseDF.withColumn("ssaCounty",
                               F.concat(
                                    #F.col("STATE_CD").substr(1,2),
                                    #F.format_string("%03d",F.col("CNTY_CD"))))
                                    F.col("STATE_CD"), F.col("CNTY_CD")))

    return baseDF

def add_fipsCounty(baseDF, cbsaDF):

    baseDF = baseDF.join(
                         cbsaDF
                            .select(
                                F.col("ssaCounty"),F.col("fipsCounty")),
                         on=["ssaCounty"],
                         how="left_outer")

    #drop the duplicate ssacounty, no longer needed
    #baseDF = baseDF.drop(F.col("ssaCounty"))

    return baseDF

def add_fipsState(baseDF):

    baseDF = baseDF.withColumn("fipsState", F.col("fipsCounty").substr(1,2))

    return baseDF

def add_fips_info(baseDF, cbsaDF):

    baseDF = add_fipsCounty(baseDF, cbsaDF)
    baseDF = add_fipsState(baseDF)

    return baseDF

def add_countyName(baseDF,cbsaDF):

    baseDF = baseDF.join(
                         cbsaDF
                            .select(F.col("countyName"),F.col("ssaCounty")),
                         on = ["ssaCounty"],
                         how = "inner")

    #baseDF = baseDF.drop(F.col("ssaCounty"))

    return(baseDF)

def add_rucc(baseDF, ersRuccDF):

    baseDF = baseDF.join(ersRuccDF
                             .select(F.col("FIPS"),F.col("RUCC_2013").alias("rucc")),
                          on=[F.col("FIPS")==F.col("fipsCounty")],
                          how="left_outer")

    baseDF = baseDF.drop("FIPS")

    return baseDF

def add_region(baseDF): 
    
    westCodes = ["04", "08", "16", "35", "30", "49", "32", "56", "02", "06", "15", "41", "53"] #state fips codes
    southCodes = ["10", "11", "12", "13", "24", "37", "45", "51", "54", "01", "21", "28", "47", "05", "22", "40", "48"]
    midwestCodes = ["18", "17", "26", "39", "55", "19", "20", "27", "29", "31", "38", "46"]
    northeastCodes = ["09", "23", "25", "33", "44", "50", "34", "36", "42"]
    
    westCondition = '(F.col("fipsState").isin(westCodes))'
    southCondition = '(F.col("fipsState").isin(southCodes))'
    midwestCondition = '(F.col("fipsState").isin(midwestCodes))'
    northeastCondition = '(F.col("fipsState").isin(northeastCodes))'

    baseDF = baseDF.withColumn("region",
                               F.when( eval(westCondition), 4)
                                .when( eval(southCondition), 3)
                                .when( eval(midwestCondition), 2)
                                .when( eval(northeastCondition), 1)
                                .otherwise(F.lit(None)))

    return baseDF

def add_regional_info(baseDF, censusDF):

    baseDF = (baseDF.join(
                           censusDF
                               .select(
                                   F.col("fipsCounty"),
                                   F.col("populationDensity"),
                                   F.col("bsOrHigher"),
                                   F.col("medianHouseholdIncome"),
                                   F.col("unemploymentRate")),
                           on=["fipsCounty"],
                           how="left"))

    return baseDF

def add_regional_info_from_ers(baseDF,ersPeopleDF, ersJobsDF, ersIncomeDF):

     #if you want to use usda ers data for the regional factors

     baseDF = (baseDF.join(
                           ersPeopleDF
                               .select(
                                   F.col("FIPS"),
                                   F.col("PopDensity2010"),F.col("Ed5CollegePlusPct")),
                            on=[F.col("FIPS")==F.col("fipsCounty")],
                            how="left"))

     #drop the duplicate column
     baseDF = baseDF.drop(F.col("FIPS"))

     baseDF = (baseDF.join(
                           ersJobsDF
                               .select(
                                   F.col("FIPS"),
                                   F.col("UnempRate2019")),
                            on=[F.col("FIPS")==F.col("fipsCounty")],
                            how="left"))

     #drop the duplicate column
     baseDF = baseDF.drop(F.col("FIPS"))

     baseDF = (baseDF.join(
                           ersIncomeDF
                               .select(
                                   F.col("FIPS"),
                                   F.col("Median_HH_Inc_ACS")),
                           on=[F.col("FIPS")==F.col("fipsCounty")],
                           how="left"))

     #drop the duplicate column
     baseDF = baseDF.drop(F.col("FIPS"))

     return baseDF

def get_aggregate_summary(baseDF, aggWhat, aggBy = "ssaCounty"): #aggWhat must be an iterable of strings-column names

    baseDF.persist() #since I will use this in a loop make it persist in memory
    baseDF.count()

    eachUnit = Window.partitionBy(aggBy)

    baseDF = baseDF.withColumn("total", #find need to find total in unit
                               F.count(F.col(aggBy)).over(eachUnit))
    returnWhat = [aggBy, "total"]

    for i in aggWhat:
        baseDF = baseDF.withColumn(i+"InUnit", #add unit counts
                                   F.sum(
                                       F.col(i)).over(eachUnit))
        returnWhat = returnWhat + [f'{i}InUnit']

        baseDF = baseDF.withColumn(i+"InUnitPerCent", #add unit percentage
                                   F.round(
                                       100.*F.col(i+"InUnit") / F.col("total"),1))
        returnWhat = returnWhat + [f'{i}InUnitPerCent']

    aggregateSummary = baseDF.select(returnWhat).distinct() #returnWhat cannot be tuple, list works

    aggregateSummary.persist() #since a loop was involved in calculating this make it persist in memory
    aggregateSummary.count()

    return aggregateSummary

def test_get_aggregate_summary(summaryDF):

    #summaryColumns = summaryDF.colRegex("`.+InUnit$`") #not working, not a list
    summaryColumns = [column for column in summaryDF.columns if column.endswith("InUnit")]
    allWell = 1
    # check for any unreasonable results
    for col in summaryColumns:
        if (summaryDF.filter( F.col(col) > F.col("total") ).count() != 0):
            print(f'F.col({col}) > F.col("total") failed')
            allWell = 0

        if (summaryDF.filter( 
                            (F.col(col).contains('None')) |
                            (F.col(col).contains('NULL')) |
                            (F.col(col) == '' ) |
                            (F.col(col).isNull()) |
                            (F.isnan(col)) ).count() != 0 ):
            print(f'{col} includes meaningless entries')
            allWell = 0

        if (summaryDF.filter( 
                            (F.col(col) < 0 )).count() != 0 ):
            print(f'{col} includes negative entries')
            allWell = 0

    if (allWell==1):
        print("No issues found.")

def add_gach(baseDF, npiProvidersDF, primary=True):

    gachColName = "gachPrimary" if primary else "gachAll"

    # join with general acute care hospital GACH flag
    baseDF = baseDF.join(npiProvidersDF.select(
                                            F.col("NPI"), F.col(gachColName).alias("gach")),
                         on = [baseDF["ORGNPINM"] == npiProvidersDF["NPI"]],
                         how = "inner")

    # the join will keep NPI as it is a different name
    baseDF = baseDF.drop(F.col("NPI"))

    return baseDF

def add_rehabilitationFromTaxonomy(baseDF, npiProvidersDF, primary=True):

    rehabilitationColName = "rehabilitationPrimary" if primary else "rehabilitationAll"

    # join with rehabilitation flag
    baseDF = baseDF.join(npiProvidersDF.select(
                                            F.col("NPI"), F.col(rehabilitationColName).alias("rehabilitationFromTaxonomy")),
                         on = [baseDF["ORGNPINM"] == npiProvidersDF["NPI"]],
                         how = "left_outer")

    # the join will keep NPI as it is a different name
    baseDF = baseDF.drop(F.col("NPI"))

    return baseDF

def add_rehabilitationFromCCN(baseDF):

    #https://www.cms.gov/regulations-and-guidance/guidance/transmittals/downloads/r29soma.pdf
    #this function is using CCN numbers to flag rehabilitation hospitals and rehabilitation units within hospitals
    # an example: https://pubmed.ncbi.nlm.nih.gov/18996234/
    baseDF = baseDF.withColumn("rehabilitationFromCCN",
                               F.when(
                                   ((F.substring(F.col("PROVIDER"),3,4).cast('int') >= 3025) & (F.substring(F.col("PROVIDER"),3,4).cast('int') <= 3099)) |
                                    (F.substring(F.col("PROVIDER"),3,1)=="T"), 1)
                                .otherwise(0))

    return baseDF 

def add_hospital(baseDF):

    baseDF = baseDF.withColumn( "hospital",
                                F.when( F.col("FAC_TYPE") == 1, 1)
                                 .otherwise(0))

    return baseDF

#two ways of adding some useful hospital information (rural/urban, number of beds etc): from community benefits insight (CBI), from hospital cost report
#right now I prefer cost report data because they seem to be about 99.5% complete, vs 80% complete for cbi
#I have checked the address I get from both with the address that NPI providers file has and all seemed correct
#but the number of beds in cbi and cost report are different quite often, and sometimes off by 50, 60%...
#I have not figured out a way to validate those, for now I trust the CMS cost report data

def add_cbi_info(baseDF,cbiDF):

    #correspondence with RESDAC on using Provider numbers, and why I am not using ORGNPINM to do the join
    #My question: is the NPI to Medicare Provider Number correspondence 1-to-1? If yes, what is the best way to convert a NPI 
    #number to a Medicare Provider Number and vice versa? If not, what do you recommend in this case?
    #NPIs and CCN may not be 1:1 depending on what you are looking for.
    #NPIs can be organizational and individual. CCNs are typically only organizational and only seen in the institutional claims paid under Part A. 
    #They will not correspond to individual physicians/providers. https://www.cms.gov/Regulations-and-Guidance/Guidance/Transmittals/downloads/R29SOMA.pdf
    #Also, a single address/facility may have more than one CCN. CCN’s are 6 digits. The first 2 digits identify the State in which the provider is located. 
    #The last 4 digits identify the type of facility. The CCN continues to serve a critical role in verifying that a provider has been Medicare 
    #certified and for what type of services.
    #Unfortunately, CMS did not create a crosswalk of NPI to CCN. Researchers have to do it themselves.
    #The NPPES is the publicly available file of NPIs. Providers were able to list their CCN on the NPI application form but it is subject to 
    #whether the provider filed out that part of the application so the NPPES is not a perfect resource.
    #There is also the Provider of Service file which has a ‘Provider ID’ which is the CCN. The file does not include the NPI but has facility 
    #name and address so researchers can try to link between facility name and address in the POS and the NPPES file.

    baseDF = baseDF.join(cbiDF.select(
                                   F.col("hospital_bed_count"), F.col("urban_location_f"), F.col("medicare_provider_number")),
                                   #F.col("street_address"), F.col("name")),
                         on=[ F.col("medicare_provider_number")==F.col("PROVIDER") ],
                         how="left_outer")

    baseDF = baseDF.drop(F.col("medicare_provider_number"))

    return baseDF

def add_provider_cost_report_info(baseDF,costReportDF):

    baseDF = baseDF.join(costReportDF
                           .select(
                               F.col("Provider CCN"),
                               F.col("Rural Versus Urban").alias("providerRuralVersusUrban"),
                               F.col("Number of Beds").alias("providerNumberOfBeds"),
                               F.col("Number of Interns and Residents (FTE)").alias("providerNumberOfResidents")),
                         #see note on add_cbi_info on why I am not using ORGNPINM for the join
                         #hospital cost report files include only the CMS Certification Number (Provider ID, CCN), they do not include NPI
                         on=[F.col("Provider CCN")==F.col("Provider")],
                         how="left_outer")

    baseDF = baseDF.drop("Provider CCN")

    return baseDF

def add_transferToIn(baseDF):

    baseDF = baseDF.withColumn( "transferToIn", #transfer implies that it was a different organization
                                F.when( F.col("STUS_CD").isin([2,5]), 1) #visits that resulted in a discharge to short term hospital (code 2) or other IPT care (code 5)
                                 .otherwise(0))

    return baseDF

def add_death_date_info(baseDF,mbsfDF): #assumes that add_death_date_info has been run on mbsfDF

    baseDF = baseDF.join( mbsfDF.filter(
                                         F.col("V_DOD_SW")=="V")
                                 .select(
                                        F.col("DSYSRTKY"),
                                        F.col("DEATH_DT_DAYOFYEAR"),F.col("DEATH_DT_YEAR"), 
                                        F.col("DEATH_DT_DAY"), F.col("DEATH_DT"),
                                        F.col("RFRNC_YR")),
                           on=[ baseDF["DSYSRTKY"]==mbsfDF["DSYSRTKY"],
                                F.col("THRU_DT_YEAR")==F.col("RFRNC_YR") ],
                           how="left_outer") #uses null when the beneficiary does not have a valid death date in mbsfDF

    baseDF=baseDF.drop(mbsfDF["DSYSRTKY"]).drop(mbsfDF["RFRNC_YR"]) #no longer need these

    return baseDF

def add_daysDeadAfterVisit(baseDF): #assumes add_through_date_info and add_death_date_info (both from mbsf.py and base.py) have been run

    baseDF = (baseDF.withColumn( "daysDeadAfterVisit",
                                 F.col("DEATH_DT_DAY")-F.col("THRU_DT_DAY")))
                                 
    return baseDF
            
def add_90DaysDead(baseDF): #this is the 90 day mortality flag, assumes I have run add_daysDeadAfterVisit

    baseDF = baseDF.withColumn( "90DaysDead",
                                 F.when( F.col("daysDeadAfterVisit") <= 90, 1)
                                  .otherwise(0))

    return baseDF

def add_365DaysDead(baseDF): #this is the 365 day mortality flag

    baseDF = baseDF.withColumn( "365DaysDead",
                                 F.when( F.col("daysDeadAfterVisit") <= 365, 1)
                                  .otherwise(0))

    return baseDF

def add_los(baseDF): #length of stay = los, assumes add date infos

    #https://resdac.org/cms-data/variables/day-count-length-stay
    baseDF = baseDF.withColumn("los",
                               F.col("THRU_DT_DAY")-F.col("ADMSN_DT_DAY")+1)

    #replace 0 with 1 in length of stay, that is how RESDAC calculates LOS
    #not needed any more, since I added the +1 above according to the definition on the resdac link
    #baseDF = baseDF.replace(0,1,subset="los") 

    return baseDF

def add_losDays(baseDF): #adds an array of all days of the claim's duration
    
    baseDF = baseDF.withColumn("losDays",
                              F.sequence( F.col("ADMSN_DT_DAY"),F.col("THRU_DT_DAY") ))
    
    return baseDF

def add_losDaysOverXUntilY(baseDF,X="CLAIMNO",Y="THRU_DT_DAY"):
    
    #add a sequence of days that represents length of stay
    baseDF = add_losDays(baseDF)
    
    #everything here will be done using this X as a partition
    eachX = Window.partitionBy(X)

    #find the set of all length of stay days for each X
    baseDF = baseDF.withColumn(f"losDaysOver{X}",
                               F.array_distinct(
                                    F.flatten(
                                        F.collect_set(F.col("losDays")).over(eachX))))
    
    #now filter that set for all days prior to Y
    baseDF = baseDF.withColumn(f"losDaysOver{X}Until{Y}",
                              F.expr(f"filter(losDaysOver{X}, x -> x < {Y})"))

    #replace null values with empty arrays [] so that any concatenation later will happen correctly
    baseDF = baseDF.withColumn(f"losDaysOver{X}Until{Y}",
                              F.coalesce( F.col(f"losDaysOver{X}Until{Y}"), F.array() ))
    
    return baseDF

def add_losOverXUntilY(baseDF,X="CLAIMNO",Y="THRU_DT_DAY"):

    #add a sequence of days that represents length of stay
    baseDF = add_losDays(baseDF)
    
    #find the sequence of los days over X until Y
    baseDF = add_losDaysOverXUntilY(baseDF,X=X,Y=Y)
    
    #length of stay is then the number of those days
    baseDF = baseDF.withColumn(f"losOver{X}Until{Y}",
                              F.size(F.col(f"losDaysOver{X}Until{Y}")))
    
    return baseDF

def add_providerMaPenetration(baseDF, maPenetrationDF):

    #from one test with stroke inpatient claims, this was about 90% complete
    baseDF = baseDF.join(maPenetrationDF
                          .select(F.col("FIPS"),F.col("Penetration").alias("providerMaPenetration"),F.col("Year")),
                         on=[ (F.col("FIPS")==F.col("providerFIPS")) & (F.col("Year")==F.col("THRU_DT_YEAR")) ],
                         how="left_outer")

    baseDF = baseDF.drop("FIPS")

    return baseDF

def add_providerRucc(baseDF, ersRuccDF):

    baseDF = baseDF.join(ersRuccDF
                             .select(F.col("FIPS"),F.col("RUCC_2013").alias("providerRucc")),
                          on=[F.col("FIPS")==F.col("providerFIPS")],
                          how="left_outer")

    baseDF = baseDF.drop("FIPS")

    return baseDF
     
def add_nihss(baseDF):

    #https://www.ahajournals.org/doi/10.1161/CIRCOUTCOMES.122.009215
    #https://www.cms.gov/files/document/2021-coding-guidelines-updated-12162020.pdf  page 80

    dgnsColumnList = [f"ICD_DGNS_CD{x}" for x in range(1,26)] #all 25 DGNS columns

    baseDF = (baseDF.withColumn("dgnsList", #add an array of all dgns codes found in their claims
                               F.array(dgnsColumnList))
                   .withColumn("nihssList", #keeps codes that match the regexp pattern
                               F.expr(f'filter(dgnsList, x -> x rlike "R297[0-9][0-9]?")'))
                   .withColumn("nihss",
                               F.when(
                                   F.size(F.col("nihssList")) == 1, F.substring(F.col("nihssList")[0],5,2))
                                .otherwise(F.lit(None)))
                   .withColumn("nihss", F.col("nihss").cast('int'))
                   .drop("dgnsList","nihssList"))

    return baseDF

def add_claim_stroke_info(baseDF, inpatient=True):

    baseDF = add_claim_stroke_treatment_info(baseDF, inpatient=inpatient)
    baseDF = add_nihss(baseDF)

    return baseDF

def add_claim_stroke_treatment_info(baseDF, inpatient=True):

    baseDF = add_tpa(baseDF, inpatient=inpatient)
    if (inpatient):
        baseDF = add_evt(baseDF)

    return baseDF

def add_processed_name(baseDF,colToProcess="providerName"):

    processedCol = colToProcess + "Processed"

    baseDF = (baseDF.withColumn(processedCol, 
                                F.regexp_replace( 
                                    F.trim( F.lower(F.col(colToProcess)) ), "\'s|\&|\.|\,| llc| inc| ltd| lp| lc|\(|\)| program", "") ) #replace with nothing
                    .withColumn(processedCol, 
                                F.regexp_replace( 
                                    F.col(processedCol) , "-| at | of | for | and ", " ") )  #replace with space
                    .withColumn(processedCol, 
                                 F.regexp_replace( 
                                     F.col(processedCol) , " {2,}", " ") )) #replace more than 2 spaces with one space 

    return baseDF

def add_cothMember(baseDF, teachingHospitalsDF):

    baseDF = baseDF.join(teachingHospitalsDF
                            .select(
                                F.col("cothMember"),
                                F.col("Medicare ID")),
                         on = [F.col("Medicare ID")==F.col("PROVIDER")],
                         how = "left_outer")

    baseDF = baseDF.drop("Medicare ID")

    return baseDF

def add_rbr(baseDF, teachingHospitalsDF): # resident to bed ratio

    #I tried using hospGme2021 data to find the RBR but that ended up being much more incomplete than the AAMC data
    #so I am now using AAMC data

    baseDF = baseDF.join(teachingHospitalsDF
                            .select(
                                F.col("FY20 IRB").cast('double').alias("rbr"),
                                F.col("Medicare ID")),
                         on = [F.col("Medicare ID")==F.col("PROVIDER")],
                         how = "left_outer")

    baseDF = baseDF.drop("Medicare ID")

    return baseDF

def add_acgmeXInZip(baseDF,acgmeXDF, X="Sites"):

    eachZip = Window.partitionBy(f"{X.lower()[:-1]}Zip") #Sites->site, Programs->program

    acgmeXDF = acgmeXDF.withColumn(f"acgme{X}InZip",
                                           F.collect_set( F.col(f"{X.lower()[:-1]}NameProcessed")).over(eachZip))

    baseDF = baseDF.join(acgmeXDF
                             .select(
                                 F.col(f"acgme{X}InZip"), F.col(f"{X.lower()[:-1]}Zip"))
                             .distinct(),
                         on=[ F.col("providerZip")==F.col(f"{X.lower()[:-1]}Zip") ],
                         how="left_outer")

    return baseDF

def add_acgmeX(baseDF,acgmeXDF, X="Site"):

    baseDF = add_processed_name(baseDF,colToProcess="providerName")
    baseDF = add_processed_name(baseDF,colToProcess="providerOtherName")

    baseDF = add_acgmeXInZip(baseDF,acgmeXDF,X=f"{X}s") #Site->Sites, Program->Programs....

    baseDF = (baseDF.withColumn("nameDistance", 
                                F.expr(f"transform( acgme{X}sInZip, x -> levenshtein(x,providerNameProcessed))"))
                    .withColumn("otherNameDistance", 
                                F.expr(f"transform( acgme{X}sInZip, x -> levenshtein(x,providerOtherNameProcessed))"))
                    .withColumn("minNameDistance", 
                                F.array_min(F.col("nameDistance")))
                    .withColumn("minOtherNameDistance", 
                                F.array_min(F.col("otherNameDistance")))
                    .withColumn("minDistance", 
                                F.least( F.col("minNameDistance"), F.col("minOtherNameDistance")) )
                    .withColumn("providerNameIsContained",
                                F.expr(f"filter( acgme{X}sInZip, x -> x rlike providerNameProcessed )"))
                    .withColumn("providerOtherNameIsContained",
                                F.expr(f"filter( acgme{X}sInZip, x -> x rlike providerOtherNameProcessed )")))

    baseDF = baseDF.withColumn(f"acgme{X}",
                               F.when( (F.col("minDistance") < 4) | #could use either an absolute or relative cutoff
                                       (F.size(F.col("providerNameIsContained"))>0) |
                                       (F.size(F.col("providerOtherNameIsContained"))>0), 1)
                                .otherwise(0))

    baseDF = baseDF.drop("nameDistance","otherNameDistance","minNameDistance","minOtherNameDistance","minDistance","providerNameIsContained","providerOtherNameIsContained", f"acgme{X}sInZip")

    return baseDF

def add_aamcTeachingHospital(baseDF,aamcHospitalsDF):

    baseDF = baseDF.join(aamcHospitalsDF
                            .select(F.col("Medicare ID"),F.col("teachingStatus").alias("aamcTeachingHospital")),
                         on=[F.col("Medicare ID")==F.col("PROVIDER")],
                         how="left_outer")

    baseDF = baseDF.drop("Medicare ID")

    return baseDF

def add_aamcMajorTeachingHospital(baseDF,aamcHospitalsDF):

    baseDF = baseDF.join(aamcHospitalsDF
                            .select(F.col("Medicare ID"),F.col("majorTeachingStatus").alias("aamcMajorTeachingHospital")),
                         on=[F.col("Medicare ID")==F.col("PROVIDER")],
                         how="left_outer")

    baseDF = baseDF.drop("Medicare ID")

    return baseDF


def add_teachingHospital(baseDF, aamcHospitalsDF, acgmeProgramsDF):

    #definition of a teaching hospital:  https://hcup-us.ahrq.gov/db/vars/hosp_bedsize/nisnote.jsp

    baseDF = add_rbr(baseDF,aamcHospitalsDF)
    baseDF = add_cothMember(baseDF, aamcHospitalsDF)
    baseDF = add_acgmeX(baseDF, acgmeProgramsDF, X="Program")

    baseDF = baseDF.withColumn("teachingHospital",
                                F.when( 
                                    (F.col("cothMember") == 1) |
                                    (F.col("rbr") >= 0.25) |
                                    (F.col("acgmeProgram") == 1), 1)
                                 .otherwise(0))

    return baseDF
  
def prep_baseDF(baseDF, claim="inpatient"):

    #add some date-related info
    #baseDF = cast_columns_as_int(baseDF,claim=claim)
    baseDF = enforce_schema(baseDF, claim=claim)
    baseDF = add_through_date_info(baseDF,claim=claim)
    #baseDF = cast_columns_as_string(baseDF,claim=claim)

    if (claim=="inpatient"):
        baseDF = add_discharge_date_info(baseDF,claim=claim)
        baseDF = add_admission_date_info(baseDF,claim=claim)
        #add SSA county of beneficiaries
        baseDF = add_ssaCounty(baseDF)
        #without a repartition, the dataframe is extremely skewed...
        #baseDF = baseDF.repartition(128, "DSYSRTKY")
    elif ( (claim=="snf") | (claim=="hosp") | (claim=="hha") ):
        baseDF = add_admission_date_info(baseDF,claim=claim)
        #without a repartition, the dataframe is extremely skewed...
        #baseDF = baseDF.repartition(128, "DESY_SORT_KEY")
    elif ( claim=="outpatient" ):
        #add SSA county of beneficiaries
        baseDF = add_ssaCounty(baseDF)
        #without a repartition, the dataframe is extremely skewed...
        #baseDF = baseDF.repartition(128, "DSYSRTKY")

    return baseDF

def add_strokeCenterCamargo(baseDF,strokeCentersCamargoDF):

    baseDF = baseDF.join(strokeCentersCamargoDF.select(
                                                   F.col("CCN"),F.col("strokeCenterCamargo")),
                         on=[F.col("CCN")==F.col("PROVIDER")],
                         how="left_outer")

    baseDF = baseDF.fillna(0,subset="strokeCenterCamargo")

    baseDF = baseDF.drop("CCN")

    return baseDF    

def add_providerOwner(baseDF, posDF):

    baseDF = baseDF.join(posDF
                          .select( F.col("GNRL_CNTL_TYPE_CD"),F.col("PRVDR_NUM")),
                         on=[F.col("PRVDR_NUM")==F.col("PROVIDER")],
                         how="left_outer")

    baseDF = baseDF.drop("PRVDR_NUM")

    return baseDF

def add_numberOfResidents(baseDF, hospCostDF):

    baseDF = baseDF.join(hospCostDF
                          .select( F.col("Number of Interns and Residents (FTE)").alias("numberOfResidents"),
                                   F.col("Provider CCN")),
                         on=[F.col("Provider CCN")==F.col("PROVIDER")],
                         how="left_outer")

    baseDF = baseDF.drop("Provider CCN")

    return baseDF

def add_providerIsCah(baseDF, posDF): #critical access hospital

    baseDF = baseDF.join(posDF
                          .select(F.col("PRVDR_NUM"), F.col("cah").alias("providerIsCah")),
                         on=[F.col("PRVDR_NUM")==F.col("PROVIDER")],
                         how="left_outer")

    baseDF = baseDF.drop("PRVDR_NUM")

    return baseDF

def add_providerStrokeVol(baseDF, stroke="anyStroke"):

    eachProvider = Window.partitionBy("ORGNPINM")

    baseDF = baseDF.withColumn("providerStrokeVol",
                               F.sum( F.col(stroke) ).over(eachProvider))

    return baseDF

def add_providerEvtVol(baseDF):

    eachProvider = Window.partitionBy("ORGNPINM")

    baseDF = baseDF.withColumn("providerEvtVol",
                               F.sum( F.col("evt") ).over(eachProvider))

    return baseDF

def add_providerTpaVol(baseDF):

    eachProvider = Window.partitionBy("ORGNPINM")

    baseDF = baseDF.withColumn("providerTpaVol",
                               F.sum( F.col("tpa") ).over(eachProvider))

    return baseDF

def add_providerMeanEvt(baseDF):

    eachProvider = Window.partitionBy("ORGNPINM")

    baseDF = baseDF.withColumn("providerMeanEvt",
                               F.mean( F.col("evt") ).over(eachProvider))

    return baseDF

def add_providerMeanTpa(baseDF):

    eachProvider = Window.partitionBy("ORGNPINM")

    baseDF = baseDF.withColumn("providerMeanTpa",
                               F.mean( F.col("tpa") ).over(eachProvider))

    return baseDF

def add_provider_stroke_treatment_info(baseDF, inpatient=True):

    baseDF = add_providerMeanTpa(baseDF)
    baseDF = add_providerTpaVol(baseDF)
    if (inpatient):
        baseDF = add_providerMeanEvt(baseDF)   
        baseDF = add_providerEvtVol(baseDF)

    return baseDF

def add_provider_stroke_info(baseDF, strokeCentersCamargoDF, inpatient=True, stroke="anyStroke"):

    baseDF = add_provider_stroke_treatment_info(baseDF, inpatient=inpatient)
    baseDF = add_providerStrokeVol(baseDF, stroke=stroke)
    baseDF = add_strokeCenterCamargo(baseDF,strokeCentersCamargoDF)

    return baseDF

def add_numberOfClaims(baseDF):

    eachDsysrtky = Window.partitionBy("DSYSRTKY")

    baseDF = baseDF.withColumn("numberOfClaims",
                               F.size(F.collect_set(F.col("CLAIMNO")).over(eachDsysrtky)))

    return baseDF

def filter_beneficiaries(baseDF, mbsfDF):

    baseDF = baseDF.join(mbsfDF.select(F.col("DSYSRTKY")), 
                         on=["DSYSRTKY"],
                         how="left_semi")

    return baseDF

def enforce_schema(baseDF, claim="inpatient"):

    baseDF = cast_columns_as_int(baseDF,claim=claim)
    baseDF = cast_columns_as_string(baseDF,claim=claim)

    #now enforce the schema set for base
    if claim=="inpatient":
        baseDF = baseDF.select([baseDF[field.name].cast(field.dataType) for field in ipBaseSchema.fields])

    return baseDF


   







