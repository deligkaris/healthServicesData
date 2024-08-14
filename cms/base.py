import pyspark.sql.functions as F
from pyspark.sql.window import Window
from .mbsf import add_ohResident
from .revenue import filter_claims, add_ed, add_mri, add_ct, get_revenue_summary
from .claims import get_claimsDF
from utilities import add_primaryTaxonomy, add_acgmeSitesInZip, add_acgmeProgramsInZip, daysInYearsPrior, usRegionFipsCodes
import re

#CMS DUA email on my question about LDS claim numbers:
#"The Claim ID is set by a sequence. A Part A (Institutional) and a Part B (Professional) Claim could have the same ID. 
#However, Claim Type would be unique.
#The Claim ID is not reset by year or claim type. When a claim is added, the sequence is incremented."
#RESDAC email: 
#"CCW just got back to me - for the LDS claims, the CLAIMNO variable is reset to 1 each year.
#We recommend that both the bene_id and claim_id are used together to identify unique claims"
#my note:
#this means that I need to be very careful when I use claims from multiple years and when I join
#information from the revenue center and line files to base.
#only RIFs will include unique claim numbers and I suspect the CMS DUA staff member that responded to my question did not realize
#I was referring to LDS and not RIF, it seems that CCW is responsible for creating the LDS
#a page with LDS, RIF differences: https://resdac.org/articles/differences-between-rif-lds-and-puf-data-files
#they do not include the claimno reset in LDS though.....
# medpar resdac video: 
# 1) ip claims: diagnosis codes are on discharge, admitting diagnosis code is often symptom-based, and vague
# 2) ip claims: most diagnosis codes are firm, rule out diagnosis codes most often not noted at all on hospitalization
# 3) ip claims: larger hospitals may have better systems for their coders or at least better quality control
#               (eg in rural hospitals patients may apper to be healthier and with worse outcomes but this could be just
#                a result of differences in coding)
# 4) ip claims: each year dataset includes claims of that year's claim through date (in contrast with the medpar file)
# 5) ip claims: discharge dates may be different than claim through dates
# 6) ip claims: a hospital stay may be broken in more than 1 claim (same dsysrtky, same admission date, same provider) but it happens
#               rarely (so need to check if this affects the project)
# 7) general advice: always good to have clinicians, billers in your team but your hospital may be doing things differently than other
#                    hospitals! (eg differences in academic institutions versus community hospitals)
# 8) ip claims: present on admission codes, some hospitals are not required to report these (eg if they are in the PPS system)
# 9) ip claims: there are medical and surgical DRG codes, need to be very broad if the goal is to identify a clinical population,
#               perhaps use diagnostic codes to narrow down the DRG codes


def add_admission_date_info(baseDF, claimType="op"):
    #unfortunately, SNF claims have a different column name for admission date
    #admissionColName = "CLM_ADMSN_DT" if claim=="snf" else "ADMSN_DT"
    if ( (claimType=="hha") ):
        baseDF = baseDF.withColumn( "ADMSN_DT", F.col("HHSTRTDT"))
    elif ( (claimType=="hosp") ):
        baseDF = baseDF.withColumn( "ADMSN_DT", F.col("HSPCSTRT") )
    baseDF = (baseDF.withColumn("ADMSN_DT_DAYOFYEAR", 
                                F.date_format(
                                   #ADMSN_DT was read as bigint, need to convert it to string that can be understood by date_format
                                   F.concat_ws('-',F.col("ADMSN_DT").substr(1,4),F.col("ADMSN_DT").substr(5,2),F.col("ADMSN_DT").substr(7,2)), 
                                   "D" #get the day of the year
                                ).cast('int'))
                    # keep the year too
                    .withColumn( "ADMSN_DT_YEAR", F.col("ADMSN_DT").substr(1,4).cast('int'))
                    # find number of days from yearStart-1 to year of admission -1
                    .withColumn( "ADMSN_DT_DAYSINYEARSPRIOR", daysInYearsPrior[F.col("ADMSN_DT_YEAR")])
                    # days in years prior to admission + days in year of admission = day nunber
                    .withColumn("ADMSN_DT_DAY", (F.col("ADMSN_DT_DAYSINYEARSPRIOR") + F.col("ADMSN_DT_DAYOFYEAR")).cast('int')))
    return baseDF

def add_discharge_date_info(baseDF, claimType="op"):
    #unfortunately, SNF claims have a different column name for discharge date
    if (claimType=="snf"):         
        baseDF = baseDF.withColumn( "DSCHRGDT", F.col("NCH_BENE_DSCHRG_DT"))
    baseDF = (baseDF.withColumn("DSCHRGDT_DAYOFYEAR",
                                F.date_format(
                                    #THRU_DT was read as bigint, need to convert it to string that can be understood by date_format
                                    F.concat_ws('-',F.col("DSCHRGDT").substr(1,4),F.col("DSCHRGDT").substr(5,2),F.col("DSCHRGDT").substr(7,2)),
                                    "D" #get the day of the year
                                ).cast('int'))
                    # keep the claim through year too
                    .withColumn( "DSCHRGDT_YEAR", F.col("DSCHRGDT").substr(1,4).cast('int'))
                    # find number of days from yearStart-1 to year of admission -1
                    .withColumn( "DSCHRGDT_DAYSINYEARSPRIOR", daysInYearsPrior[F.col("DSCHRGDT_YEAR")])
                    # days in years prior to admission + days in year of admission = day nunber
                    .withColumn( "DSCHRGDT_DAY", (F.col("DSCHRGDT_DAYSINYEARSPRIOR") + F.col("DSCHRGDT_DAYOFYEAR")).cast('int')))
    return baseDF

def add_XDaysFromYDAY(baseDF, YDAY="ADMSN_DT_DAY", X=90):
    baseDF = baseDF.withColumn(f"{X}DaysFrom{YDAY}", F.col(YDAY)+X )
    return baseDF

def add_ishStrokeDgns(baseDF):
    # PRNCPAL_DGNS_CD: diagnosis, condition problem or other reason for the admission/encounter/visit to 
    # be chiefly responsible for the services, redundantly stored as ICD_DGNS_CD1
    # ADMTG_DGNS_CD: initial diagnosis at admission, may not be confirmed after evaluation, 
    # may be different than the eventual diagnosis as in ICD_DGNS_CD1-25
    # which suggests that the ICD_DGNS_CDs are after evaluation, therefore ICD_DGNS_CDs are definitely not rule-out 
    # JB: well, you can never be certain that they are not rule-out, but the principal diagnostic code for stroke has been validated
    baseDF = baseDF.withColumn("ishStrokeDgns",
                              # ^I63[\d]: beginning of string I63 matches 0 or more digit characters 0-9
                              # I63 cerebral infraction 
                              F.when( F.regexp_extract( F.col("PRNCPAL_DGNS_CD"), '^I63[\d]*',0) !='', 1)
                               .otherwise(0)) 
    return baseDF

def add_ishStrokeDrg(baseDF):
    #https://svn.bmj.com/content/6/2/194
    ishStrokeDrgCodes=[61,62,63]
    ishStrokeDrgCondition = '(F.col("DRG_CD").isin(ishStrokeDrgCodes))'
    baseDF = baseDF.withColumn("ishStrokeDrg", F.when( eval(ishStrokeDrgCondition), 1).otherwise(0))
    return baseDF

def add_ishStroke(baseDF, inpatient=True):
    baseDF = add_ishStrokeDgns(baseDF)
    if inpatient:
        baseDF = add_ishStrokeDrg(baseDF)
        baseDF = baseDF.withColumn("ishStroke", F.when( (F.col("ishStrokeDgns")==1) | (F.col("ishStrokeDrg")==1), 1).otherwise(0))
    else:
        baseDF = baseDF.withColumn("ishStroke", F.when( F.col("ishStrokeDgns")==1, 1).otherwise(0))
    return baseDF

def add_otherStroke(baseDF):
    #Stroke, not specified as haemorrhage or infarction
    #https://icd.who.int/browse10/2016/en#/I60-I69
    baseDF = baseDF.withColumn("otherStroke",
                              # ^I64[\d]: beginning of string I64 matches 0 or more digit characters 0-9
                              F.when( F.regexp_extract( F.col("PRNCPAL_DGNS_CD"), '^I64[\d]*',0) !='', 1)
                               .otherwise(0))
    return baseDF

def add_ichStroke(baseDF):
    baseDF = baseDF.withColumn("ichStroke",
                              # ^I61[\d]: beginning of string I61 matches 0 or more digit characters 0-9
                              F.when( F.regexp_extract( F.col("PRNCPAL_DGNS_CD"), '^I61[\d]*',0) !='', 1)
                               .otherwise(0)) 
    return baseDF

def add_tiaStroke(baseDF):
    baseDF = baseDF.withColumn("tiaStroke",
                              # ^G45[\d]: beginning of string I61 matches 0 or more digit characters 0-9
                              F.when( F.regexp_extract( F.col("PRNCPAL_DGNS_CD"), '^G45[\d]*',0) !='', 1)
                               .otherwise(0)) 
    return baseDF

def add_anyStroke(baseDF):
    '''anyStroke refers to any type of stroke that is at the moment present in the df'''
    anyStrokeCondition = '(' + '|'.join(f"(F.col('{stroke}')==1)" for stroke in 
                                        ["ishStroke", "otherStroke", "ichStroke","tiaStroke"] if stroke in baseDF.columns) + ')'
    baseDF = baseDF.withColumn("anyStroke", F.when( eval(anyStrokeCondition), 1).otherwise(0))
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
    baseDF = (baseDF.withColumn("dbsPrcdrCodes", F.expr( f"filter(prcdrCodeAll, x -> x in {dbsPrcdrCodes})")))

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
    baseDF = (baseDF.withColumn("firstADMSN_DT_DAY", F.min(F.col("ADMSN_DT_DAY")).over(eachDsysrtky))
                    .withColumn("firstClaim", #and mark it/them (could be more than 1)
                                F.when(F.col("ADMSN_DT_DAY")==F.col("firstADMSN_DT_DAY"),1).otherwise(0))
                    .drop("firstADMSN_DT_DAY"))
    return baseDF

def add_firstClaimSum(baseDF):
    eachDsysrtky=Window.partitionBy("DSYSRTKY")
    baseDF = (baseDF.withColumn("firstClaimSum", F.sum(F.col("firstClaim")).over(eachDsysrtky)))
    return baseDF

def add_lastClaim(baseDF):

    eachDsysrtky=Window.partitionBy("DSYSRTKY")

    baseDF = (baseDF.withColumn("lastTHRU_DT_DAY",
                                F.max(F.col("THRU_DT_DAY")).over(eachDsysrtky))
                    .withColumn("lastClaim", #and mark it/them (could be more than 1)
                                F.when(F.col("THRU_DT_DAY")==F.col("lastTHRU_DT_DAY"),1)
                                 .otherwise(0))
                    .drop("lastTHRU_DT_DAY"))

    return baseDF

def add_lastClaimSum(baseDF):

    eachDsysrtky=Window.partitionBy("DSYSRTKY")

    baseDF = (baseDF.withColumn("lastClaimSum", F.sum(F.col("lastClaim")).over(eachDsysrtky)))

    return baseDF

def add_moreThan1ClaimsPerStay(baseDF):
    # claims with same beneficiary, organization, and admission date as defined as one hospitalization stay
    eachStay = Window.partitionBy(["DSYSRTKY","ORGNPINM","ADMSN_DT"])
    baseDF = baseDF.withColumn("moreThan1ClaimsPerStay", F.when(  F.count(F.col("DSYSRTKY")).over(eachStay) > 1, 1).otherwise(0))
    return baseDF

def add_minClaimNoPerStay(baseDF):
    eachStay = Window.partitionBy(["DSYSRTKY","ORGNPINM","ADMSN_DT"])
    baseDF = baseDF.withColumn("minClaimNoPerStay", F.min(F.col("CLAIMNO")).over(eachStay))
    return baseDF

def filter_minClaimNoPerStay(baseDF):
    return add_minClaimNoPerStay(baseDF).filter(F.col("CLAIMNO")==F.col('minClaimNoPerStay'))

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

def add_provider_npi_info(baseDF, npiProviderDF):
    baseDF = (baseDF.join(npiProviderDF.select(
                          F.col("NPI"),
                          F.col("Provider Organization Name (Legal Business Name)").alias("providerName"),
                          F.col("Provider Other Organization Name").alias("providerOtherName"),
                          F.concat_ws(",",
                                              F.col("Provider First Line Business Practice Location Address"),
                                              F.col("Provider Second Line Business Practice Location Address"),
                                              F.col("Provider Business Practice Location Address City Name"),
                                              F.col("Provider Business Practice Location Address State Name"),
                                              F.col("Provider Business Practice Location Address Postal Code").substr(1,5))
                                              .alias("providerAddress"),
                          F.col("Provider Business Practice Location Address Postal Code").substr(1,5).alias("providerZip"),
                          F.col("Provider Business Practice Location Address State Name").alias("providerState")),
                      on = [F.col("ORGNPINM") == F.col("NPI")],
                      how = "left_outer")
                    .drop(F.col("NPI")))
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
                                F.col("fipscounty").alias("providerFIPS"))
                            .distinct(),
                        on="providerFIPS",
                        how="left_outer"))

    return baseDF

def add_providerRegion(baseDF):

    westCondition = '(F.col("providerStateFIPS").isin(usRegionFipsCodes["west"]))' 
    southCondition = '(F.col("providerStateFIPS").isin(usRegionFipsCodes["south"]))'
    midwestCondition = '(F.col("providerStateFIPS").isin(usRegionFipsCodes["midwest"]))'
    northeastCondition = '(F.col("providerStateFIPS").isin(usRegionFipsCodes["northeast"]))'

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

def add_provider_pos_info(baseDF, posDF):
    #I found that CBSA will duplicate some of my baseDF rows, I did not look into why because I found that posDF can be used as well
    #baseDF = baseDF.join(cbsaDF
    #                         .select(
    #                            F.lower(F.trim(F.col("countyname"))).alias("countyname"),
    #                            F.upper(F.trim(F.col("state"))).alias("state"),
    #                            F.col("fipscounty").alias("providerFips")),
    #                     on=[ (F.col("countyname")==F.col("providerCounty")) & (F.col("state")==F.col("providerState")) ],
    #                     #on=[F.col("countyname").contains(F.col("providerCounty"))], #in 1 test gave identical results as above
    #                     how="left_outer")
    #the posDF will give me ~99.9%of the providerFIPS codes
    baseDF = (baseDF.join(posDF.select( F.col("PRVDR_NUM"),F.col("providerFIPS"), F.col("providerStateFIPS"),
                                        F.col("GNRL_CNTL_TYPE_CD"), F.col("cah").alias("providerIsCah") ),
                         on=[F.col("PRVDR_NUM")==F.col("PROVIDER")],
                         how="left_outer")
                     .drop("PRVDR_NUM"))
    return baseDF

def add_providerSysId(baseDF, chspHospDF):
    baseDF = baseDF.join(chspHospDF.select(F.col("ccn").alias("PROVIDER"),
                                           F.col("health_sys_id").alias("providerSysId"),
                                           F.col("year").alias("THRU_DT_YEAR")),
                         on=["THRU_DT_YEAR","PROVIDER"],
                         how="left_outer")
    return baseDF

def add_provider_info(baseDF, npiProvidersDF, cbsaDF, posDF, ersRuccDF, maPenetrationDF, costReportDF, ahaDF, chspHospDF):

    baseDF = add_provider_npi_info(baseDF, npiProvidersDF)
    baseDF = add_provider_pos_info(baseDF, posDF)
    baseDF = add_providerRegion(baseDF)
    baseDF = add_providerCountyName(baseDF, cbsaDF)
    baseDF = add_providerRucc(baseDF, ersRuccDF)
    baseDF = add_providerMaPenetration(baseDF, maPenetrationDF)
    #right now I prefer cost report data because they seem to be about 99.5% complete, vs 80% complete for cbi
    #baseDF = add_cbi_info(baseDF, cbiDF)
    baseDF = add_provider_cost_report_info(baseDF, costReportDF)
    baseDF = add_aha_info(baseDF, ahaDF)
    baseDF = add_providerSysId(baseDF, chspHospDF)
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
    #https://svn.bmj.com/content/6/2/194
    baseDF = add_ccvPrcdr(baseDF)
    drgCodes=[23,24]
    drgCondition = '((F.col("DRG_CD").isin(drgCodes)) & (F.col("ccvPrcdr")==0) & (F.col("ishStrokeDgns")==1))'
    baseDF = baseDF.withColumn("evtDrg", F.when( eval(drgCondition), 1).otherwise(0))
    return baseDF

def add_ccvPrcdr(baseDF):
    #procedure codes for craniotomy or craniectomy
    ccPrcdrCodes = ("00J00ZZ", "00W00JZ", "00W00KZ", "0N800ZZ", "0N803ZZ", "0N804ZZ", "0NC10ZZ", "0NC13ZZ", "0NC14ZZ", "0NC30ZZ", "0NC33ZZ",
                  "0NC34ZZ", "0NC40ZZ", "0NC43ZZ", "0NC44ZZ", "0NC50ZZ", "0NC53ZZ", "0NC54ZZ", "0NC60ZZ", "0NC63ZZ", "0NC64ZZ", "0NC70ZZ",
                  "0NC73ZZ", "0NC74ZZ", "0NH00MZ", "0NH03MZ", "0NH04MZ", "0NP000Z", "0NP004Z", "0NP005Z", "0NP007Z", "0NP007Z", "0NP00KZ",
                  "0NP00SZ", "0NP030Z", "0NP034Z", "0NP037Z", "0NP03KZ", "0NP03SZ", "0NP040Z", "0NP044Z", "0NP047Z", "0NP04KZ", "0NP04SZ",
                  "0NP0X4Z", "0NP0XSZ", "0NW000Z", "0NW004Z", "0NW005Z", "0NW007Z", "0NW00JZ", "0NW00KZ", "0NW00MZ", "0NW00SZ", "0NW030Z",
                  "0NW034Z", "0NW035Z", "0NW037Z", "0NW03JZ", "0NW03KZ", "0NW03MZ", "0NW03SZ", "0NW040Z", "0NW044Z", "0NW045Z", "0NW047Z",
                  "0NW04JZ", "0NW04KZ", "0NW04MZ", "0NW04SZ", "0W9100Z", "0W010ZZ", "0W9130Z", "0W913ZZ", "0W9140Z", "0W914ZZ", "0WC10ZZ",
                  "0WC13ZZ", "0WC14ZZ", "0WH10YZ", "0WH13YZ", "0WH14YZ", "0WJ10ZZ", "0WP100Z", "0WP101Z", "0WP10JZ", "0WP10YZ", "0WP130Z", 
                  "0WP131Z", "0WP13JZ", "0WP13YZ", "0WP140Z", "0WP141Z", "0WP14JZ", "0WP14HZ", "0WW00Z", "0WW101Z", "0WW103Z", "0WW10JZ", 
                  "0WW10YZ", "0WW130Z", "0WW131Z", "0WW133Z", "0WW13JZ", "0WW13YZ", "0WW140Z", "0WW141Z", "0WW143Z", "0WW14JZ", "0WW14YZ",
                  "0N500ZZ", "0N503ZZ", "0N504ZZ", "0NB00ZZ", "0NB03ZZ", "0NB04ZZ", "0NT10ZZ", "0NT30ZZ", "0NT40ZZ", "0NT50ZZ", "0NT60ZZ",
                  "0NT70ZZ", "009100Z", "00910ZZ", "00C10ZZ", "00C13ZZ", "00C14ZZ", "009000Z", "00900ZZ", "009030Z", "00903ZZ", "009040Z",
                  "00904ZZ", "00C00ZZ", "00C03ZZ", "00C04ZZ", "00H003Z", "00H003Z", "00H00YZ", "00H032Z", "00H033Z", "00H03YZ", "00H042Z",
                  "00H043Z", "00H04YZ", "00H602Z", "00H603Z", "00H60YZ", "00H632Z", "00H633Z", "00H63YZ", "00H642Z", "00H643Z", "00H64YZ",
                  "00P000Z", "00P002Z", "00P003Z", "00P007Z", "00P00JZ", "00P00KZ", "00P00YZ", "00P030Z", "00P032Z", "00P033Z", "00P037Z",
                  "00P03JZ", "00P03KZ", "00P03YZ", "00P040Z", "00P042Z", "00P043Z", "00P047Z", "00P04JZ", "00P04KZ", "00P04YZ", "00P600Z",
                  "00P602Z", "00P603Z", "00P60YZ", "00P630Z", "00P632Z", "00P633Z", "00P63YZ", "00P640Z", "00P642Z", "00P643Z", "00P64YZ",
                  "00P6X2Z", "00W000Z", "00W002Z", "00W003Z", "00W007Z", "00W00MZ", "00W00YZ", "00W030Z", "00W032Z", "00W033Z", "00W037Z",
                  "00W03JZ", "00W03KZ", "00W03MZ", "00W03YZ", "00W040Z", "00W042Z", "00W043Z", "00W047Z", "00W04JZ", "00W04KZ", "00W04MZ",
                  "00W04YZ", "00W600Z", "00W602Z", "00W603Z", "00W60MZ", "00W60YZ", "00W630Z", "00W632Z", "00W633Z", "00W63MZ", "00W63YZ",
                  "00W640Z", "00W642Z", "00W643Z", "00W64MZ", "00W64YZ", "00B70ZZ", "00B73ZZ", "00B74ZZ", "00500ZZ", "00503ZZ", "00504ZZ",
                  "00B00ZZ", "00B03ZZ", "00B04ZZ")
    #procedure codes for ventriculostomy
    vPrcdrCodes = ("Z982", "009600Z", "009630Z", "009640Z", "001607B", "00160JB", "00160KB", "001637B", "00163JB", "00163KB", "001647B",
                   "00164JB", "00164KB", "009130Z", "00913ZZ", "009140Z", "00914ZZ", "009230Z", "00923ZZ", "009240Z", "00924ZZ", "009430Z",
                   "00943ZZ", "009440Z", "00944ZZ", "009530Z", "009540Z", "00954ZZ", "00963ZZ", "00994ZZ")  
    baseDF = (baseDF.withColumn("ccPrcdrCodes", F.expr( f"filter(prcdrCodeAll, x -> x in {ccPrcdrCodes})"))
                    .withColumn("vPrcdrCodes", F.expr( f"filter(prcdrCodeAll, x -> x in {vPrcdrCodes})"))
                    .withColumn("ccvPrcdr", F.when( (F.size(F.col("ccPrcdrCodes"))>0) | (F.size(F.col("vPrcdrCodes"))>0), 1).otherwise(0))
                    .drop("ccPrcdrCodes", "vPrcdrCodes"))
    return baseDF

def add_evtPrcdr(baseDF):
    evtPrcdrCodes=("03CG3ZZ","03CH3ZZ","03CJ3ZZ","03CK3ZZ","03CL3ZZ","03CM3ZZ","03CN3ZZ","03CP3ZZ","03CQ3ZZ")
    baseDF = (baseDF.withColumn("evtPrcdrCodes", F.expr( f"filter(prcdrCodeAll, x -> x in {evtPrcdrCodes})"))
                    .withColumn("evtPrcdr", F.when( F.size(F.col("evtPrcdrCodes"))>0,     1).otherwise(0))
                    .drop("evtPrcdrCodes"))
    #a different approach, not sure if this is slower/faster
    #evtPrcdrCodes=["03CG3ZZ","03CH3ZZ","03CJ3ZZ","03CK3ZZ","03CL3ZZ","03CM3ZZ","03CN3ZZ","03CP3ZZ","03CQ3ZZ"]
    #evtPrcdrCondition = '(' + '|'.join('(F.col(' + f'"ICD_PRCDR_CD{x}"' + ').isin(evtPrcdrCodes))' for x in range(1,26)) +')'
    #evtCondition = '( (F.col("evtDrg")==1) | (F.col("evtPrcdr")==1) )' # do NOT forget the parenthesis!!!
    #baseDF = baseDF.withColumn("evt", 
    #                           F.when(eval(evtCondition) ,1) #set them to true
    #                            .otherwise(0)) #otherwise false
    return baseDF

# main reference: https://svn.bmj.com/content/6/2/194
# the current implementation includes a difference with the main reference:
# When DRG codes 23 or 24 are present in a claim we classify that as an EVT claim but they excluded any claim with these 
# two DRG codes that had procedure codes consistent with craniectomy/craniotomy/ventriculostomy.
def add_evt(baseDF):
    # EVT takes place only in inpatient settings
    baseDF = add_evtDrg(baseDF)
    baseDF = add_evtPrcdr(baseDF)
    evtCondition = '( (F.col("evtDrg")==1) | (F.col("evtPrcdr")==1) )' # do NOT forget the parenthesis!!!
    baseDF = baseDF.withColumn("evt", F.when(eval(evtCondition) ,1).otherwise(0)) 
    return baseDF

def add_evtOsu(baseDF):
    return baseDF.withColumn("evtOsu", F.col("osu")*F.col("evt"))

def add_tpaDrg(baseDF):
    tpaDrgCodes = [61,62,63]
    tpaDrgCondition = '( (F.col("DRG_CD").isin(tpaDrgCodes)) | ( (F.col("DRG_CD")==65) & ( (F.col("tpaPrcdr")==1)|(F.col("tpaDgns")==1) ) ) )'
    baseDF = baseDF.withColumn("tpaDrg", F.when( eval(tpaDrgCondition), 1).otherwise(0))
    return baseDF

def add_tpaPrcdr(baseDF):
    tpaPrcdrCodes = ("3E03317", "3E03317")                         
    baseDF = (baseDF.withColumn("tpaPrcdrCodes", F.expr( f"filter(prcdrCodeAll, x -> x in {tpaPrcdrCodes})"))
                    .withColumn("tpaPrcdr", F.when( F.size(F.col("tpaPrcdrCodes"))>0,     1).otherwise(0))
                    .drop("tpaPrcdrCodes"))
    return baseDF

def add_tpaDgns(baseDF):
    #this diagnostic code is: Status post administration of tPA (rtPA) in a different facility within the last 24 
    #hours prior to admission to current facility, so this code should not be used to identify tpa performed at the provider of the claim (I think)
    tpaDgnsCodes = ("Z9282", "Z9282")
    baseDF = (baseDF.withColumn("tpaDgnsCodes", F.expr( f"filter(dgnsCodeAll, x -> x in {tpaDgnsCodes})")) 
                    .withColumn("tpaDgns", F.when( F.size(F.col("tpaDgnsCodes"))>0,     1).otherwise(0))
                    .drop("tpaDgnsCodes"))
    return baseDF

def add_tpaCpt(baseDF):
    #CPT codes from: https://svn.bmj.com/content/6/2/194
    tpaCptCodes = ("37195", "37201", "37202")
    eachClaim = Window.partitionBy("CLAIMNO")
    #find tpa cpt codes in claims
    baseDF = (baseDF.withColumn("hcpcsCodeAll", F.collect_set(F.col("HCPCS_CD")).over(eachClaim))
                    .withColumn("tpaCptCodes", F.expr(f"filter(hcpcsCodeAll, x -> x in {tpaCptCodes})"))
                    #if tpa cpt codes are found, then tpaCpt was performed
                    .withColumn("tpaCpt", F.when( F.size(F.col("tpaCptCodes"))>0,     1).otherwise(0))
                    .drop("tpaCptCodes"))
    return baseDF

# main reference: https://svn.bmj.com/content/6/2/194
# the current implementation includes two differences with the main reference:
# We did not use CPT codes to find tpa claims.
# We use the 4 DRG codes they used but they coupled DRG code 65 with a DGNS code indicating alteplase receipt and we did not do that.
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
        baseDF = add_tpaDgns(baseDF) #used only in inpatient
        baseDF = add_tpaDrg(baseDF)
        tpaCondition = '( (F.col("tpaDrg")==1) | (F.col("tpaPrcdr")==1) | (F.col("tpaDgns")==1) )' # do NOT forget the parenthesis!!!
    else:
        tpaCondition = '( (F.col("tpaPrcdr")==1) )'
    baseDF = baseDF.withColumn("tpa", F.when(eval(tpaCondition),1).otherwise(0))
    return baseDF

def add_dgnsCodeAll(baseDF):
    dgnsCodeColumns = [f"ICD_DGNS_CD{x}" for x in range(1,26)]
    baseDF = baseDF.withColumn("dgnsCodeAll", F.array(dgnsCodeColumns))
    return baseDF

def add_prcdrCodeAll(baseDF):
    prcdrCodeColumns = [f"ICD_PRCDR_CD{x}" for x in range(1,26)]
    baseDF = baseDF.withColumn("prcdrCodeAll", F.array(prcdrCodeColumns))
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

def add_beneficiary_info(baseDF, mbsfDF, cbsaDF, ersRuccDF, claimType="op"):

    baseDF = add_age(baseDF, mbsfDF)
    baseDF = add_death_date_info(baseDF,mbsfDF)
    baseDF = add_daysDeadAfterThroughDate(baseDF)
    baseDF = add_90DaysAfterThroughDateDead(baseDF)
    baseDF = add_365DaysAfterThroughDateDead(baseDF)
    if (claimType=="ip"):
        baseDF = add_daysDeadAfterAdmissionDate(baseDF)
        baseDF = add_90DaysAfterAdmissionDateDead(baseDF)
        baseDF = add_365DaysAfterAdmissionDateDead(baseDF)
 
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
    
    westCondition = '(F.col("fipsState").isin(usRegionFipsCodes["west"]))'
    southCondition = '(F.col("fipsState").isin(usRegionFipsCodes["south"]))'
    midwestCondition = '(F.col("fipsState").isin(usRegionFipsCodes["midwest"]))'
    northeastCondition = '(F.col("fipsState").isin(usRegionFipsCodes["northeast"]))'

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

def get_aggregate_summary(baseDF, aggItems, aggBy = ["ssaCounty"], alsoReturnItems=[]): #aggWhat must be an iterable of strings-column names

    baseDF.persist() #since I will use this in a loop make it persist in memory
    baseDF.count()

    eachUnit = Window.partitionBy(aggBy)

    baseDF = baseDF.withColumn("total", #find total in unit
                               F.count(F.col(aggBy[0])).over(eachUnit))
    returnItems = aggBy + ["total"] + alsoReturnItems

    for i in aggItems:
        baseDF = baseDF.withColumn(i+"InUnit", #add unit counts
                                   F.sum(F.col(i)).over(eachUnit))
        returnItems = returnItems + [f'{i}InUnit']

        baseDF = baseDF.withColumn(i+"InUnitPerCent", #add unit percentage
                                   F.round(100.*F.col(i+"InUnit") / F.col("total"),1))
        returnItems = returnItems + [f'{i}InUnitPerCent']

    aggregateSummary = baseDF.select(returnItems).distinct() #returnWhat cannot be tuple, list works

    aggregateSummary.persist() #since a loop was involved in calculating this, make it persist in memory
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
    baseDF = (baseDF.join(npiProvidersDF.select( F.col("NPI"), F.col(gachColName).alias("gach") ),
                         on = [baseDF["ORGNPINM"] == npiProvidersDF["NPI"]],
                         how = "inner")
                    .drop(F.col("NPI")))
    return baseDF

def add_rehabilitationFromTaxonomy(baseDF, npiProvidersDF, primary=True):
    rehabilitationColName = "rehabilitationPrimaryTaxonomy" if primary else "rehabilitationAllTaxonomy"
    baseDF = (baseDF.join(npiProvidersDF.select( F.col("NPI"), F.col(rehabilitationColName).alias("rehabilitationFromTaxonomy") ),
                         on = [baseDF["ORGNPINM"] == npiProvidersDF["NPI"]],
                         how = "left_outer")
                    .drop(F.col("NPI")))
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

def add_rehabilitation(baseDF, npiProvidersDF, primary=True):
    baseDF = add_rehabilitationFromTaxonomy(baseDF, npiProvidersDF, primary=primary)
    baseDF = add_rehabilitationFromCCN(baseDF)
    baseDF = (baseDF.withColumn("rehabilitation", F.when( (F.col("rehabilitationFromCCN")==1) | (F.col("rehabilitationFromTaxonomy")==1), 1)
                                                   .otherwise(0))
                    .drop("rehabilitationFromCCN", "rehabilitationFromTaxonomy"))
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
    baseDF = (baseDF.join(costReportDF
                           .select(
                               F.col("Provider CCN"),
                               F.col("Rural Versus Urban").alias("providerRuralVersusUrban"),
                               F.col("Number of Beds").alias("providerNumberOfBeds"),
                               F.col("Number of Interns and Residents (FTE)").alias("providerNumberOfResidents")),
                         #see note on add_cbi_info on why I am not using ORGNPINM for the join
                         #hospital cost report files include only the CMS Certification Number (Provider ID, CCN), they do not include NPI
                         on=[F.col("Provider CCN")==F.col("Provider")],
                         how="left_outer")
                    .drop("Provider CCN"))
    return baseDF

def add_transferToIn(baseDF):

    baseDF = baseDF.withColumn( "transferToIn", #transfer implies that it was a different organization
                                F.when( F.col("STUS_CD").isin([2,5]), 1) #visits that resulted in a discharge to short term hospital (code 2) or other IPT care (code 5)
                                 .otherwise(0))

    return baseDF

def add_death_date_info(baseDF,mbsfDF): #assumes that add_death_date_info has been run on mbsfDF

    baseDF = baseDF.join( mbsfDF.filter( F.col("V_DOD_SW")=="V")
                                .select( F.col("DSYSRTKY"),
                                         F.col("DEATH_DT_DAYOFYEAR"),F.col("DEATH_DT_YEAR"),F.col("DEATH_DT_DAY"), F.col("DEATH_DT") ),
                                         #F.col("RFRNC_YR")),
                          #on=[ baseDF["DSYSRTKY"]==mbsfDF["DSYSRTKY"],
                          #      F.col("THRU_DT_YEAR")==F.col("RFRNC_YR") ],
                          on="DSYSRTKY",            
                          how="left_outer") #uses null when the beneficiary does not have a valid death date in mbsfDF

    #baseDF=baseDF.drop(mbsfDF["DSYSRTKY"]).drop(mbsfDF["RFRNC_YR"]) #no longer need these

    return baseDF

def add_daysDeadAfterThroughDate(baseDF): #assumes add_through_date_info and add_death_date_info (both from mbsf.py and base.py) have been run

    baseDF = (baseDF.withColumn( "daysDeadAfterThroughDate", F.col("DEATH_DT_DAY")-F.col("THRU_DT_DAY")))
                                 
    return baseDF

def add_daysDeadAfterAdmissionDate(baseDF): #assumes add_through_date_info and add_death_date_info (both from mbsf.py and base.py) have been run
                    
    baseDF = (baseDF.withColumn( "daysDeadAfterAdmissionDate", F.col("DEATH_DT_DAY")-F.col("ADMSN_DT_DAY")))
                 
    return baseDF
            
def add_90DaysAfterThroughDateDead(baseDF): #this is the 90 day mortality flag, assumes I have run add_daysDeadAfter

    baseDF = baseDF.withColumn( "90DaysAfterThroughDateDead", F.when( F.col("daysDeadAfterThroughDate") <= 90, 1)
                                                               .otherwise(0))
    return baseDF

def add_90DaysAfterAdmissionDateDead(baseDF): #this is the 90 day mortality flag, assumes I have run add_daysDeadAfter
    
    baseDF = baseDF.withColumn( "90DaysAfterAdmissionDateDead", F.when( F.col("daysDeadAfterAdmissionDate") <= 90, 1)
                                                                 .otherwise(0))
    return baseDF

def add_365DaysAfterThroughDateDead(baseDF): #this is the 365 day mortality flag

    baseDF = baseDF.withColumn( "365DaysAfterThroughDateDead", F.when( F.col("daysDeadAfterThroughDate") <= 365, 1)
                                                                .otherwise(0))
    return baseDF

def add_365DaysAfterAdmissionDateDead(baseDF): #this is the 365 day mortality flag
                                    
    baseDF = baseDF.withColumn( "365DaysAfterAdmissionDateDead", F.when( F.col("daysDeadAfterAdmissionDate") <= 365, 1)
                                                                  .otherwise(0))
    return baseDF 

def add_los(baseDF): #length of stay = los
    #https://resdac.org/cms-data/variables/day-count-length-stay
    baseDF = baseDF.withColumn("los", F.col("THRU_DT_DAY")-F.col("ADMSN_DT_DAY")+1)
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
    #find the sequence of los days over X until Y
    baseDF = add_losDaysOverXUntilY(baseDF,X=X,Y=Y)
    
    #length of stay is then the number of those days
    baseDF = baseDF.withColumn(f"losOver{X}Until{Y}",
                              F.size(F.col(f"losDaysOver{X}Until{Y}")))
    return baseDF

def add_providerMaPenetration(baseDF, maPenetrationDF):
    #from one test with stroke inpatient claims, this was about 90% complete
    baseDF = baseDF.join(maPenetrationDF
                          .select(F.col("FIPS").alias("providerFIPS"),
                                  F.col("Penetration").alias("providerMaPenetration"),
                                  F.col("Year").alias("THRU_DT_YEAR")),
                         on=["providerFIPS","THRU_DT_YEAR"],
                         how="left_outer")
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
    baseDF = (baseDF.withColumn("nihssList", F.expr(f'filter(dgnsCodeAll, x -> x rlike "R297[0-9][0-9]?")'))
                   .withColumn("nihss", F.when( F.size(F.col("nihssList")) == 1, F.substring(F.col("nihssList")[0],5,2))
                                         .otherwise(F.lit(None)))
                   .withColumn("nihss", F.col("nihss").cast('int'))
                   .drop("dgnsList","nihssList"))
    return baseDF

def add_nihssGroup(baseDF):
    baseDF = baseDF.withColumn("nihssGroup",
                               F.when( ((F.col("nihss")>=0)&(F.col("nihss")<10)), F.lit(0) )
                                .when( ((F.col("nihss")>=10)&(F.col("nihss")<20)), F.lit(1) )
                                .when( ((F.col("nihss")>=20)&(F.col("nihss")<30)), F.lit(2) )
                                .when( ((F.col("nihss")>=30)&(F.col("nihss")<40)), F.lit(3) )
                                .when( ((F.col("nihss")>=40)&(F.col("nihss")<43)), F.lit(4) )
                                .otherwise(F.lit(None)))
    return baseDF

def add_nihss_info(baseDF):
    baseDF = add_nihss(baseDF)
    baseDF = add_nihssGroup(baseDF)
    return baseDF

def add_claim_stroke_info(baseDF, inpatient=True):
    baseDF = add_claim_stroke_treatment_info(baseDF, inpatient=inpatient)
    baseDF = add_nihss_info(baseDF)
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

#did not have time to validate extensively this function
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

def add_aamc_teaching_info(baseDF,aamcHospitalsDF):
    baseDF = (baseDF.join(aamcHospitalsDF.select(F.col("Medicare ID"),F.col("aamcTeachingStatus"), F.col("aamcMajorTeachingStatus")),
                         on=[F.col("Medicare ID")==F.col("PROVIDER")],
                         how="left_outer")
                    .drop("Medicare ID"))
    return baseDF

#did not have time to validate extensively this function
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
  
def prep_baseDF(baseDF, claim="ip"):

    #add some date-related info
    #baseDF = cast_columns_as_int(baseDF,claim=claim)
    baseDF = clean_base(baseDF, claim=claim)
    #baseDF = enforce_schema(baseDF, claim=claim)
    baseDF = add_through_date_info(baseDF,claim=claim)
    #baseDF = cast_columns_as_string(baseDF,claim=claim)

    if (claim=="ip"):
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
    elif ( claim=="op" ):
        #add SSA county of beneficiaries
        baseDF = add_ssaCounty(baseDF)
        #without a repartition, the dataframe is extremely skewed...
        #baseDF = baseDF.repartition(128, "DSYSRTKY")
    elif ( claim=="car" ):
        baseDF = add_ssaCounty(baseDF)
        baseDF = add_denied(baseDF)
 
    return baseDF

def clean_base(baseDF, claim="snf"):

    #mbsf, op, ip files were cleaned and this line was removed in them, but the rest of the files still include the first row
    #that essentially repeats the column names
    if ( (claim=="snf") | (claim=="hha") | (claim=="hosp") | (claim=="car") ):
        baseDF = baseDF.filter(~(F.col("DESY_SORT_KEY")=="DESY_SORT_KEY"))

    return baseDF

def add_strokeCenterCamargo(baseDF,strokeCentersCamargoDF):

    baseDF = baseDF.join(strokeCentersCamargoDF.select(
                                                   F.col("CCN"),F.col("strokeCenterCamargo")),
                         on=[F.col("CCN")==F.col("PROVIDER")],
                         how="left_outer")

    baseDF = baseDF.fillna(0,subset="strokeCenterCamargo")

    baseDF = baseDF.drop("CCN")

    return baseDF    

def add_numberOfResidents(baseDF, hospCostDF):

    baseDF = baseDF.join(hospCostDF
                          .select( F.col("Number of Interns and Residents (FTE)").alias("numberOfResidents"),
                                   F.col("Provider CCN")),
                         on=[F.col("Provider CCN")==F.col("PROVIDER")],
                         how="left_outer")

    baseDF = baseDF.drop("Provider CCN")

    return baseDF

def add_providerStrokeVol(baseDF, stroke="anyStroke"):
    eachProvider = Window.partitionBy(["ORGNPINM","THRU_DT_YEAR"])
    baseDF = baseDF.withColumn("providerStrokeVol", F.sum( F.col(stroke) ).over(eachProvider))
    return baseDF

def add_provider_stroke_treatment_info(baseDF, inpatient=True):
    eachProvider = Window.partitionBy(["ORGNPINM","THRU_DT_YEAR"])
    baseDF = (baseDF.withColumn("providerMeanTpa", F.mean( F.col("tpa") ).over(eachProvider))
                    .withColumn("providerTpaVol", F.sum( F.col("tpa") ).over(eachProvider)))
    if (inpatient):
        baseDF = (baseDF.withColumn("providerMeanEvt", F.mean( F.col("evt") ).over(eachProvider))
                        .withColumn("providerEvtVol", F.sum( F.col("evt") ).over(eachProvider)))
    return baseDF

def add_provider_stroke_info(baseDF, inpatient=True, stroke="anyStroke"):
    baseDF = add_provider_stroke_treatment_info(baseDF, inpatient=inpatient)
    baseDF = add_providerStrokeVol(baseDF, stroke=stroke)
    return baseDF

def add_numberOfClaims(baseDF):

    eachDsysrtky = Window.partitionBy("DSYSRTKY")

    baseDF = baseDF.withColumn("numberOfClaims",
                               F.size(F.collect_set(F.col("CLAIMNO")).over(eachDsysrtky)))

    return baseDF

def filter_beneficiaries(baseDF, mbsfDF):

    baseDF = baseDF.join(mbsfDF.select(F.col("DSYSRTKY"), F.col("RFRNC_YR").alias("THRU_DT_YEAR")), 
                         on=["DSYSRTKY","THRU_DT_YEAR"],
                         how="left_semi")

    return baseDF

def add_cAppalachiaResident(baseDF):  

    #cAppalachia: central Appalachia, Kentucky, North Carolina, Ohio, Tennessee, Virginia, West Virginia)
    cAppalachiaCond = 'F.col("STATE_CD").isin(["18","34","36","44","49","51"])'

    baseDF = baseDF.withColumn("cAppalachiaResident",
                               F.when( eval(cAppalachiaCond), 1)
                                .otherwise(0))
    return baseDF
   
def add_denied(baseDF):

    #this applies to the carrier base file, unsure if/how it can extend to other base files
    #the code appears as either "0" or "00", I think it is a 2 character code
    #https://www.cms.gov/priorities/innovation/files/x/bundled-payments-for-care-improvement-carrier-file.pdf
    deniedCond = '(F.col("PMTDNLCD").isin(["0","00"]))' 

    baseDF = baseDF.withColumn("denied",
                               F.when( eval(deniedCond),1)
                                .otherwise(0))
    return baseDF

def add_aha_info(baseDF, ahaDF): #american hospital association info
    #dictionary can be found at https://www.ahadata.com/aha-data-resources
    #annual survey file layouts includes field explanations and information about data sources
    baseDF = baseDF.join(ahaDF.select(F.col("MCRNUM").alias("PROVIDER"),  
                                      F.col("year").alias("THRU_DT_YEAR"),
                                      F.col("LAT").alias("providerAhaLat"), 
                                      F.col("LONG").alias("providerAhaLong"), 
                                      F.col("ahaCah"),                               #critical access hospital
                                      F.col("STRCHOS").alias("ahaTelestrokeHos"),    #telestroke care hospital
                                      F.col("STRCSYS").alias("ahaTelestrokeSys"),    #telestroke care health system
                                      F.col("STRCVEN").alias("ahaTelestrokeVen"),    #telestroke care joint venture
                                      F.col("ahaBeds"),                              #total facility beds - nursing home beds
                                      F.col("ahaSize"),
                                      F.col("ahaOwner"),
                                      F.col("ahaCbsaType"),
                                      F.col("ahaNisTeachingHospital"),
                                      F.col("ahaResidentToBedRatio")),
                         on=["PROVIDER","THRU_DT_YEAR"],
                         how="left_outer")
    return baseDF

#inputs: baseDF is probably an inpatient or outpatient claims DF, XDF is probably hosp, hha, snf, or ip claims, X specifies claim type
#outputs: baseDF with four additional columns, losAtX90, losDaysAtX90, losAtX365, losDaysAtX365
#note: due to the complex joins etc I prefer to add the four columns at the same time here
def add_los_at_X_info(baseDF, XDF, X="hosp"):

    baseDF = add_XDaysFromYDAY(baseDF, YDAY="ADMSN_DT_DAY", X=90)
    baseDF = add_XDaysFromYDAY(baseDF, YDAY="ADMSN_DT_DAY", X=365)

    #XDF = (XDF.select(F.col("DSYSRTKY"), F.col("ADMSN_DT_DAY"), F.col("THRU_DT_DAY") ) #need only 3 columns from this df
    #          .join(baseDF.select("DSYSRTKY"),
    #                on="DSYSRTKY",
    #                how="left_semi")) #need claims only from the beneficiaries in baseDF 

    #for every base claim, find the X claims that started after the base through date
    XDF = (XDF.join(baseDF.select(F.col("DSYSRTKY"), 
                                  F.col("CLAIMNO").alias("baseCLAIMNO"), 
                                  F.col("THRU_DT_DAY").alias("baseTHRU_DT_DAY"), 
                                  F.col("90DaysFromADMSN_DT_DAY"), 
                                  F.col("365DaysFromADMSN_DT_DAY")),
                    on="DSYSRTKY",
                    #on=[ baseDF.DSYSRTKY==XDF.DSYSRTKY,
                    #     XDF.ADMSN_DT_DAY - baseDF.baseTHRU_DT_DAY >= 0 ],
                    how="inner"))  #inner join ensures that each X claim is matched will all relevant base claims
              .filter(F.col("ADMSN_DT_DAY") - F.col("baseTHRU_DT_DAY") >= 0))        

    XDF = add_losDays(XDF) #add a sequence of days that represents length of stay

    XDF = (add_losOverXUntilY(XDF, X="baseCLAIMNO", Y="90DaysFromADMSN_DT_DAY")
           .withColumnRenamed("losOverbaseCLAIMNOUntil90DaysFromADMSN_DT_DAY", f"losAt{X}90")
           .withColumnRenamed("losDaysOverbaseCLAIMNOUntil90DaysFromADMSN_DT_DAY", f"losDaysAt{X}90"))

    XDF = (add_losOverXUntilY(XDF,X="baseCLAIMNO",Y="365DaysFromADMSN_DT_DAY")
            .withColumnRenamed("losOverbaseCLAIMNOUntil365DaysFromADMSN_DT_DAY", f"losAt{X}365")
            .withColumnRenamed("losDaysOverbaseCLAIMNOUntil365DaysFromADMSN_DT_DAY", f"losDaysAt{X}365"))

    #bring results back to base 
    baseDF = baseDF.join(XDF.select(F.col("baseCLAIMNO").alias("CLAIMNO"),
                                    F.col(f"losAt{X}90"), F.col(f"losDaysAt{X}90"),
                                    F.col(f"losAt{X}365"), F.col(f"losDaysAt{X}365"))
                            .distinct(),
                         on="CLAIMNO",
                         how="left_outer")

    # if a beneficiary does not have other X claims, put a 0
    baseDF = baseDF.fillna(0.0,subset=f"losAt{X}90").fillna(0.0,subset=f"losAt{X}365")
    #replace nulls due to left_outer join with empty arrays [] so that the concatenation will be done correctly
    baseDF = (baseDF.withColumn(f"losDaysAt{X}90", F.coalesce( F.col(f"losDaysAt{X}90"), F.array()))
                    .withColumn(f"losDaysAt{X}365",F.coalesce( F.col(f"losDaysAt{X}365"), F.array())))

    return baseDF

def add_los_total_info(baseDF):

    losDays90Columns = [c for c in baseDF.columns if re.match('^losDaysAt[a-zA-Z]+90$', c)]
    losDays365Columns = [c for c in baseDF.columns if re.match('^losDaysAt[a-zA-Z]+365$', c)]

    #baseDF = (baseDF.withColumn("losDaysTotal90", F.array_distinct( F.concat( baseDF.colRegex("`^losDaysAt[a-zA-Z]+90$`"))))
    #                .withColumn("losDaysTotal365", F.array_distinct( F.concat( baseDF.colRegex("`^losDaysAt[a-zA-Z]+365$`"))))
    baseDF = (baseDF.withColumn("losDaysTotal90", F.array_distinct( F.concat( *losDays90Columns  )))
                    .withColumn("losDaysTotal365", F.array_distinct( F.concat( *losDays365Columns )))
                    .withColumn("losTotal90", F.when( F.col("losDaysTotal90").isNull(), 0)
                                               .otherwise( F.size(F.col("losDaysTotal90"))))
                    .withColumn("losTotal365", F.when( F.col("losDaysTotal365").isNull(), 0)
                                               .otherwise( F.size(F.col("losDaysTotal365")))))
    return baseDF 

def add_days_at_home_info(baseDF, snfDF, hhaDF, hospDF, ipDF):

    baseDF = add_los_at_X_info(baseDF, snfDF, X="snf")
    baseDF = add_los_at_X_info(baseDF, hhaDF, X="hha")
    baseDF = add_los_at_X_info(baseDF, hospDF, X="hosp")
    baseDF = add_los_at_X_info(baseDF, ipDF, X="ip")

    baseDF = add_los_total_info(baseDF)

    baseDF = (baseDF.withColumn("homeDaysTotal90", F.when( F.col("STUS_CD")==20, F.lit(None))
                                                    .when( F.col("90DaysAfterAdmissionDateDead")==1, F.lit(None))
                                                    .otherwise( 90-F.col("losTotal90") ))
                    .withColumn("homeDaysTotal365", F.when( F.col("STUS_CD")==20, F.lit(None))
                                                    .when( F.col("365DaysAfterAdmissionDateDead")==1, F.lit(None))
                                                    .otherwise( 365-F.col("losTotal365") )))
    return baseDF

def add_nonPPS_info(ipClaimsDF, opBaseDF, opRevenueDF):

    #non-PPS hospitals (PPS_IND == null), eg CAH, do not need to bundle the outpatient ED visit with the inpatient stay
    #so for non-PPS hospitals I need to search in the outpatient file...
    #as a test, doing this for the PPS hospitals (PPS_IND==2) should yield exactly zero

    opBaseDF = opBaseDF.join(ipClaimsDF.filter(F.col("PPS_IND").isNull())
                              .select(F.col("ORGNPINM"), F.col("DSYSRTKY"), F.col("ADMSN_DT_DAY")),
                             on=[ipClaimsDF.ADMSN_DT_DAY == opBaseDF.THRU_DT_DAY,
                                 #ipClaimsStrokes.ADMSN_DT_DAY >= opBase.THRU_DT_DAY,
                                 #ipClaimsStrokes.ADMSN_DT_DAY <= opBase.THRU_DT_DAY+1,
                                 ipClaimsDF.DSYSRTKY==opBaseDF.DSYSRTKY, 
                                 ipClaimsDF.ORGNPINM==opBaseDF.ORGNPINM],
                             how="left_semi")

    opRevenueDF = filter_claims(opRevenueDF, opBaseDF)
    opRevenueDF = add_ed(opRevenueDF, inClaim=True)
    opRevenueDF = add_mri(opRevenueDF, inClaim=True)
    opRevenueDF = add_ct(opRevenueDF, inClaim=True)

    opRevenueDFSummary = get_revenue_summary(opRevenueDF)

    opClaimsDF = get_claimsDF(opBaseDF,opRevenueDFSummary).filter(F.col("ed")==1)

    #now bring back to the ip claims the updated information about the non-PPS hospitals
    ipClaimsDF = (ipClaimsDF.join(opClaimsDF
                                   .select(F.col("ORGNPINM"),F.col("DSYSRTKY"),
                                           F.col("THRU_DT_DAY").alias("ADMSN_DT_DAY"),
                                           F.col("ed").alias("oped"),
                                           F.col("mri").alias("opmri"),
                                           F.col("ct").alias("opct")),
                                  on=["ORGNPINM","DSYSRTKY","ADMSN_DT_DAY"],
                                  how="left_outer")
                   .fillna(0, subset=["oped","opmri","opct"])
                   .withColumn("ed", ((F.col("ed").cast("boolean"))|(F.col("oped").cast("boolean"))).cast('int'))
                   .withColumn("mri", ((F.col("mri").cast("boolean"))|(F.col("opmri").cast("boolean"))).cast('int'))
                   .withColumn("ct", ((F.col("ct").cast("boolean")) | ((F.col("opct").cast("boolean")))).cast('int'))
                   .drop("oped","opmri","opct"))

    return ipClaimsDF















