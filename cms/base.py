import pyspark.sql.functions as F
from pyspark.sql.window import Window

def get_admission_dates(baseDF):

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
                                 .when(F.col("ADMSN_DT_YEAR")==2017 ,366+366) #set them to 366 for leap years
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

def get_through_dates(baseDF):

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
                                 .when(F.col("THRU_DT_YEAR")==2017 ,366+366) #set them to 366 for leap years
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

def get_strokes(baseDF):

    # PRNCPAL_DGNS_CD: diagnosis, condition problem or other reason for the admission/encounter/visit to 
    # be chiefly responsible for the services, redundantly stored as ICD_DGNS_CD1
    # ADMTG_DGNS_CD: initial diagnosis at admission, may not be confirmed after evaluation, 
    # may be different than the eventual diagnosis as in ICD_DGNS_CD1-25
    # which suggests that the ICD_DGNS_CDs are after evaluation, therefore ICD_DGNS_CDs are definitely not rule-out 
    # JB: well, you can never be certain that they are not rule-out, but the principal diagnostic code for stroke has been validated

    baseDF = baseDF.withColumn("strokeClaim",
                              # ^I63[\d]: beginning of string I63 matches 0 or more digit characters 0-9
                              # I63 cerebral infraction
                              F.when((F.regexp_extract( F.col("PRNCPAL_DGNS_CD"), '^I63[\d]*',0) !=''), 1)
                               .otherwise(0)) 

    return baseDF

def get_oh_providers(baseDF):

    # keep providers in OH (PRSTATE)
    # ohio is code 36, SSA code, https://resdac.org/cms-data/variables/state-code-claim-ssa
    ohProviderCondition = '(F.col("PRSTATE")==36)'

    baseDF = baseDF.withColumn("ohProvider",
                               F.when(eval(ohProviderCondition), 1)
                                .otherwise(0))

    return baseDF
            
def get_first_claim(baseDF):

    # find the first claims for each beneficiary
    # limitation: our data start on yearStart, so we cannot really know when all beneficiaries had their first claim

    eachDsysrtky=Window.partitionBy("DSYSRTKY")

    #find day the first stroke occured
    baseDF = baseDF.withColumn("firstADMSN_DT_DAY",
                                F.min(F.col("ADMSN_DT_DAY")).over(eachDsysrtky))

    #and mark it/them (could be more than 1)
    baseDF = baseDF.withColumn("firstClaim",
                                F.when(F.col("ADMSN_DT_DAY")==F.col("firstADMSN_DT_DAY"),1)
                                 .otherwise(0))

    #a fairly small portion will have more than 1 claim on the same day, keep only those that include exactly 1 claim on the first stroke
    baseDF = (baseDF.withColumn("firstClaimSum",
                               F.sum(F.col("firstClaim")).over(eachDsysrtky))
                     .filter(
                               F.col("firstClaimSum")<=1))

    baseDF = baseDF.drop("firstClaimSum") #no longer needed

    return baseDF

def get_provider_name(baseDF, cmsProviderDF):

    baseDF = baseDF.join(
                      cmsProviderDF.select(
                          F.col("NPI"),F.col("Provider Organization Name (Legal Business Name)").alias("ProviderName")),
                      on = [F.col("ORGNPINM") == F.col("NPI")],
                      how = "inner")

    # drop the NPI column that was just added
    baseDF = baseDF.drop(F.col("NPI"))

    return baseDF

def get_osu_claim(baseDF):

    osuNpi = ["1447359997"]  # set the NPI(s) I will use for OSU

    osuCondition = '(F.col("ORGNPINM").isin(osuNpi))' # do NOT forget the parenthesis!!

    # add a column to indicate which claims were at OSU
    baseDF = baseDF.withColumn("osuClaim", 
                               F.when(eval(osuCondition) ,1) #set them to true
                                .otherwise(0)) #otherwise false

    return baseDF

def get_evt_claim(baseDF):

    # EVT takes place only in inpatient settings, EVT events are found on base claim file, not in the revenue center
    evtDrgCodes=[23,24]
    evtPrcdrCodes=["03CG3ZZ","03CH3ZZ","03CJ3ZZ","03CK3ZZ","03CL3ZZ","03CM3ZZ","03CN3ZZ","03CP3ZZ","03CQ3ZZ"]

    evtDrgCondition = '(F.col("DRG_CD").isin(evtDrgCodes))'

    evtPrcdrCondition = '(' + '|'.join('(F.col(' + f'"ICD_PRCDR_CD{x}"' + ').isin(evtPrcdrCodes))' for x in range(1,26)) +')'

    evtCondition = '(' + evtDrgCondition + '|' + evtPrcdrCondition + ')' # do NOT forget the parenthesis!!!

    # add a column to indicate which patients with stroke had an EVT
    baseDF = baseDF.withColumn("evtClaim", 
                               F.when(eval(evtCondition) ,1) #set them to true
                                .otherwise(0)) #otherwise false

    return baseDF

def get_tpa_claim(baseDF, inpatient=True):

    # tPA can take place in either outpatient or inpatient setting
    # however, in an efficient health care world, tPA would be administered at the outpatient setting
    # perhaps with the help of telemedicine and the bigger hub's guidance, and then the patient would be transferred to the bigger hospital
    # the diagnostic code from IP should be consistent with the procedure code from outpatient but check this
    # after 2015, use ICD10 exclusively, no need to check for ICD9
    # tpa can be found in DRG, DGNS and PRCDR codes

    tpaDrgCodes = [61,62,63,65]
    tpaDgnsCodes = ["Z9282"]
    tpaPrcdrCodes = ["3E03317"]

    tpaDrgCondition = '(F.col("DRG_CD").isin(tpaDrgCodes))'

    tpaPrcdrCondition = '(' + '|'.join('(F.col(' + f'"ICD_PRCDR_CD{x}"' + ').isin(tpaPrcdrCodes))' for x in range(1,26)) +')'

    tpaDgnsCondition = '(' + '|'.join('(F.col(' + f'"ICD_DGNS_CD{x}"' + ').isin(tpaDgnsCodes))' for x in range(1,26)) +')'

    if (inpatient):
        tpaCondition = '(' + tpaDrgCondition + '|' + tpaPrcdrCondition + '|' + tpaDgnsCondition + ')' # inpatient condition
    else:
        tpaCondition = tpaPrcdrCondition # outpatient condition

    baseDF = baseDF.withColumn("tpaClaim",
                               F.when(eval(tpaCondition),1) # 1 if tpa was done during visit
                                .otherwise(0))

    return baseDF

def get_beneficiary_info(baseDF,mbsfDF):

    # county codes can be an issue because MBSF includes a county code for mailing address and 12 county codes 
    # for each month, need to decide at the beginning which county code to use for each patient

    baseDF = baseDF.join( 
                         mbsfDF
                             .select(
                                F.col("DSYSRTKY"),F.col("SEX"),F.col("RACE"),F.col("AGE"),
                                F.concat(
                                    F.col("STATE_CD").substr(1,2),
                                    F.format_string("%03d",F.col("CNTY_CD"))).alias("STCNTY_CD")),
                         on = ["DSYSRTKY"],
                         how = "inner")

    return baseDF







