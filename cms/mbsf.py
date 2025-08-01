import pyspark.sql.functions as F
from pyspark.sql.window import Window
from utilities import daysInYearsPrior, monthsInYearsPrior, usRegionFipsCodes

#notes (most are from the Resdac tutorial: https://youtu.be/-nxGbTPVLo8?si=TsTNDXDpZlPTsvpX )
#note: demographic information is largely reliable and valid (age, dob, sex, race, residence, date of death)
#note: age is the oldest they could be, age at the end of the calendar year, or age as the date of death
#note: some deaths are missed by medicare (or SSA)
#      no standard way to remove the likely dead people but some options are:
#      delete anyone over 100 or 90 with no health care use in a year
#      delete anyone over 90 who has not part B coverage
#      delete anyone older than the oldest person in the US
#note: non-validated death dates are assigned at the end of the month so
#      including non-validated death dates as actual death dates will over-estimate survival times
#note: hispanic ethnicity code has a sensitivity of about 35%
#note: denominator file indicates mailing address on when file is finalized so it may reflect changes after the end of the calendar year
#      mbsf indicates mailing address as of december 31 of the calendar year
#note: data support the idea that those with A-only coverage probably have incomplete claims, even for part A services
#note: state buy-in is not the same as poverty
#note: if at some point you are interested in doing statical calculation of FFS and MA enrollments, eg to verify processes,
#      the https://www.kff.org/state-category/medicare/medicare-enrollment/ 
#      I think in this website they calculated the sum of months, divided by 12, to get the person-years numbers for enrollments
#      CMS also has data on enrollment: https://www.cms.gov/medicare/payment/medicare-advantage-rates-statistics/ffs-data-2015-2021
#note: Regarding the CMS hierarchical condition category, email from RESDAC: 
#      There are fields for the HCC in the SSP ACO RIF data, but I am not aware of any in the LDS data.
#note: The beneficiary ID is unique to an individual but also to a study, which means you cannot link CMS data from two different DUAs
#      because each individual will have two different beneficiary IDs in the two DUA datasets.
#      https://www.youtube.com/watch?v=fSYP5-1EAIs
#note: The denominator files from 2015 and earlier include at the beginning several, more than 1, rows with the same DSYSRTKY (all zeros).
#      During the Resdac workshop I asked about this and got a response via email (these files = the LDS files):
#      We do not support these files per se but from my access to the files, I can confirm that the files we have also have the same repetition 
#      of DESY_SORT_KEY.  CCW is the data provider for LDS. It may be helpful to reach out to them. ccwhelp@ccwdata.org
#note: no explicit ALS indicator, if <65yo categorized as disabled, if >65 then old age
#note: beneficiaries in each of (elderly, disabled, ESRD only) are not the same, different male proportion, annual mortality, age, top DRG for IP care

def add_allPartAB(mbsfDF):
    partABCodes = ["3","C"]
    mbsfDF = (mbsfDF.withColumn("partABArray", 
                                F.array( [F.when( F.col("BUYIN" + str(x)).isin(partABCodes), 1 ).otherwise(0) for x in range(1,13)]))
                    .withColumn("partABFirstMonthOfYear", F.array_position(F.col("partABArray"), 1).cast('int'))
                    .withColumn("partABLastMonthOfYear", F.when( (F.col("DEATH_DT_MONTH").isNull()) | (F.col("DEATH_DT_YEAR")>F.col("RFRNC_YR")) , 12)
                                                          .otherwise(F.col("DEATH_DT_MONTH")))
                    .withColumn("partABArraySliced",
                                F.when( F.col("partABFirstMonthOfYear")>0, 
                                        F.expr("slice(partABArray, partABFirstMonthOfYear, partABLastMonthOfYear-partABFirstMonthOfYear+1)"))
                                 .otherwise( F.array([F.lit(0)])))
                    .withColumn("partABArraySlicedFiltered",
                                F.expr("filter(partABArraySliced, x->x!=1)"))
                    .withColumn("allPartAB",
                                F.when( F.size(F.col("partABArraySlicedFiltered"))>0, 0).otherwise(1)))
                    #number of months beneficiary should have part AB coverage, depending on death (or not)
                    #.withColumn("abMoCntForAllPartAB", 
                    #            F.when(F.col("partABFirstMonth")>0, F.col("partABLastMonth")-F.col("partABFirstMonth")+1)
                    #             .otherwise(0))
                    #.withColumn("allPartAB", F.when( (F.col("abMoCntForAllPartAB")==F.col("B_MO_CNT")) & 
                    #                                 (F.col("abMoCntForAllPartAB")==F.col("A_MO_CNT")), 1).otherwise(0)))
    return mbsfDF

def add_hmo(mbsfDF):
    # Using the HMO_COVERAGE indicator would in theory exclude beneficiaries that may have had their claims processed by CMS
    # but in practice there is evidence that CMS does not process all of these beneficiaries's claims, so
    # we need to exclude beneficiaries with codes 1 and A
    # Using either the hmoCodes list or the HMO_COVERAGE column produces the same results (the second approach is faster)
    # https://resdac.org/articles/identifying-medicare-managed-care-beneficiaries-master-beneficiary-summary-or-denominator
    # https://resdac.org/cms-data/variables/hmo-indicator

    #first approach, fastest, takes into account death date and if this is the year beneficiary became 65 or eligible for FFS
    mbsfDF = mbsfDF.withColumn("hmo", F.when( F.col("HMO_MO")==0, 0).otherwise(1))

    #second approach, produces same results as first approach
    #hmoIndColumns = list(map(lambda x: f"HMOIND{x}",range(1,13))) # ['HMOIND1','HMOIND2',...'HMOIND12'] 
    #hmoCodes = ("1","2","A","B","C")  #these indicate not FFS

    #mbsfDF = (mbsfDF.withColumn("hmoIndAll",   #make the array
    #                            F.array(hmoIndColumns))
    #                #keep all 12 elements if beneficiary did not die that year, otherwise keep up to the month of death
    #                #HMOINDs after month of death are all 0
    #                .withColumn("hmoIndAllSliced",
    #                            F.when( F.col("DEATH_DT_MONTH").isNull(), F.col("hmoIndAll"))
    #                             #.otherwise(F.slice("hmoIndAll",F.lit(1),F.col("DEATH_DT_MONTH"))) #unsure why this is not working
    #                             .otherwise(F.expr("slice(hmoIndAll,1,DEATH_DT_MONTH)")))
    #                 #no need to keep all elements, just the distinct ones
    #                .withColumn("hmoIndAllSlicedDistinct",
    #                            F.array_distinct(F.col("hmoIndAllSliced")))
    #                #keep from sliced array only codes that indicate hmo
    #                .withColumn("hmoIndAllSlicedDistinctHmo",
    #                            F.expr(f"filter(hmoIndAllSlicedDistinct, x -> x in {hmoCodes})"))
    #                #indicate who had hmo and who did not
    #                .withColumn("hmo",
    #                            F.when( F.size(F.col("hmoIndAllSlicedDistinctHmo"))>0, 1)
    #                             .otherwise(0))
    #                .drop("hmoIndAll","hmoIndAllSliced","hmoIndAllSlicedDistinct","hmoIndAllSlicedDistinctHmo"))

    #third approach, searching over all 12 variables, even when beneficiaries are dead for part of the year
    #a death date would not probably affect the results, since after death HMOINDXX becomes 0 anyway.
    #hmoIndCondition = '(' + '|'.join('F.col(' + f'"{x}"' + ').isin(yesHmoList)' for x in hmoIndList) +')'
    #mbsfDF = mbsfDF.withColumn("noHMO", 
    #                            F.when(eval(hmoIndCondition),0) #set them to false, as CMS definitely did not process their claims
    #                             .otherwise(1)) #otherwise True

    return mbsfDF

def add_medicaidEver(mbsfDF):
    # This function determines if there is one month in the year where the beneficiary was either
    # partially or fully enrolled in Medicaid
    # Following recommendation from CCW technical guidance document on 
    # Defining Medicare-Medicaid Dually Enrolled Beneficiaries in CMS Administrative Data
    # https://www.cms.gov/medicare-medicaid-coordination/medicare-and-medicaid-coordination/medicare-medicaid-coordination-office/downloads/mmco_dualeligibledefinition.pdf
    #The monthly variable “Medicare-Medicaid Dual Eligibility” in the MBSF identifies dual status 
    #(https://resdac.org/articles/identifying-dual-eligible-medicare-beneficiaries-medicare-beneficiary-enrollment-files ).
    dualCodes = (1,2,3,4,5,6,8)
    mbsfDF = (mbsfDF.withColumn("dualArray", F.array(["DUAL_" + str(x).zfill(2) for x in range(1,13)]))
                    .withColumn("dualArrayFiltered", F.expr(f"filter(dualArray, x->x in {dualCodes})"))
                    .withColumn("medicaidEver", F.when( F.size(F.col("dualArrayFiltered"))>0, 1).otherwise(0))
                    .drop("dualArray", "dualArrayFiltered"))
    return mbsfDF

def add_enrollment_info(mbsfDF):
    mbsfDF = add_rfrncYrMonthsInYearsPrior(mbsfDF)
    mbsfDF = add_allPartAB(mbsfDF)
    mbsfDF = add_hmo(mbsfDF)
    mbsfDF = add_ffs(mbsfDF)
    mbsfDF = add_ffsFirstMonth(mbsfDF)
    mbsfDF = add_continuousFfs(mbsfDF)
    mbsfDF = add_continuousRfrncYr(mbsfDF)
    mbsfDF = add_continuousFfsAndRfrncYr(mbsfDF)
    mbsfDF = add_meanContinuousFfsAndRfrncYrForCountyYear(mbsfDF)
    mbsfDF = add_medicaidEver(mbsfDF)
    mbsfDF = add_anyEsrd(mbsfDF)
    return mbsfDF

def add_ffs(mbsfDF):
    mbsfDF = mbsfDF.withColumn("ffs", F.when( (F.col("hmo")==0) & (F.col("allPartAB")==1) , 1).otherwise(0))
    return mbsfDF

def add_ffsFirstMonthOfYear(mbsfDF):
    mbsfDF = mbsfDF.withColumn("ffsFirstMonthOfYear", F.when( F.col("ffs")==1, F.col("partABFirstMonthOfYear") ).otherwise(F.lit(None)))
    return mbsfDF

def add_rfrncYrMonthsInYearsPrior(mbsfDF):
    mbsfDF = mbsfDF.withColumn("rfrncYrMonthsInYearsPrior", monthsInYearsPrior[F.col("RFRNC_YR")])
    return mbsfDF

def add_ffsFirstMonth(mbsfDF):
    mbsfDF = add_ffsFirstMonthOfYear(mbsfDF)
    eachDsysrtky=Window.partitionBy("DSYSRTKY")
    mbsfDF = mbsfDF.withColumn("ffsFirstMonth", 
                               F.when( F.col("ffsFirstMonthOfYear").isNotNull(),
                                       F.min(F.col("ffsFirstMonthOfYear")+F.col("rfrncYrMonthsInYearsPrior")).over(eachDsysrtky))
                                .otherwise(F.lit(None)))
    return mbsfDF

def add_ffsSinceJanuary(mbsfDF):
    partABCodes = ["3","C"]
    mbsfDF = mbsfDF.withColumn("ffsSinceJanuary", F.col("ffs") * F.col("BUYIN1").isin(partABCodes).cast('int'))
    return mbsfDF

def add_continuousFfs(mbsfDF):
    mbsfDF = add_ffsSinceJanuary(mbsfDF)
    eachDsysrtky=Window.partitionBy("DSYSRTKY")
    eachDsysrtkyOrdered=eachDsysrtky.orderBy("RFRNC_YR")
    mbsfDF = (mbsfDF.withColumn("ffsSinceJanuaryDifference",
                                (F.col("ffsSinceJanuary")-F.lag("ffsSinceJanuary",1).over(eachDsysrtkyOrdered))) 
                    .fillna(value=0,subset=["ffsSinceJanuaryDifference"])
                    .withColumn("continuousFfs", F.when( (F.min(F.col("ffs")).over(eachDsysrtky)==0) | 
                                                         (F.min(F.col("ffsSinceJanuaryDifference")).over(eachDsysrtky)<0), 0)
                                                  .otherwise(1)))
    return mbsfDF

def filter_continuousFfs(mbsfDF):
    mbsfDF = mbsfDF.filter(F.col("continuousFfs")==1)
    return mbsfDF

def filter_FFS(mbsfDF):
    mbsfDF = mbsfDF.filter(F.col("ffs")==1)
    return mbsfDF

def add_rfrncYrDifference(mbsfDF):
    eachDsysrtky=Window.partitionBy("DSYSRTKY")
    eachDsysrtkyOrdered=eachDsysrtky.orderBy("RFRNC_YR")
    eachDsysrtkyOrderedUntilnow= eachDsysrtkyOrdered.rowsBetween(Window.unboundedPreceding,Window.currentRow)

    mbsfDF = (mbsfDF.withColumn("rfrncYrDifference", (F.col("RFRNC_YR")-F.lag("RFRNC_YR",1).over(eachDsysrtkyOrdered)))
                    .fillna(value=0,subset=["rfrncYrDifference"]) #for the first year, all difference entries will be null, replace them with 0
                    #if a beneficiary has a year of no coverage (difference greater than 1) then propagate that to the future
                    .withColumn("rfrncYrDifference", F.max(F.col("rfrncYrDifference")).over(eachDsysrtkyOrderedUntilnow)))

    #approach that uses a self-join 
    #find when a beneficiary, and which beneficiaries, had a gap in FFS coverage
    #yearWithBreakInFFS =   (mbsf
    #                        .select("DSYSRTKY","RFRNC_YR","RFRNC_YR_DIFFERENCE")
    #                        .filter(F.col("RFRNC_YR_DIFFERENCE")>1))
    #
    #and then keep the rows before the break in coverage took place 
    # Q: I do not know why the need to alias
    #mbsf = mbsf.alias("mbsf").join(
    #                            yearWithBreakInFFS.alias("yearWithBreakInFFS"),
    #                            on=[ F.col("mbsf.DSYSRTKY") == F.col("yearWithBreakInFFS.DSYSRTKY"), #same beneficiary
    #                                 #if there is a gap remove all rows after the long gap
    #                                 F.col("mbsf.RFRNC_YR") >= F.col("yearWithBreakInFFS.RFRNC_YR")],
    #                            how="left_anti")
    return mbsfDF

def add_continuousRfrncYr(mbsfDF):
    mbsfDF = add_rfrncYrDifference(mbsfDF)
    eachDsysrtky=Window.partitionBy("DSYSRTKY")
    mbsfDF = mbsfDF.withColumn("continuousRfrncYr",
                               F.when( F.max(F.col("rfrncYrDifference")).over(eachDsysrtky)>1, 0)
                                .otherwise(1))
    return mbsfDF

def add_continuousFfsAndRfrncYr(mbsfDF):
    mbsfDF = mbsfDF.withColumn("continuousFfsAndRfrncYr", F.col("ffs")*F.col("continuousRfrncYr")*F.col("continuousFfs"))
    return mbsfDF

def filter_continuousRfrncYr(mbsfDF):
    mbsfDF = mbsfDF.filter(F.col("continuousRfrncYr")==1)
    return mbsfDF

def filter_continuous_coverage(mbsfDF):
    eachDsysrtky=Window.partitionBy("DSYSRTKY")
    #now keep only beneficiary-years with continuous coverage
    mbsfDF = mbsfDF.filter(F.col("rfrncYrDifference")<=1)
    #find the beginning of each beneficiary's FFS coverage
    mbsfDF = mbsfDF.withColumn("rfrncYrInitial", F.min(F.col("RFRNC_YR")).over(eachDsysrtky))
    return mbsfDF

def add_anyEsrd(mbsfDF):
    #Resdac note ESRD:
    #There are three variables you could use to identify ESRD beneficiaries in the MBSF Base file: End-Stage Renal Disease (ESRD) Indicator ESRD_IND, 
    #Current Reason for Entitlement Code ENTLMT_RSN_CURR, and Medicare Status Code January-December MDCR_STATUS_CODE_01-12.
    #Based on some simple cross-tabulations in the 2019 MBSF, these three variables do not map perfectly to one another. 
    #For example, there are people with ESRD_IND = Y who have MDCR_STATUS_CODE_01-12 indicating that they do not have ESRD. 
    #We do expect some discrepancy between these variables because while the variables that we receive come from the same general source, 
    #they come from different tables, which can cause a mismatch. 
    #CCW does not have a recommendation for which is more reliable. 
    #The only other thing to bear in mind is that the ESRD indicator is associated with ESRD benefit use and may differ from the 
    #total Medicare population diagnosed with ESRD.
    #My note:
    #Using one of those 3 variables at a time on the 2017 FFS portion of the data, and multiplying the counts by 100/67 (there were 67% enrollment to FFS 
    #in 2017), always leads me to a prevalence rate that seems to be lower than the 760,000 that I see on the USRDS website. 
    #https://usrds-adr.niddk.nih.gov/2022/end-stage-renal-disease/1-incidence-prevalence-patient-characteristics-and-treatment-modalities
    #Only when I used OR logic for all those 3 variables, the pseudocode is 
    #ESRD_IND==Y OR CREC in (2,3) OR OREC in (2,3) OR MDCR_STUS_CD in (11,21,31) (any of the 12)
    #I got to a number of beneficiaries that when multiplied by 100/67 is fairly close to the 760,000 that I see as the estimated ESRD 
    #prevalence rate for 2017.
    #Another Resdac note:
    #I do know that the USRDS data is the ‘gold standard' for studying ESRD, so if your project focuses on that population you might 
    #look into requesting that data.
    #https://resdac.org/articles/data-resources-studying-end-stage-renal-disease-esrd
    #I heard back from our analyst, and she was also slightly skeptical of the statement, “excluding Medicare managed care and end stage 
    #renal disease beneficiaries due to incomplete claims data in these groups”, especially because the study population sighted was an over 65 population.
    #Studying ESRD in a younger population could result in incomplete claims due to enrollment limitations, plus the entitlement ends 36 months after 
    #transplant. Another potential limitation: perhaps the authors were concerned about the impact of the ESRD PPS bundling, but we do not believe 
    #this would impact the ability to see hospital transfers.
    #Other than that, we’re really not aware of any reason why the claims would be incomplete for a population with ESRD over age 65. The gold standard 
    #data source, USRDS, combines CMS administrative claims data as part of their data set.
    #We believe the FFS data for benes with ESRD to be representative.

    mbsfDF = (mbsfDF.withColumn("mdcrStusArray", F.array(["MDCR_STUS_CD_" + str(x).zfill(2) for x in range(1,13)]))
                    .withColumn("anyEsrdInMdcrStus", 
                                F.when( F.size( F.expr(f"filter(mdcrStusArray, x -> x in (11,21,31))") )>0, 1).otherwise(0))
                    .withColumn("anyEsrd",
                                F.when( (F.col("ESRD_IND")=="Y") | 
                                        (F.col("OREC").isin([2,3])) | (F.col("CREC").isin([2,3])) | 
                                        (F.col("anyEsrdInMdcrStus")==1), 1)
                                 .otherwise(0)))
    return mbsfDF

def add_ohResident(mbsfDF): #also used in base.py
    # see if there was any month where beneficiary was in OH (STATE_CNTY_CD)
    # see if they were in OH based on mailing address, Ohio is code 36
    # mbsf file is finalized in spring I think of every year, so the state code is the residence state at that moment
    # https://www.nrcs.usda.gov/wps/portal/nrcs/detail/?cid=nrcs143_013696 for FIPS codes used for the 12 additional codes
    #
    #ohResidencyCondition = \
    #    '(' + '|'.join('(F.col(' + f'"STATE_CNTY_FIPS_CD_{x:02d}"' + ').substr(1,2) == "39")' for x in range(1,13)) +')'\
    #                            + '|(F.col("STATE_CD")==36)'

    #annual residency codes are finalized on end of december of the calendar year
    #residency is determined based on mailing address for official correspondence
    #https://www.youtube.com/watch?v=-nxGbTPVLo8
    ohResidencyCondition = '(F.col("STATE_CD")=="36")'

    # keep mbsf data for Ohio residents only
    #mbsf = mbsf.filter(eval(ohResidencyCondition))

    mbsfDF = mbsfDF.withColumn("ohResident", F.when(eval(ohResidencyCondition),1).otherwise(0))

    #add a column with a list of years where beneficiaries were in FFS and in Ohio (just in case I need it)
    #mbsfDF = mbsfDF.join(mbsfDF
    #                 .groupBy("DSYSRTKY")
    #                 .agg(F.array_sort(F.collect_list("RFRNC_YR")).alias("yearsInFFS")) #create sorted list of years
    #                 .select(F.col("DSYSRTKY"),F.col("yearsInFFS")),
    #                 on="DSYSRTKY",
    #                 how="inner")

    return mbsfDF

def add_cAppalachiaResident(mbsfDF):  
    #cAppalachia: central Appalachia, Kentucky, North Carolina, Ohio, Tennessee, Virginia, West Virginia)
    cAppalachiaCond = 'F.col("STATE_CD").isin(["18","34","36","44","49","51"])'

    mbsfDF = mbsfDF.withColumn("cAppalachiaResident", F.when( eval(cAppalachiaCond), 1).otherwise(0))
    return mbsfDF    

def add_death_date_info(mbsfDF):
    mbsfDF = (mbsfDF.withColumn("DEATH_DT_DAYOFYEAR", 
                                F.when( F.col("V_DOD_SW")=="V", #for the ones that have a valid death date
                                  F.date_format(
                                     #ADMSN_DT was read as bigint, need to convert it to string that can be understood by date_format
                                     F.concat_ws('-',F.col("DEATH_DT").substr(1,4),F.col("DEATH_DT").substr(5,2),F.col("DEATH_DT").substr(7,2)), 
                                     "D" #get the day of the year
                                  ).cast('int'))
                               .otherwise(F.lit(None)))
                    # keep the year too
                    .withColumn("DEATH_DT_YEAR", 
                                F.when( F.col("V_DOD_SW")=="V", #for the ones that have a valid death date                              
                                    F.col("DEATH_DT").substr(1,4).cast('int'))
                                 .otherwise(F.lit(None)))
                    # keep the month 
                    .withColumn("DEATH_DT_MONTH",
                                F.when( F.col("V_DOD_SW")=="V", #for the ones that have a valid death date                              
                                    F.col("DEATH_DT").substr(5,2).cast('int'))
                                 .otherwise(F.lit(None)))
                    # find number of days from yearStart-1 to year of death -1
                    .withColumn("DEATH_DT_DAYSINYEARSPRIOR", 
                                F.when( F.col("V_DOD_SW")=="V", daysInYearsPrior[F.col("DEATH_DT_YEAR")])  #for the ones that have a valid death date
                                 .otherwise(F.lit(None)))
                    # assign a day number starting at day 1 of yearStart-1
                    .withColumn("DEATH_DT_DAY",
                                F.when( F.col("V_DOD_SW")=="V", #for the ones that have a valid death date 
                                    # days in years prior to admission + days in year of admission = day nunber
                                    (F.col("DEATH_DT_DAYSINYEARSPRIOR") + F.col("DEATH_DT_DAYOFYEAR")).cast('int'))
                                 .otherwise(F.lit(None)))
                    .drop("DEATH_DT_DAYSINYEARSPRIOR", "DEATH_DT_DAYOFYEAR")) 
    return mbsfDF

def get_dead(mbsfDF): #assumes that add_death_date_info has been run on mbsfDF
    deadDF = (mbsfDF.filter( F.col("V_DOD_SW")=="V" ).select(F.col("DSYSRTKY"),F.col("DEATH_DT_DAY")))
    return deadDF

def add_ssaCounty(mbsfDF):
    mbsfDF = mbsfDF.withColumn("ssaCounty", F.concat( F.col("STATE_CD"), F.col("CNTY_CD") ))
                                    #F.col("STATE_CD").substr(1,2),
                                    #F.format_string("%03d",F.col("CNTY_CD"))))
    return mbsfDF

def drop_unused_columns(mbsfDF): #mbsf is typically large and usually early on the code I no longer need these...
    dropColumns = (list(map(lambda x: "STATE_CNTY_FIPS_CD_" + f"{x}".zfill(2),range(1,13))) +
                   list(map(lambda x: "MDCR_STUS_CD_" + f"{x}".zfill(2),range(1,13))) +
                   list(map(lambda x: "DUAL_" + f"{x}".zfill(2),range(1,13))) +
                   list(map(lambda x: f"BUYIN{x}",range(1,13))) +
                   list(map(lambda x: f"HMOIND{x}",range(1,13))) +
                   ["SAMPLE_GROUP","OREC","CREC","A_TRM_CD","B_TRM_CD","A_MO_CNT","B_MO_CNT","HMO_MO","BUYIN_MO","ESRD_IND", 
                    "lastYearWithClaim", "probablyDead",
                    "anyEsrdInMdcrStus", "mdcrStusArray", "rfrncYrDifference", "ffsSinceJanuaryDifference", "ffsSinceJanuary",
                    "ffsFirstMonthOfYear", "allPartAB", "hmo", "ffs", "partABArraySlicedFiltered", "partABArraySliced",
                    "partABLastMonthOfYear", "partABFirstMonthOfYear", "partABArray", "rfrncYrMonthsInYearsPrior"])
    mbsfDF = mbsfDF.drop(*dropColumns)
    return mbsfDF

def filter_valid_dod(mbsfDF): 
    #deaths are always validated but death dates are not always validated
    #unvalidated death dates will typically register at end of a month and this will bias survival rate calculations
    #https://www.youtube.com/watch?v=-nxGbTPVLo8
    mbsfDF = mbsfDF.filter( (F.col("DEATH_DT").isNull()) | (F.col("V_DOD_SW")=="V") )
    return mbsfDF

def add_willDie(mbsfDF):
    eachDSYSRTKY = Window.partitionBy("DSYSRTKY")
    mbsfDF = mbsfDF.withColumn("willDie", F.max( (~F.col("DEATH_DT").isNull()).cast('int')).over(eachDSYSRTKY))
    return mbsfDF

def add_probablyDead(mbsfDF, ipBaseDF, opBaseDF, snfBaseDF, hospBaseDF, hhaBaseDF):
    eachDSYSRTKY = Window.partitionBy("DSYSRTKY")
    mbsfDF = (mbsfDF
                 #CMS misses a few deaths so some beneficiaries included in MBSF are actually dead
                 #no standard approach on how to find these
                 #one approach: if there is no op or ip claim for someone older than 90 years old, remove them from MBSF
                 .join( ipBaseDF.select(F.col("DSYSRTKY"), F.col("THRU_DT_YEAR"))
                                .union(opBaseDF.select(F.col("DSYSRTKY"), F.col("THRU_DT_YEAR")))
                                .union(snfBaseDF.select(F.col("DSYSRTKY"), F.col("THRU_DT_YEAR")))
                                .union((hospBaseDF.select(F.col("DSYSRTKY"), F.col("THRU_DT_YEAR"))))
                                .union(hhaBaseDF.select(F.col("DSYSRTKY"), F.col("THRU_DT_YEAR")))
                                .select(F.col("DSYSRTKY"), F.max(F.col("THRU_DT_YEAR")).over(eachDSYSRTKY).alias("lastYearWithClaim"))
                                .distinct(),
                        on = "DSYSRTKY",
                        how="left_outer")
                 .fillna(0, subset="lastYearWithClaim")
                 .withColumn("probablyDead",
                       F.when( (F.col("AGE")>90) & (F.col("RFRNC_YR")>F.col("lastYearWithClaim")) & (F.col("willDie")==0), 1)
                        .otherwise(0)))
    return mbsfDF

def filter_probably_dead(mbsfDF):
    mbsfDF = mbsfDF.filter( F.col("probablyDead")==0 )   
    return mbsfDF

def add_residentsInCounty(mbsfDF):
    eachCounty = Window.partitionBy(["ssaCounty","RFRNC_YR"])
    mbsfDF = mbsfDF.withColumn("residentsInCounty", F.count(F.col("DSYSRTKY")).over(eachCounty))
    return mbsfDF

def add_fipsCounty(mbsfDF, cbsaDF):
    mbsfDF = mbsfDF.join(cbsaDF.select(F.col("ssaCounty"),F.col("fipsCounty")),
                         on=["ssaCounty"],
                         how="left_outer")
    return mbsfDF

def add_fipsState(mbsfDF):
    mbsfDF = mbsfDF.withColumn("fipsState", F.col("fipsCounty").substr(1,2))
    return mbsfDF

def add_countyName(mbsfDF,cbsaDF):
    mbsfDF = mbsfDF.join(cbsaDF.select(F.col("countyName"),F.col("ssaCounty")),
                         on = ["ssaCounty"],
                         how = "left_outer")
    return(mbsfDF)

def add_sdoh_info(mbsfDF, sdohDF):
    #I do not have all this information for all years so I am excluding them for now
    mbsfDF = mbsfDF.join(sdohDF.select(F.col("ACS_MEDIAN_HH_INC").alias("medianHhIncome"), #median household income
                                       F.col("medianHhIncomeAdjusted"),
                                       #F.col("AHRF_TOT_NEUROLOGICAL_SURG").alias("totalNeuroSurgeons"), #total number of neurological surgeons
                                       #F.col("CDCA_HEART_DTH_RATE_ABOVE35").alias("cvDeathsRate"), #total cv disease deaths per 100k population
                                       #F.col("CDCA_PREV_DTH_RATE_BELOW74").alias("preventableCvDeathRate"), #total avoidable heart disease and stroke deaths per 100k population ages 74 and below
                                       #F.col("CDCA_STROKE_DTH_RATE_ABOVE35").alias("strokeDeathRate"), #total stroke deaths per 100k population ages 35 and over
                                       F.col("COUNTYFIPS").alias("fipsCounty"),
                                       #F.col("HIFLD_MEDIAN_DIST_UC").alias("medianDistanceUc"), #median distance in miles to the nearest UC, using population weighted tract centroids in the county
                                       F.col("POS_MEDIAN_DIST_ED").alias("medianDistanceEd"), #median distance in miles to the nearest ED, using population weighted tract centroids in the county
                                       F.col("POS_MEDIAN_DIST_MEDSURG_ICU").alias("medianDistanceIcu"), #median distance in miles to the nearest medical-surgical ICU, using population weighted tract centroids in the county
                                       F.col("POS_MEDIAN_DIST_TRAUMA").alias("medianDistanceTrauma"), #median distance in miles to the nearest trauma center calculated using population weighted tract centroids in the county
                                       F.col("YEAR").alias("RFRNC_YR")),
                         on = ["fipsCounty", "RFRNC_YR"],
                         how = "left_outer")
    return mbsfDF 

def add_maPenetration(mbsfDF, maPenetrationDF):
    mbsfDF = mbsfDF.join(maPenetrationDF
                          .select(F.col("FIPS").alias("fipsCounty"),
                                  F.col("Penetration").alias("maPenetration"),
                                  F.col("Year").alias("RFRNC_YR")),
                         on=["fipsCounty","RFRNC_YR"],
                         how="left_outer")
    return mbsfDF

def add_meanContinuousFfsAndRfrncYrForCountyYear(mbsfDF):
    eachCountyYear = Window.partitionBy(["fipsCounty","RFRNC_YR"])
    mbsfDF = mbsfDF.withColumn("meanContinuousFfsAndRfrncYrForCountyYear", F.mean(F.col("continuousFfsAndRfrncYr")).over(eachCountyYear))
    return mbsfDF

def add_rucc(mbsfDF, ersRuccDF):
    mbsfDF = mbsfDF.join(ersRuccDF.select(F.col("FIPS").alias("fipsCounty"),F.col("RUCC_2013").alias("rucc")),
                          on="fipsCounty",
                          how="left_outer")
    return mbsfDF

def add_region(mbsfDF): 
    
    westCondition = '(F.col("fipsState").isin(usRegionFipsCodes["west"]))'
    southCondition = '(F.col("fipsState").isin(usRegionFipsCodes["south"]))'
    midwestCondition = '(F.col("fipsState").isin(usRegionFipsCodes["midwest"]))'
    northeastCondition = '(F.col("fipsState").isin(usRegionFipsCodes["northeast"]))'

    mbsfDF = mbsfDF.withColumn("region",
                               F.when( eval(westCondition), 4)
                                .when( eval(southCondition), 3)
                                .when( eval(midwestCondition), 2)
                                .when( eval(northeastCondition), 1)
                                .otherwise(F.lit(None)))
    return mbsfDF

def add_census_info(mbsfDF, censusDF):
    mbsfDF = mbsfDF.join(censusDF.select(F.col("fipsCounty"), F.col("year").alias("RFRNC_YR"), F.col("totalPopulation"), F.col("medianHouseholdIncome")),
                         on = ["fipsCounty", "RFRNC_YR"], 
                         how = "left_outer")
    return mbsfDF

def prep_mbsf(mbsfDF):
    '''I noticed that in 2015 the same beneficiary appeared more than once, I did not notice that in 2016 and 2017.
    I am keeping only one row for each beneficiary because any join with base claims will multiply the claims that are associated with the
    beneficiaries that appear in more than one mbsf row.'''
    eachDsysrtkyYear = Window.partitionBy(["DSYSRTKY","RFRNC_YR"]).orderBy("DSYSRTKY")
    mbsfDF = mbsfDF.withColumn("nRow", F.row_number().over(eachDsysrtkyYear)).filter(F.col("nRow")==1).drop("nRow") #just make a choice
    return mbsfDF

def add_beneficiary_info(mbsfDF, dataDICT):
    mbsfDF = add_death_date_info(mbsfDF)
    mbsfDF = add_ssaCounty(mbsfDF)
    mbsfDF = add_fipsCounty(mbsfDF, dataDICT["cbsa"])
    mbsfDF = add_fipsState(mbsfDF)
    mbsfDF = add_region(mbsfDF)
    mbsfDF = add_rucc(mbsfDF, dataDICT["ersRucc"])
    mbsfDF = add_enrollment_info(mbsfDF)
    mbsfDF = add_willDie(mbsfDF)
    mbsfDF = add_sdoh_info(mbsfDF, dataDICT["sdoh"])
    mbsfDF = add_maPenetration(mbsfDF, dataDICT["maPenetration"])
    mbsfDF = add_census_info(mbsfDF, dataDICT["census"])
    return mbsfDF








