import pyspark.sql.functions as F
from pyspark.sql.window import Window
from utilities import daysInYearsPrior

#notes (most are from the Resdac tutorial: https://youtu.be/-nxGbTPVLo8?si=TsTNDXDpZlPTsvpX )
#note: demographic information is largely reliable and valid
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

def add_allPartAB(mbsfDF):
    partABCodes = ["3","C"]
    mbsfDF = (mbsfDF.withColumn("partABArray", 
                                F.array( [F.when( F.col("BUYIN" + str(x)).isin(partABCodes), 1 ).otherwise(0) for x in range(1,13)]))
                    .withColumn("partABFirstMonth", F.array_position(F.col("partABArray"), 1))
                    .withColumn("partABLastMonth", F.when( F.col("DEATH_DT_MONTH").isNull(), 12).otherwise(F.col("DEATH_DT_MONTH")))
                    .withColumn("partABArraySliced",
                                F.when( F.col("partABFirstMonth")>0, F.expr("slice(partABArray, partABFirstMonth, partABLastMonth-partABFirstMonth+1)"))
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

def add_allPartB(mbsfDF): 

    #an approach that takes into account death date and if this is the first year beneficiary first became eligible for Medicare
    notPartBCodes = ["0","1","A"]
    #an array of 12 0/1 codes eg [0,0,0,0,1,1,1,1,0,0..] 
    #1 is when BUYIN_XX code is part B code, 0 is when BUYIN_XX code is not part B code
    mbsfDF = (mbsfDF.withColumn("partBArray", 
                                F.array( [F.when( F.col("BUYIN" + str(x)).isin(notPartBCodes), 0 ).otherwise(F.lit('1')) for x in range(1,13)]))
                    .withColumn("partBFirstMonth", F.array_position(F.col("partBArray"), '1'))
                    .withColumn("partBLastMonth", F.when( F.col("DEATH_DT_MONTH").isNull(), 12).otherwise(F.col("DEATH_DT_MONTH")))
                    #number of months beneficiary should have part B coverage, depending on death (or not)
                    .withColumn("bMoCntForAllPartB", 
                                F.when(F.col("partBFirstMonth")>0, F.col("partBLastMonth")-F.col("partBFirstMonth")+1)
                                 .otherwise(F.lit(0)))
                    .withColumn("allPartB", F.when( F.col("bMoCntForAllPartB")==F.col("B_MO_CNT"), 1).otherwise(0)))

    #first approach, takes into account death date, does not take into account if this is the year beneficiary first became eligible for Medicare
    #mbsfDF = mbsfDF.withColumn("allPartB",
    #                           F.when( (F.col("DEATH_DT_MONTH")==F.col("B_MO_CNT")) | (F.col("B_MO_CNT")==12), 1)
    #                            .otherwise(0))

    #second approach, produces same results as first approach above, but slower, more flexible though since I check monthly indicators
    #buyInColumns = list(map(lambda x: f"BUYIN{x}",range(1,13))) # ['BUYIN1','BUYIN2',...'BUYIN12']
    #https://resdac.org/cms-data/variables/medicare-entitlementbuy-indicator-january
    #notPartBCodes = ("0","1","A")

    #mbsfDF = (mbsfDF.withColumn("buyInAll",   #make the array
    #                            F.array(buyInColumns))
    #                #keep all 12 elements if beneficiary did not die that year, otherwise keep up to the month of death (after that month all codes are 0)
    #                .withColumn("buyInAllSliced",   
    #                            F.when( F.col("DEATH_DT_MONTH").isNull(), F.col("buyInAll"))
    #                             #.otherwise(F.slice("buyInAll",F.lit(1),F.col("DEATH_DT_MONTH"))) #unsure why this is not working
    #                             .otherwise(F.expr("slice(buyInAll,1,DEATH_DT_MONTH)")))
    #                 #no need to keep all elements, just the distinct ones
    #                .withColumn("buyInAllSlicedDistinct",  
    #                            F.array_distinct(F.col("buyInAllSliced")))
    #                #keep from sliced array only codes that indicate not enrollment in part B
    #                #instead of filter, I could also use F.array_overlap, not sure if it would be faster though
    #                .withColumn("buyInAllSlicedDistinctNotPartB", 
    #                            F.expr(f"filter(buyInAllSlicedDistinct, x -> x in {notPartBCodes})"))
    #                #indicate who had part B and who did not
    #                .withColumn("allPartBEligible",  
    #                            F.when( F.size(F.col("buyInAllSlicedDistinctNotPartB"))>0, 0)
    #                             .otherwise(1))
    #                .drop("buyInAll","buyInAllSliced","buyInAllSlicedDistinct","buyInAllSlicedDistinctNotPartB"))

    return mbsfDF

def add_allPartA(mbsfDF): #assumes add death date info

    #first approach, takes into account death date, does not take into account if this is the year beneficiary first became eligible for Medicare
    mbsfDF = mbsfDF.withColumn("allPartA",
                               F.when( (F.col("DEATH_DT_MONTH")==F.col("A_MO_CNT")) | (F.col("A_MO_CNT")==12), 1)
                                .otherwise(0))

    return mbsfDF

def add_hmo(mbsfDF):

    # Using the HMO_COVERAGE indicator would in theory exclude beneficiaries that may have had their claims processed by CMS
    # but in practice there is evidence that CMS does not process all of these beneficiaries's claims, so
    # we need to exclude beneficiaries with codes 1 and A
    # Using either the hmoCodes list or the HMO_COVERAGE column produces the same results (the second approach is faster)
    # https://resdac.org/articles/identifying-medicare-managed-care-beneficiaries-master-beneficiary-summary-or-denominator
    # https://resdac.org/cms-data/variables/hmo-indicator

    #first approach, fastest, takes into account death date and if this is the year beneficiary became 65 or eligible for FFS
    mbsfDF = mbsfDF.withColumn("hmo",
                               F.when( F.col("HMO_MO")==0, 0)
                                .otherwise(1))

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
    dualCodes = (1,2,3,4,5,6,8)
    mbsfDF = (mbsfDF.withColumn("dualArray", F.array(["DUAL_" + str(x).zfill(2) for x in range(1,13)]))
                    .withColumn("dualArrayFiltered", F.expr(f"filter(dualArray, x->x in {dualCodes})"))
                    .withColumn("medicaidEver", F.when( F.size(F.col("dualArrayFiltered"))>0, 1).otherwise(0))
                    .drop("dualArray", "dualArrayFiltered"))
    return mbsfDF

def add_enrollment_info(mbsfDF):
    #mbsfDF = add_allPartA(mbsfDF)
    #mbsfDF = add_allPartB(mbsfDF)
    mbsfDF = add_allPartAB(mbsfDF)
    mbsfDF = add_hmo(mbsfDF)
    mbsfDF = add_ffs(mbsfDF)
    mbsfDF = add_ffsSinceJanuary(mbsfDF)
    mbsfDF = add_ffsSinceJanuaryDifference(mbsfDF)
    mbsfDF = add_continuousFfs(mbsfDF)
    mbsfDF = add_rfrncYrDifference(mbsfDF)
    mbsfDF = add_continuousYears(mbsfDF)
    mbsfDF = add_medicaidEver(mbsfDF)
    return mbsfDF

def add_ffs(mbsfDF):
    mbsfDF = mbsfDF.withColumn("ffs", F.when( (F.col("hmo")==0) & (F.col("allPartAB")==1) , 1).otherwise(0))
    return mbsfDF

def add_ffsSinceJanuary(mbsfDF):
    partABCodes = ["3","C"]
    mbsfDF = mbsfDF.withColumn("ffsSinceJanuary", F.col("ffs") * F.col("BUYIN1").isin(partABCodes).cast('int'))
    return mbsfDF

def add_continuousFfs(mbsfDF):
    eachDsysrtky=Window.partitionBy("DSYSRTKY")
    eachDsysrtkyOrdered=eachDsysrtky.orderBy("RFRNC_YR")
    mbsfDF = (mbsfDF.withColumn("ffsSinceJanuaryDifference",
                                (F.col("ffsSinceJanuary")-F.lag("ffsSinceJanuary",1).over(eachDsysrtkyOrdered))) 
                    .fillna(value=0,subset=["ffsSinceJanuaryDifference"])
                    .withColumn("continuousFfs", F.when( F.min(F.col("ffsSinceJanuaryDifference")).over(eachDsysrtky)<0, 0)
                                                  .otherwise(1)))
    return mbsfDF

def filter_FFS(mbsfDF):
    #mbsfDF = mbsfDF.filter(F.col("hmo")==0).filter(F.col("allPartA")==1).filter(F.col("allPartB")==1)
    #mbsfDF = mbsfDF.filter(F.col("hmo")==0).filter(F.col("allPartAB")==1)
    mbsfDF = mbsfDF.filter(F.col("ffs")==1)
    return mbsfDF

def add_rfrncYrDifference(mbsfDF):

    #for every beneficiary, order their year of FFS coverage
    eachDsysrtky=Window.partitionBy("DSYSRTKY")
    eachDsysrtkyOrdered=eachDsysrtky.orderBy("RFRNC_YR")

    #and then calculate the difference between two consecutive years of FFS coverage
    mbsfDF = mbsfDF.withColumn("rfrncYrDifference",
                               (F.col("RFRNC_YR")-F.lag("RFRNC_YR",1).over(eachDsysrtkyOrdered)))

    #for the first year, all difference entries will be null, replace them with 0
    mbsfDF = mbsfDF.fillna(value=0,subset=["rfrncYrDifference"])

    #each window now will keep values until the current row
    eachDsysrtkyOrderedUntilnow= eachDsysrtkyOrdered.rowsBetween(Window.unboundedPreceding,Window.currentRow)

    #if a beneficiary has a year of no coverage (difference greater than 1) then propagate that to the future
    mbsfDF = mbsfDF.withColumn("rfrncYrDifference", F.max(F.col("rfrncYrDifference")).over(eachDsysrtkyOrderedUntilnow))

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

def add_continuousYears(mbsfDF):
    eachDsysrtky=Window.partitionBy("DSYSRTKY")
    mbsfDF = mbsfDF.withColumn("continuousYears",
                               F.when( F.max(F.col("rfrncYrDifference")).over(eachDsysrtky)>1, 0)
                                .otherwise(1))
    return mbsfDF

def filter_continuous_coverage(mbsfDF):
    eachDsysrtky=Window.partitionBy("DSYSRTKY")
    #now keep only beneficiary-years with continuous coverage
    mbsfDF = mbsfDF.filter(F.col("rfrncYrDifference")<=1)
    #find the beginning of each beneficiary's FFS coverage
    mbsfDF = mbsfDF.withColumn("rfrncYrInitial", F.min(F.col("RFRNC_YR")).over(eachDsysrtky))
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

    mbsfDF = mbsfDF.withColumn("ohResident",
                               F.when(eval(ohResidencyCondition),1)
                                .otherwise(0))

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

    mbsfDF = mbsfDF.withColumn("cAppalachiaResident",
                               F.when( eval(cAppalachiaCond), 1)
                                .otherwise(0))
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
                                 .otherwise(F.lit(None))))
    return mbsfDF

def get_dead(mbsfDF): #assumes that add_death_date_info has been run on mbsfDF

    deadDF = (mbsfDF.filter(
                      F.col("V_DOD_SW")=="V")
                   .select(
                      F.col("DSYSRTKY"),F.col("DEATH_DT_DAY")))

    return deadDF

def add_ssaCounty(mbsfDF):

    mbsfDF = mbsfDF.withColumn("ssaCounty",
                               F.concat( F.col("STATE_CD"), F.col("CNTY_CD") ))
                                    #F.col("STATE_CD").substr(1,2),
                                    #F.format_string("%03d",F.col("CNTY_CD"))))

    return mbsfDF

def drop_unused_columns(mbsfDF): #mbsf is typically large and usually early on the code I no longer need these...

    dropColumns = (list(map(lambda x: "STATE_CNTY_FIPS_CD_" + f"{x}".zfill(2),range(1,13))) +
                   list(map(lambda x: "MDCR_STUS_CD_" + f"{x}".zfill(2),range(1,13))) +
                   list(map(lambda x: "DUAL_" + f"{x}".zfill(2),range(1,13))) +
                   list(map(lambda x: f"BUYIN{x}",range(1,13))) +
                   list(map(lambda x: f"HMOIND{x}",range(1,13))) +
                   ["SAMPLE_GROUP","OREC","CREC","A_TRM_CD","B_TRM_CD","A_MO_CNT","B_MO_CNT","HMO_MO","BUYIN_MO"])
    mbsfDF = mbsfDF.drop(*dropColumns)
    return mbsfDF

def filter_valid_dod(mbsfDF): 

    #deaths are always validated but death dates are not always validated
    #unvalidated death dates will typically register at end of a month and this will bias survival rate calculations
    #https://www.youtube.com/watch?v=-nxGbTPVLo8
    mbsfDF = mbsfDF.filter( (F.col("DEATH_DT").isNull()) | (F.col("V_DOD_SW")=="V") )
    return mbsfDF

def add_probablyDead(mbsfDF, ipBaseDF, opBaseDF):

    eachDSYSRTKY = Window.partitionBy("DSYSRTKY")
    mbsfDF = (mbsfDF
                 #CMS misses a few deaths so some beneficiaries included in MBSF are actually dead
                 #no standard approach on how to find these
                 #one approach: if there is no op or ip claim for someone older than 90 years old, remove them from MBSF
                 .join( ipBaseDF.select(F.col("DSYSRTKY"), F.col("THRU_DT_YEAR"))
                                .union(opBaseDF.select(F.col("DSYSRTKY"), F.col("THRU_DT_YEAR")))
                                .select(F.col("DSYSRTKY"), F.max(F.col("THRU_DT_YEAR")).over(eachDSYSRTKY).alias("lastYearWithClaim"))
                                .distinct(),
                        on = "DSYSRTKY",
                        how="left_outer")
                 .fillna(0, subset="lastYearWithClaim")
                 .withColumn("probablyDead",
                       F.when( (F.col("AGE")>90) & (F.col("RFRNC_YR")>F.col("lastYearWithClaim")), 1)
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

def add_countyName(mbsfDF,cbsaDF):

    mbsfDF = mbsfDF.join(cbsaDF.select(F.col("countyName"),F.col("ssaCounty")),
                         on = ["ssaCounty"],
                         how = "left_outer")
    return(mbsfDF)



