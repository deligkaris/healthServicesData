import pyspark.sql.functions as F
from pyspark.sql.window import Window

def add_allPartBEligible(mbsfDF): #assumes add death date info

    buyInColumns = list(map(lambda x: f"BUYIN{x}",range(1,13))) # ['BUYIN1','BUYIN2',...'BUYIN12']

    # if patients were enrolled in any of these, then they did not have Part B during the entire year
    #"0": not entitled, "1": Part A only, "A": Part A state buy-in
    #"2": Part B only, "3": Part A and Part B
    #"B": Part B state buy in, "c": Part A and Part B state buy-in
    # source: RESDAC MBSF excel file
    notPartBCodes = ("0","1","A")

    mbsfDF = (mbsfDF.withColumn("buyInAll",   #make the array
                                F.array(buyInColumns))
                    #keep all 12 elements if beneficiary did not die that year, otherwise keep up to the month of death (after that month all codes are 0)
                    .withColumn("buyInAllSliced",   
                                F.when( F.col("DEATH_DT_MONTH").isNull(), F.col("buyInAll"))
                                 #.otherwise(F.slice("buyInAll",F.lit(1),F.col("DEATH_DT_MONTH"))) #unsure why this is not working
                                 .otherwise(F.expr("slice(buyInAll,1,DEATH_DT_MONTH)")))
                     #no need to keep all elements, just the distinct ones
                    .withColumn("buyInAllSlicedDistinct",  
                                F.array_distinct(F.col("buyInAllSliced")))
                    #keep from sliced array only codes that indicate not enrollment in part B
                    .withColumn("buyInAllSlicedDistinctNotPartB", 
                                F.expr(f"filter(buyInAllSlicedDistinct, x -> x in {notPartBCodes})"))
                    #indicate who had part B and who did not
                    .withColumn("allPartBEligible",  
                                F.when( F.size(F.col("buyInAllSlicedDistinctNotPartB"))>0, 0)
                                 .otherwise(1)))

    # could be an alternative, but have not checked
    #mbsf = mbsf.withColumn("allPartBEligibleTEST", 
    #                                   F.when(F.col("B_MO_CNT")==12,1)
    #                                    .otherwise(0))

    return mbsfDF

def add_hmo(mbsfDF):

    # this approach is the only way to get what we need from MBSF
    # Using the HMO_COVERAGE indicator would exclude beneficiaries that may have had their claims processed by CMS

    # enrollment summaries
    hmoIndColumns = list(map(lambda x: f"HMOIND{x}",range(1,13))) # ['HMOIND1','HMOIND2',...'HMOIND12'] 

    #"C": Lock-in GHO to process all provider claims
    hmoCodes = ("C")

    mbsfDF = (mbsfDF.withColumn("hmoIndAll",   #make the array
                                F.array(hmoIndColumns))
                    #keep all 12 elements if beneficiary did not die that year, otherwise keep up to the month of death
                    .withColumn("hmoIndAllSliced",
                                F.when( F.col("DEATH_DT_MONTH").isNull(), F.col("hmoIndAll"))
                                 #.otherwise(F.slice("hmoIndAll",F.lit(1),F.col("DEATH_DT_MONTH"))) #unsure why this is not working
                                 .otherwise(F.expr("slice(hmoIndAll,1,DEATH_DT_MONTH)")))
                     #no need to keep all elements, just the distinct ones
                    .withColumn("hmoIndAllSlicedDistinct",
                                F.array_distinct(F.col("hmoIndAllSliced")))
                    #keep from sliced array only codes that indicate hmo
                    .withColumn("hmoIndAllSlicedDistinctHmo",
                                F.expr(f"filter(hmoIndAllSlicedDistinct, x -> x in {hmoCodes})"))
                    #indicate who had hmo and who did not
                    .withColumn("hmo",
                                F.when( F.size(F.col("hmoIndAllSlicedDistinctHmo"))>0, 1)
                                 .otherwise(0)))   

    #this would search over all 12 variables, even when beneficiaries are dead for part of the year
    #hmoIndCondition = '(' + '|'.join('F.col(' + f'"{x}"' + ').isin(yesHmoList)' for x in hmoIndList) +')'
    #mbsfDF = mbsfDF.withColumn("noHMO", 
    #                            F.when(eval(hmoIndCondition),0) #set them to false, as CMS definitely did not process their claims
    #                             .otherwise(1)) #otherwise True

    return mbsfDF

def add_enrollment_info(mbsfDF):
        
    mbsfDF = add_allPartBEligible(mbsfDF)
    mbsfDF = add_hmo(mbsfDF)

    return mbsfDF

def filter_FFS(mbsfDF):

    mbsfDF = add_enrollment_info(mbsfDF)
    mbsfDF = mbsfDF.filter(F.col("hmo")==0).filter(F.col("allPartBEligible")==1)

    return mbsfDF

def filter_continuous_coverage(mbsfDF):

    #for every beneficiary, order their year of FFS coverage
    eachDsysrtky=Window.partitionBy("DSYSRTKY")
    eachDsysrtkyOrdered=eachDsysrtky.orderBy("RFRNC_YR")

    #and then calculate the difference between two consecutive years of FFS coverage
    mbsfDF = mbsfDF.withColumn("RFRNC_YR_DIFFERENCE",
                               (F.col("RFRNC_YR")-F.lag("RFRNC_YR",1).over(eachDsysrtkyOrdered)))

    #for the first year, all difference entries will be null, replace them with 0
    mbsfDF = mbsfDF.fillna(value=0,subset=["RFRNC_YR_DIFFERENCE"])

    #each window now will keep values until the current row
    eachDsysrtkyOrderedUntilnow= eachDsysrtkyOrdered.rowsBetween(Window.unboundedPreceding,Window.currentRow)

    #if a beneficiary has a year of no coverage (difference greater than 1) then propagate that to the future
    mbsfDF = mbsfDF.withColumn("RFRNC_YR_DIFFERENCE",
                                F.max(
                                    F.col("RFRNC_YR_DIFFERENCE")).over(eachDsysrtkyOrderedUntilnow))

    #now keep only beneficiary-years with continuous coverage
    mbsfDF = mbsfDF.filter(F.col("RFRNC_YR_DIFFERENCE")<=1)

    #find the beginning of each beneficiary's FFS coverage
    mbsfDF = mbsfDF.withColumn("RFRNC_YR_INITIAL",
                                F.min(F.col("RFRNC_YR")).over(eachDsysrtky))

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

def add_ohResident(mbsfDF): #also used in base.py

    # see if there was any month where beneficiary was in OH (STATE_CNTY_CD)
    # see if they were in OH based on mailing address, Ohio is code 36
    # mbsf file is finalized in spring I think of every year, so the state code is the residence state at that moment
    # https://www.nrcs.usda.gov/wps/portal/nrcs/detail/?cid=nrcs143_013696 for FIPS codes used for the 12 additional codes
    #
    #ohResidencyCondition = \
    #    '(' + '|'.join('(F.col(' + f'"STATE_CNTY_FIPS_CD_{x:02d}"' + ').substr(1,2) == "39")' for x in range(1,13)) +')'\
    #                            + '|(F.col("STATE_CD")==36)'
    ohResidencyCondition = '(F.col("STATE_CD")==36)'

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

def cast_dates_as_int(mbsfDF): #date fields in the dataset must be interpreted as integers (and not as floats)

    columns = ["DEATH_DT"] 

    for iColumns in columns:
        mbsfDF = mbsfDF.withColumn( iColumns, F.col(iColumns).cast('int'))

    return mbsfDF

def add_death_date_info(mbsfDF):

    #leapYears=[2016,2020,2024,2028]

    mbsfDF = mbsfDF.withColumn( "DEATH_DT_DAYOFYEAR", 
                              F.when( F.col("V_DOD_SW")=="V", #for the ones that have a valid death date
                                  F.date_format(
                                     #ADMSN_DT was read as bigint, need to convert it to string that can be understood by date_format
                                     F.concat_ws('-',F.col("DEATH_DT").substr(1,4),F.col("DEATH_DT").substr(5,2),F.col("DEATH_DT").substr(7,2)), 
                                     "D" #get the day of the year
                                  ).cast('int'))
                               .otherwise(F.lit(None)))

    # keep the year too
    mbsfDF = mbsfDF.withColumn( "DEATH_DT_YEAR", 
                                F.when( F.col("V_DOD_SW")=="V", #for the ones that have a valid death date                              
                                    F.col("DEATH_DT").substr(1,4).cast('int'))
                                 .otherwise(F.lit(None)))

    # keep the month 
    mbsfDF = mbsfDF.withColumn( "DEATH_DT_MONTH",
                                F.when( F.col("V_DOD_SW")=="V", #for the ones that have a valid death date                              
                                    F.col("DEATH_DT").substr(5,2).cast('int'))
                                 .otherwise(F.lit(None)))

    # find number of days from yearStart-1 to year of death -1
    mbsfDF = mbsfDF.withColumn( "DEATH_DT_DAYSINYEARSPRIOR", 
                                F.when( F.col("V_DOD_SW")=="V", #for the ones that have a valid death date
                                    #some admissions have started in yearStart-1
                                    F.when(F.col("DEATH_DT_YEAR")==2015 ,0)  #this should be yearStart-1
                                     .when(F.col("DEATH_DT_YEAR")==2016 ,365) 
                                     .when(F.col("DEATH_DT_YEAR")==2017 ,366+365) #set them to 366 for leap years
                                     .when(F.col("DEATH_DT_YEAR")==2018 ,366+365*2)
                                     .when(F.col("DEATH_DT_YEAR")==2019 ,366+365*3)
                                     .when(F.col("DEATH_DT_YEAR")==2020 ,366+365*4)
                                     .when(F.col("DEATH_DT_YEAR")==2021 ,366*2+365*4)
                                     .when(F.col("DEATH_DT_YEAR")==2022 ,366*2+365*5)
                                     .when(F.col("DEATH_DT_YEAR")==2023 ,366*2+365*6)
                                     .otherwise(365)) #otherwise 365
                                 .otherwise(F.lit(None)))

    # assign a day number starting at day 1 of yearStart-1
    mbsfDF = mbsfDF.withColumn( "DEATH_DT_DAY",
                                F.when( F.col("V_DOD_SW")=="V", #for the ones that have a valid death date 
                                    # days in years prior to admission + days in year of admission = day nunber
                                    (F.col("DEATH_DT_DAYSINYEARSPRIOR") + F.col("DEATH_DT_DAYOFYEAR")).cast('int'))
                                 .otherwise(F.lit(None)))

    return mbsfDF

def get_dead(mbsfDF): #assumes that add_death_date_info has been run on mbsfDF

    deadDF = (mbsfDF.filter(
                      F.col("V_DOD_SW")=="V")
                   .select(
                      F.col("DSYSRTKY"),F.col("DEATH_DT_DAY")))

    return deadDF








