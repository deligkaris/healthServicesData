import pyspark.sql.functions as F
from pyspark.sql.window import Window
from cms.SCHEMAS.mbsf_schema import mbsfSchema

def add_allPartB(mbsfDF): #assumes add death date info

    #first approach, takes into account death date, does not take into account if this is the year beneficiary first became eligible for Medicare
    mbsfDF = mbsfDF.withColumn("allPartB",
                               F.when( (F.col("DEATH_DT_MONTH")==F.col("B_MO_CNT")) | (F.col("B_MO_CNT")==12), 1)
                                .otherwise(0))

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

def add_enrollment_info(mbsfDF):

    mbsfDF = add_allPartA(mbsfDF)
    mbsfDF = add_allPartB(mbsfDF)
    mbsfDF = add_hmo(mbsfDF)

    return mbsfDF

def filter_FFS(mbsfDF):

    mbsfDF = (add_enrollment_info(mbsfDF)
              .filter(F.col("hmo")==0).filter(F.col("allPartA")==1).filter(F.col("allPartB")==1))

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

    #annual residency codes are finalized on end of december of the calendar year
    #residency is determined based on mailing address for official correspondence
    #https://www.youtube.com/watch?v=-nxGbTPVLo8
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

def cast_columns_as_int(mbsfDF): #date fields in the dataset must be interpreted as integers (and not as floats)

    columns = ["DEATH_DT", "DSYSRTKY"] 

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

def add_ssaCounty(mbsfDF):

    mbsfDF = mbsfDF.withColumn("ssaCounty",
                               F.concat( F.col("STATE_CD"), F.col("CNTY_CD") ))
                                    #F.col("STATE_CD").substr(1,2),
                                    #F.format_string("%03d",F.col("CNTY_CD"))))

    return mbsfDF

def prep_mbsfDF(mbsfDF):

    mbsfDF = enforce_schema(mbsfDF)

    # DEATH_DT is currently a double, need to convert to int to be consistent with other date fields in CMS data
    #mbsfDF  = cast_columns_as_int(mbsfDF)

    # add the death date of year, year, and day in order to calculate 90 day mortality rate when needed
    mbsfDF = add_death_date_info(mbsfDF)
 
    #need to have ssa state+county code
    mbsfDF = add_ssaCounty(mbsfDF)

    #without a repartition, the dataframe is extremely skewed...
    #mbsfDF = mbsfDF.repartition(128, "DSYSRTKY")

    return mbsfDF

def enforce_schema(mbsfDF):

    #some columns need to be converted to ints first (the ones that are now double and that will be converted to strings at the end)
    stCntFipsColList = [f"STATE_CNTY_FIPS_CD_{x:02d}" for x in range(1,13)]
    castToIntColList = stCntFipsColList + ["STATE_CD"]
    mbsfDF = mbsfDF.select([F.col(c).cast('int') if c in castToIntColList else F.col(c) for c in mbsfDF.columns])

    #some columns need to be formatted independently because there may be leading 0s
    mbsfDF = (mbsfDF.withColumn("STATE_CD", 
                                F.when( F.col("STATE_CD").isNull(), F.col("STATE_CD") )
                                 .otherwise( F.format_string("%02d",F.col("STATE_CD"))))
                    .withColumn("CNTY_CD", 
                                F.when( F.col("CNTY_CD").isNull(), F.col("CNTY_CD") )
                                 .otherwise( F.format_string("%03d",F.col("CNTY_CD"))))
                    .select([ F.when( ~F.col(c).isNull(), F.format_string("%05d",F.col(c)).alias(c) ) if c in stCntFipsColList else F.col(c) for c in mbsfDF.columns ]))

    #now enforce the schema set for mbsf
    mbsfDF = mbsfDF.select([mbsfDF[field.name].cast(field.dataType) for field in mbsfSchema.fields])

    return mbsfDF

def drop_unused_columns(mbsfDF): #mbsf is typically large and usually early on the code I no longer need these...

    dropColumns = (list(map(lambda x: "STATE_CNTY_FIPS_CD_" + f"{x}".zfill(2),range(1,13))) +
                   list(map(lambda x: "MDCR_STUS_CD_" + f"{x}".zfill(2),range(1,13))) +
                   list(map(lambda x: "DUAL_" + f"{x}".zfill(2),range(1,13))) +
                   list(map(lambda x: f"BUYIN{x}",range(1,13))) +
                   list(map(lambda x: f"HMOIND{x}",range(1,13))) +
                   ["SAMPLE_GROUP"])

    mbsfDF = mbsfDF.drop(*dropColumns)

    return mbsfDF

def clean_mbsf(mbsfDF, ipBaseDF, opBaseDF):

    eachDSYSRTKY = Window.partitionBy("DSYSRTKY")

    mbsfDF = (mbsfDF
                 #deaths are always validated but death dates are not always validated
                 #unvalidated death dates will typically register at end of a month and this will bias survival rate calculations
                 #https://www.youtube.com/watch?v=-nxGbTPVLo8
                 .filter( (F.col("DEATH_DT").isNull()) | (F.col("V_DOD_SW")=="V") )
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
                        .otherwise(0))
                 .filter( F.col("probablyDead")==0 ))   

    return mbsfDF

