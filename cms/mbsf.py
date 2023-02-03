import pyspark.sql.functions as F
from pyspark.sql.window import Window

def getEnrollment(mbsfDF):
	
	buyInList = list(map(lambda x: f"BUYIN{x}",range(1,13))) # ['BUYIN1','BUYIN2',...'BUYIN12']

	# if patients were enrolled in any of these, then they did not have Part B during the entire year
	#"0": not entitled, "1": Part A only, "A": Part A state buy-in
	#"2": Part B only, "3": Part A and Part B
	#"B": Part B state buy in, "c": Part A and Part B state buy-in
	# source: RESDAC MBSF excel file
	#
	notPartBList = ["0","1","A"]

	# condition (a string for now) that allows us to see who had Part B and who did not at any time during the year
	buyInCondition = '(' + '|'.join('F.col(' + f'"{x}"' + ').isin(notPartBList)' for x in buyInList) +')'

	#add a column to indicate who had Part B and who did not at any time during the year
	mbsfDF=mbsfDF.withColumn("allPartBEligible", 
        			F.when(eval(buyInCondition),0)
        			 .otherwise(1))

	# this is doing exactly what the previous is doing
	#mbsf = mbsf.withColumn("allPartBEligibleTEST", 
	#                			F.when(F.col("B_MO_CNT")==12,1)
	#                			 .otherwise(0))

	# enrollment summaries, *allows unpacking the list
	hmoIndList = list(map(lambda x: f"HMOIND{x}",range(1,13))) # ['HMOIND1','HMOIND2',...'HMOIND12'] 

	#"C": Lock-in GHO to process all provider claims
	yesHmoList = ["C"]

	hmoIndCondition = '(' + '|'.join('F.col(' + f'"{x}"' + ').isin(yesHmoList)' for x in hmoIndList) +')'

	# add a column to indicate whether there is a chance CMS processed the claims
	# this approach is the only way to get what we need from MBSF
	# Using the HMO_COVERAGE indicator would exclude beneficiaries that may have had their claims processed by CMS
	mbsfDF = mbsfDF.withColumn("noHMO", 
            			F.when(eval(hmoIndCondition),0) #set them to false, as CMS definitely did not process their claims
            			 .otherwise(1)) #otherwise True

	return mbsfDF

def keepContinuousCoverage(mbsfDF):

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
                      			F.max(F.col("RFRNC_YR_DIFFERENCE")).over(eachDsysrtkyOrderedUntilnow))

	#now keep only beneficiary-years with continuous coverage
	mbsfDF = mbsfDF.filter(F.col("RFRNC_YR_DIFFERENCE")<=1)

	#find the beginning of each beneficiary's FFS coverage
	mbsfDF = mbsfDF.withColumn("RFRNC_YR_INITIAL",
                       			F.min(F.col("RFRNC_YR")).over(eachDsysrtky))

	return mbsfDF

def getOhResidency(mbsfDF):

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












