import pyspark.sql.functions as F
from pyspark.sql.window import Window

def getAdmissionDates(baseDF):

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

def getThroughDates(baseDF):

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


