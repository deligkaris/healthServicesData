import pyspark.sql.functions as F
from pyspark.sql.window import Window
import cms.base as baseF

def get_closest_ip_claim(opIpDF):
    eachOpClaim=Window.partitionBy("opTHRU_DT_DAY", "opCLAIMNO")
    opIpDF = opIpDF.withColumn("isClosestIpClaim", (F.col("ipADMSN_DT_DAY")==F.min(F.col("ipADMSN_DT_DAY")).over(eachOpClaim)).cast('int'))
    return opIpDF.filter(F.col("isClosestIpClaim")==1).drop("isClosestIpClaim")

def get_closest_op_claim(opIpDF):
    eachIpClaim=Window.partitionBy("ipADMSN_DT_DAY", "ipCLAIMNO")
    opIpDF = opIpDF.withColumn("isClosestOpClaim", (F.col("opTHRU_DT_DAY")==F.min(F.col("opTHRU_DT_DAY")).over(eachIpClaim)).cast('int'))
    return opIpDF.filter(F.col("isClosestOpClaim")==1).drop("isClosestOpClaim")

def remove_uncertain_transfers(opIpDF):
    '''Some ip claims are associated with more than one op claim because there are more than one op claims that meet all
    other transfer criteria. Since we do not have hourly chronological information we cannot distinguish which op claim
    was the last one (to use for the transfer), and we will remove those. There will not be a transfer associated with these cases.
    The same is true the other way around (more than one ip claims for one op claim).
    Based on the cases I encountered those are very very few.'''
    eachIpClaim=Window.partitionBy("ipADMSN_DT_DAY", "ipCLAIMNO")
    eachOpClaim=Window.partitionBy("opTHRU_DT_DAY", "opCLAIMNO")
    opIpDF = (opIpDF.withColumn("numberOfIpClaimsForOpClaim", F.count(F.col("ipCLAIMNO")).over(eachOpClaim))
                    .filter(F.col("numberOfIpClaimsForOpClaim")==1).drop("numberOfIpClaimsForOpClaim")
                    .withColumn("numberOfOpClaimsForIpClaim", F.count(F.col("opCLAIMNO")).over(eachIpClaim))
                    .filter(F.col("numberOfOpClaimsForIpClaim")==1).drop("numberOfOpClaimsForIpClaim"))
    return opIpDF

def get_clean_transfers(opIpDF):
    opIpDF = get_closest_ip_claim(opIpDF)
    opIpDF = get_closest_op_claim(opIpDF)
    opIpDF = remove_uncertain_transfers(opIpDF)
    return opIpDF

def get_transfers(opClaimsDF, ipClaimsDF):
    # rename the dataframe columns because pandas cannot handle two columns with the same name, just in case pd is used in analysis 
    ipClaimsDF = ipClaimsDF.toDF(*("ip"+c for c in ipClaimsDF.columns))
    opClaimsDF = opClaimsDF.toDF(*("op"+c for c in opClaimsDF.columns))
    opIpDF = opClaimsDF.join(ipClaimsDF,    
                             on = [(F.col("opDSYSRTKY")==F.col("ipDSYSRTKY")) &
                                   (F.col("opTHRU_DT_DAY")<=F.col("ipADMSN_DT_DAY")) &
                                   (F.col("opTHRU_DT_DAY")>=F.col("ipADMSN_DT_DAY")-1) &
                                   (F.col("opORGNPINM") != F.col("ipORGNPINM"))],
                             how = "inner") #drop any claims that do not match
    opIpDF = get_clean_transfers(opIpDF)
    return opIpDF

def add_transfertpa(opIpDF):
    return opIpDF.withColumn("transfertpa", F.when( (F.col("iptpa")==1) | (F.col("optpa")==1), 1).otherwise(0))

def add_transfernihss(opIpDF):
    return opIpDF.withColumn("transfernihss", F.when( F.col("opnihss").isNull(), F.col("ipnihss")).otherwise(F.col("opnihss")))

def add_transfernihssGroup(opIpDF):
    return opIpDF.withColumn("transfernihssGroup", F.when( F.col("opnihssGroup").isNull(), F.col("ipnihssGroup")).otherwise(F.col("opnihssGroup")))   

def add_stroke_info(opIpDF):
    opIpDF = add_transfertpa(opIpDF)
    opIpDF = add_transfernihss(opIpDF)
    opIpDF = add_transfernihssGroup(opIpDF)
    return opIpDF

def add_firstTransfer(transferDF):
    '''Assumes that get_clean_transfers has been run on the transferDF.'''
    eachDsysrtky=Window.partitionBy("opDSYSRTKY")
    transferDF = transferDF.withColumn("firstTransfer", (F.col("opTHRU_DT_DAY") == F.min(F.col("opTHRU_DT_DAY")).over(eachDsysrtky)).cast('int') )
    return transferDF

def add_provider_transfer_volume_info(transferDF):
    eachOpProvider = Window.partitionBy(["opORGNPINM","opTHRU_DT_YEAR"])
    eachIpProvider = Window.partitionBy(["ipORGNPINM","ipTHRU_DT_YEAR"])
    transferDF = (transferDF.withColumn("providerTransferOutVol", F.count( F.col("opCLAIMNO") ).over(eachOpProvider))
                            .withColumn("providerTransferInVol", F.count( F.col("ipCLAIMNO") ).over(eachIpProvider)))
    return transferDF

def add_prior_hospitalization_info(transferDF, ipBaseDF):
    transferDF = baseF.add_prior_hospitalization_info(
                          transferDF.withColumnRenamed("ipDSYSRTKY", "DSYSRTKY")
                                    .withColumnRenamed("ipADMSN_DT_DAY", "ADMSN_DT_DAY")
                                    .withColumnRenamed("ipADMSN_DT_MONTH", "ADMSN_DT_MONTH")
                                    .withColumnRenamed("ipCLAIMNO", "CLAIMNO"), 
                          ipBaseDF)
    return transferDF

def add_days_at_home_info(transferDF, snfBaseDF, hhaBaseDF, hospBaseDF, ipBaseDF):
    columnsInitial = transferDF.columns
    columnsTemp = [re.sub("^ip","",c) for c in columnsInitial] #need to rename transfers df because days_at_home needs standard column names
    transferDF = transferDF.toDF(*columnsTemp)
    transferDF = baseF.add_days_at_home_info(transferDF, snfBaseDF, hhaBaseDF, hospBaseDF, ipBaseDF)
    revertColumnRenaming = dict(zip(columnsTemp,columnsInitial)) #and now change the column names back to their initial ones
    columnsAll = transferDF.columns
    columnsFinal = columnsAll
    for i in range(len(columnsAll)):
        c = columnsAll[i]
        if c in columnsTemp:
            columnsFinal[i] = revertColumnRenaming[c]     
    transferDF = transferDF.toDF(*columnsFinal)
    return transferDF

