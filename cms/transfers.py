import re
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import cms.base as baseF

def get_closest_to_claim(transfersDF):
    eachFromClaim=Window.partitionBy("fromTHRU_DT_DAY", "fromCLAIMNO")
    transfersDF = transfersDF.withColumn("isClosestToClaim", (F.col("toADMSN_DT_DAY")==F.min(F.col("toADMSN_DT_DAY")).over(eachFromClaim)).cast('int'))
    return transfersDF.filter(F.col("isClosestToClaim")==1).drop("isClosestToClaim")

def get_closest_from_claim(transfersDF):
    eachToClaim=Window.partitionBy("toADMSN_DT_DAY", "toCLAIMNO")
    transfersDF = transfersDF.withColumn("isClosestFromClaim", (F.col("fromTHRU_DT_DAY")==F.max(F.col("fromTHRU_DT_DAY")).over(eachToClaim)).cast('int'))
    return transfersDF.filter(F.col("isClosestFromClaim")==1).drop("isClosestFromClaim")

def remove_uncertain_transfers(transfersDF):
    '''Some ip claims are associated with more than one op claim because there are more than one op claims that meet all
    other transfer criteria. Since we do not have hourly chronological information we cannot distinguish which op claim
    was the last one (to use for the transfer), and we will remove those. There will not be a transfer associated with these cases.
    The same is true the other way around (more than one ip claims for one op claim).
    Based on the cases I encountered those are very very few.'''
    eachToClaim=Window.partitionBy("toADMSN_DT_DAY", "toCLAIMNO")
    eachFromClaim=Window.partitionBy("fromTHRU_DT_DAY", "fromCLAIMNO")
    transfersDF = (transfersDF.withColumn("numberOfToClaimsForFromClaim", F.count(F.col("toCLAIMNO")).over(eachFromClaim))
                              .filter(F.col("numberOfToClaimsForFromClaim")==1).drop("numberOfToClaimsForFromClaim")
                              .withColumn("numberOfFromClaimsForToClaim", F.count(F.col("fromCLAIMNO")).over(eachToClaim))
                              .filter(F.col("numberOfFromClaimsForToClaim")==1).drop("numberOfFromClaimsForToClaim"))
    return transfersDF

def get_clean_transfers(transfersDF):
    transfersDF = get_closest_to_claim(transfersDF)
    transfersDF = get_closest_from_claim(transfersDF)
    transfersDF = remove_uncertain_transfers(transfersDF)
    return transfersDF

def get_transfers(fromClaimsDF, toClaimsDF):
    # rename the dataframe columns because pandas cannot handle two columns with the same name, just in case pd is used in analysis 
    toClaimsDF = toClaimsDF.toDF(*("to"+c for c in toClaimsDF.columns))
    fromClaimsDF = fromClaimsDF.toDF(*("from"+c for c in fromClaimsDF.columns))
    transfersDF = fromClaimsDF.join(toClaimsDF,    
                             on = [(F.col("fromDSYSRTKY")==F.col("toDSYSRTKY")) &
                                   (F.col("fromTHRU_DT_DAY")<=F.col("toADMSN_DT_DAY")) &
                                   (F.col("fromTHRU_DT_DAY")>=F.col("toADMSN_DT_DAY")-1) &
                                   (F.col("fromORGNPINM") != F.col("toORGNPINM"))],
                             how = "inner") #drop any claims that do not match
    transfersDF = get_clean_transfers(transfersDF)
    return transfersDF

def add_transfertpa(transfersDF):
    return transfersDF.withColumn("transfertpa", F.when( (F.col("totpa")==1) | (F.col("fromtpa")==1), 1).otherwise(0))

def add_transfernihss(transfersDF):
    return transfersDF.withColumn("transfernihss", F.when( F.col("fromnihss").isNull(), F.col("tonihss")).otherwise(F.col("fromnihss")))

def add_transfernihssGroup(transfersDF):
    return transfersDF.withColumn("transfernihssGroup", F.when( F.col("fromnihssGroup").isNull(), F.col("tonihssGroup")).otherwise(F.col("fromnihssGroup")))   

def add_stroke_info(transfersDF):
    transfersDF = add_transfertpa(transfersDF)
    transfersDF = add_transfernihss(transfersDF)
    transfersDF = add_transfernihssGroup(transfersDF)
    transfersDF = add_node_stroke_treatment_info(transfersDF)
    transfersDF = add_dyad_stroke_treatment_info(transfersDF)
    return transfersDF

def add_firstTransfer(transfersDF):
    '''Assumes that get_clean_transfers has been run on the transferDF.'''
    eachDsysrtky=Window.partitionBy("fromDSYSRTKY")
    transfersDF = transfersDF.withColumn("firstTransfer", (F.col("fromTHRU_DT_DAY") == F.min(F.col("fromTHRU_DT_DAY")).over(eachDsysrtky)).cast('int') )
    return transfersDF

def add_node_volume_info(transfersDF):
    eachFromProvider = Window.partitionBy(["fromORGNPINM","fromTHRU_DT_YEAR"])
    eachToProvider = Window.partitionBy(["toORGNPINM","toTHRU_DT_YEAR"])
    transfersDF = (transfersDF.withColumn("nodeOutVol", F.count( F.col("fromCLAIMNO") ).over(eachFromProvider))
                              .withColumn("nodeInVol", F.count( F.col("toCLAIMNO") ).over(eachToProvider)))
    return transfersDF

def add_prior_hospitalization_info(transfersDF, ipBaseDF):
    transfersDF = (baseF.add_prior_hospitalization_info(
                           transfersDF.withColumnRenamed("toDSYSRTKY", "DSYSRTKY")
                                      .withColumnRenamed("toADMSN_DT_DAY", "ADMSN_DT_DAY")
                                      .withColumnRenamed("toADMSN_DT_MONTH", "ADMSN_DT_MONTH")
                                      .withColumnRenamed("toCLAIMNO", "CLAIMNO")
                                      .withColumnRenamed("toffsFirstMonth", "ffsFirstMonth"), 
                           ipBaseDF)
                       .withColumnRenamed("DSYSRTKY", "toDSYSRTKY")
                       .withColumnRenamed("ADMSN_DT_DAY", "toADMSN_DT_DAY")
                       .withColumnRenamed("ADMSN_DT_MONTH", "toADMSN_DT_MONTH")
                       .withColumnRenamed("CLAIMNO", "toCLAIMNO")
                       .withColumnRenamed("ffsFirstMonth", "toffsFirstMonth"))
    return transfersDF

def add_days_at_home_info(transfersDF, snfBaseDF, hhaBaseDF, hospBaseDF, ipBaseDF):
    columnsInitial = transfersDF.columns
    columnsTemp = [re.sub("^to","",c) for c in columnsInitial] #need to rename transfers df because days_at_home needs standard column names
    transfersDF = transfersDF.toDF(*columnsTemp)
    transfersDF = baseF.add_days_at_home_info(transfersDF, snfBaseDF, hhaBaseDF, hospBaseDF, ipBaseDF)
    revertColumnRenaming = dict(zip(columnsTemp,columnsInitial)) #and now change the column names back to their initial ones
    columnsAll = transfersDF.columns
    columnsFinal = columnsAll
    for i in range(len(columnsAll)):
        c = columnsAll[i]
        if c in columnsTemp:
            columnsFinal[i] = revertColumnRenaming[c]     
    transfersDF = transfersDF.toDF(*columnsFinal)
    return transfersDF

def add_dyad(transfersDF):
    transfersDF = transfersDF.withColumn("dyad", F.array( F.col("fromORGNPINM"),F.col("toORGNPINM"), F.col("fromTHRU_DT_YEAR")))
    return transfersDF

def add_dyadVi(transfersDF):
    transfersDF = transfersDF.withColumn("dyadVi", F.when( 
						        (~F.col("fromproviderSysId").isNull()) & 
                                                        (~F.col("toproviderSysId").isNull()) &
                                                        (F.col("fromproviderSysId")==F.col("toproviderSysId")), 1)
                                                    .otherwise(0))
    return transfersDF

def add_dyadTransferVol(transfersDF):
    eachDyad = Window.partitionBy("dyad")
    transfersDF = transfersDF.withColumn("dyadTransferVol", F.count( F.col("fromCLAIMNO") ).over(eachDyad))
    return transfersDF

def add_dyad_evt_info(transfersDF):
    eachDyad = Window.partitionBy("dyad")
    transfersDF = (transfersDF.withColumn("dyadEvtVol", F.sum( F.col("toevt") ).over(eachDyad))
                              .withColumn("dyadEvtMean", F.mean( F.col("toevt")).over(eachDyad))
                              .withColumn("dyadIncludesEvt", F.max( F.col("toevt") ).over(eachDyad)))
    return transfersDF

def add_dyad_tpa_info(transfersDF):
    eachDyad = Window.partitionBy("dyad")
    transfersDF = (transfersDF.withColumn("dyadTpaVol", F.sum( F.col("transfertpa") ).over(eachDyad))
                              .withColumn("dyadTpaMean", F.mean( F.col("transfertpa")).over(eachDyad))
                              .withColumn("dyadIncludesTpa", F.max( F.col("transfertpa") ).over(eachDyad)))
    return transfersDF

def add_dyad_stroke_treatment_info(transfersDF):
    transfersDF = add_dyad_tpa_info(transfersDF)
    transfersDF = add_dyad_evt_info(transfersDF)
    return transfersDF

def add_dyadAcrossCounties(transfersDF):
    transfersDF = transfersDF.withColumn("dyadAcrossCounties", 
                                         F.when( (F.col("toproviderFIPS")!=F.col("fromproviderFIPS")) &
                                                 (~F.col("toproviderFIPS").isNull()) &
                                                 (~F.col("fromproviderFIPS").isNull()), 1)
                                          .when( (F.col("toproviderFIPS").isNull()) | 
                                                 (F.col("fromproviderFIPS").isNull()), F.lit(None)) 
                                          .otherwise(0))
    return transfersDF

def add_node_stroke_treatment_info(transfersDF):
    eachFromProvider = Window.partitionBy(["fromORGNPINM","fromTHRU_DT_YEAR"])
    eachToProvider = Window.partitionBy(["toORGNPINM","toTHRU_DT_YEAR"])
    transfersDF = (transfersDF.withColumn("nodeFromEvtVol", F.sum( F.col("toevt") ).over(eachFromProvider))
                              .withColumn("nodeFromTpaVol", F.sum( F.col("transfertpa") ).over(eachFromProvider))
                              .withColumn("nodeFromEvtMean", F.mean( F.col("toevt")).over(eachFromProvider))
                              .withColumn("nodeFromTpaMean", F.mean( F.col("transfertpa")).over(eachFromProvider))
                              .withColumn("nodeToEvtVol", F.sum( F.col("toevt") ).over(eachToProvider))
                              .withColumn("nodeToTpaVol", F.sum( F.col("transfertpa") ).over(eachToProvider))
                              .withColumn("nodeToEvtMean", F.mean( F.col("toevt")).over(eachToProvider))
                              .withColumn("nodeToTpaMean", F.mean( F.col("transfertpa")).over(eachToProvider)))
    return transfersDF
 
def add_node_from_to_info(transfersDF):
    eachFromProvider = Window.partitionBy(["fromORGNPINM","fromTHRU_DT_YEAR"])
    eachToProvider = Window.partitionBy(["toORGNPINM","toTHRU_DT_YEAR"])
    transfersDF = (transfersDF.withColumn("nodeFromToSet", F.collect_set( F.col("toORGNPINM")).over(eachFromProvider))
                              .withColumn("nodeFromToSize", F.size( F.col("nodeFromToSet") ))
                              .withColumn("nodeToFromSet", F.collect_set( F.col("fromORGNPINM")).over(eachToProvider))
                              .withColumn("nodeToFromSize", F.size( F.col("nodeToFromSet") )))
    return transfersDF

def add_dyad_info(transfersDF):
    transfersDF = add_dyad(transfersDF)
    transfersDF = add_dyadVi(transfersDF)
    transfersDF = add_dyadTransferVol(transfersDF)
    transfersDF = add_dyadAcrossCounties(transfersDF)
    return transfersDF

def add_node_info(transfersDF):
    transfersDF = add_node_volume_info(transfersDF)
    transfersDF = add_node_from_to_info(transfersDF)
    return transfersDF


















 
