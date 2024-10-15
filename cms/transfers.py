import pyspark.sql.functions as F
from pyspark.sql.window import Window

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

def get_tranfers(opClaimsDF, ipClaimsDF):
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

def add_trasfernihssGroup(opIpDF):
    return opIpDF.withColumn("transfernihssGroup", F.when( F.col("opnihssGroup").isNull(), F.col("ipnihssGroup")).otherwise(F.col("opnihssGroup"))))   

def add_stroke_info(opIpDF):
    opIpDF = add_transfertpa(opIpDF)
    opIpDF = add_transfernihss(opIpDF)
    opIpDF = add_transfernihssGroup(opIpDF)
    return opIpDF
