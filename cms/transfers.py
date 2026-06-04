import pyspark.sql.functions as F
from pyspark.sql.window import Window
import cms.base as baseF
import cms.stays as staysF
import utilities as utilitiesF

def _rename_columns(df, mapping):
    '''Single-projection rename: aliases each column named in mapping to its target and passes the
    rest through unchanged, preserving column order. Preferred over a chained-withColumnRenamed loop
    because each withColumnRenamed adds its own Project node, bloating the logical plan that Catalyst
    walks on every optimizer pass; this is one Project node regardless of how many columns are renamed.'''
    return df.select([F.col(c).alias(mapping.get(c, c)) for c in df.columns])

def get_closest_to_claim(transfersDF):
    '''Transfers include a from stay and a to stay...this function finds the to stay that is the closest in time to the from stay.'''
    eachFromClaim=Window.partitionBy("fromTHRU_DT_DAY", "fromCLAIMNO")
    transfersDF = transfersDF.withColumn("isClosestToClaim", (F.col("toADMSN_DT_DAY")==F.min(F.col("toADMSN_DT_DAY")).over(eachFromClaim)).cast('int'))
    return transfersDF.filter(F.col("isClosestToClaim")==1).drop("isClosestToClaim")

def get_closest_from_claim(transfersDF):
    '''Transfers include a from stay and a to stay...this function finds the from stay that is the closest in time to the to stay.'''
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
    '''Returns only transfers for which we have confidence in, no ambiguities'''
    transfersDF = get_closest_to_claim(transfersDF)
    transfersDF = get_closest_from_claim(transfersDF)
    transfersDF = remove_uncertain_transfers(transfersDF)
    return transfersDF

def get_transfers(fromClaimsDF, toClaimsDF):
    '''Takes two dataframes, one that represents stays in the from provider and another one that represents stays in the to provider.
    Merges the two dataframes to create transfers from the from provider to the to provider.'''
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
    transfersDF = add_firstTransfer(transfersDF)
    transfersDF = add_node_and_dyad_info(transfersDF)
    return transfersDF

def add_transfertpa(transfersDF):
    return transfersDF.withColumn("transfertpa", F.when( (F.col("totpa")==1) | (F.col("fromtpa")==1), 1).otherwise(0))

def add_transferct(transfersDF):
    return transfersDF.withColumn("transferct", F.when( (F.col("toct")==1) | (F.col("fromct")==1), 1).otherwise(0))

def add_transfermri(transfersDF):
    return transfersDF.withColumn("transfermri", F.when( (F.col("tomri")==1) | (F.col("frommri")==1), 1).otherwise(0))

def add_transfernihss(transfersDF):
    return transfersDF.withColumn("transfernihss", F.when( F.col("fromnihss").isNull(), F.col("tonihss")).otherwise(F.col("fromnihss")))

def add_transfernihssGroup(transfersDF):
    return transfersDF.withColumn("transfernihssGroup", F.when( F.col("fromnihssGroup").isNull(), F.col("tonihssGroup")).otherwise(F.col("fromnihssGroup")))   
def add_stroke_info(transfersDF):
    transfersDF = add_transferct(transfersDF)
    transfersDF = add_transfermri(transfersDF)
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
    '''Given transfers, calculate the number of transfers that go out from a given from provider and the
    number of transfers that go in to a given to provider.
    Both partitions key on fromTHRU_DT_YEAR (the transfer's originating year) so that node- and dyad-level
    aggregations share a single year basis -- see add_dyad.'''
    eachFromProvider = Window.partitionBy(["fromORGNPINM","fromTHRU_DT_YEAR"])
    eachToProvider = Window.partitionBy(["toORGNPINM","fromTHRU_DT_YEAR"])
    transfersDF = (transfersDF.withColumn("nodeOutVol", F.count( F.col("fromCLAIMNO") ).over(eachFromProvider))
                              .withColumn("nodeInVol", F.count( F.col("toCLAIMNO") ).over(eachToProvider)))
    return transfersDF

def add_prior_hospitalization_info(transfersDF, ipBaseDF):
    '''Adds columns about prior hospitalizations for each transfer patient.
    baseF.add_prior_hospitalization_info reads its keys by canonical (un-prefixed) name, so we rename only
    the specific to-side columns it consults, then restore them after the call. Output columns it adds
    (hospitalizationsIn12Months, hospitalizedIn12Months, hospitalizationsIn6Months, hospitalizedIn6Months)
    describe the receiving stay and are kept un-prefixed.'''
    toToCanonical = {
        "toDSYSRTKY":        "DSYSRTKY",
        "toCLAIMNO":         "CLAIMNO",
        "toADMSN_DT_DAY":    "ADMSN_DT_DAY",
        "toADMSN_DT_MONTH":  "ADMSN_DT_MONTH",
        "toffsFirstMonth":   "ffsFirstMonth",
    }
    transfersDF = _rename_columns(transfersDF, toToCanonical)
    transfersDF = baseF.add_prior_hospitalization_info(transfersDF, ipBaseDF)
    transfersDF = _rename_columns(transfersDF, {dst: src for src, dst in toToCanonical.items()})
    return transfersDF

def add_days_at_home_info(transfersDF, snfBaseDF, hhaBaseDF, hospBaseDF, ipBaseDF):
    '''For the transfers dataframe, this function adds columns about the number of days the patients stayed home.
    baseF.add_days_at_home_info reads its date/status columns by canonical (un-prefixed) name, so we rename only
    the specific to-side columns it consults, then restore them after the call. Output columns it adds
    (homeDays90, homeDays365, etc.) describe the receiving stay and are kept un-prefixed.'''
    toToCanonical = {
        "toDSYSRTKY":                       "DSYSRTKY",
        "toCLAIMNO":                        "CLAIMNO",
        "toADMSN_DT_DAY":                   "ADMSN_DT_DAY",
        "toTHRU_DT_DAY":                    "THRU_DT_DAY",
        "toDEATH_DT_DAY":                   "DEATH_DT_DAY",
        "toSTUS_CD":                        "STUS_CD",
        "to90DaysAfterAdmissionDateDead":   "90DaysAfterAdmissionDateDead",
        "to365DaysAfterAdmissionDateDead":  "365DaysAfterAdmissionDateDead",
    }
    transfersDF = _rename_columns(transfersDF, toToCanonical)
    transfersDF = baseF.add_days_at_home_info(transfersDF, snfBaseDF, hhaBaseDF, hospBaseDF, ipBaseDF)
    transfersDF = _rename_columns(transfersDF, {dst: src for src, dst in toToCanonical.items()})
    return transfersDF

def add_comorbidity_info(transfersDF, ipBase, opBase, claimType="ip", method="Glasheen2019"):
    '''Adds comorbidity condition flags + comorbityIndex describing the TO (receiving)
    stay of each transfer. staysF.add_comorbidity_info reads its keys by canonical
    (un-prefixed) name, so we rename only the specific to-side columns it consults,
    then restore them after the call. hospitalizationsIn12Months is already un-prefixed
    on transfersDF (see add_prior_hospitalization_info) and the comorbidity output
    columns are likewise kept un-prefixed since they describe the receiving stay.
    Requires add_prior_hospitalization_info to have run first.'''
    toToCanonical = {
        "toDSYSRTKY":    "DSYSRTKY",
        "toTHRU_DT_DAY": "THRU_DT_DAY",
    }
    transfersDF = _rename_columns(transfersDF, toToCanonical)
    transfersDF = staysF.add_comorbidity_info(transfersDF, ipBase, opBase,
                                              claimType=claimType, method=method)
    transfersDF = _rename_columns(transfersDF, {dst: src for src, dst in toToCanonical.items()})
    return transfersDF

def add_dyad(transfersDF):
    '''Dyad is defined by the two NPI numbers and the year'''
    transfersDF = transfersDF.withColumn("dyad", F.array( F.col("fromORGNPINM"),F.col("toORGNPINM"), F.col("fromTHRU_DT_YEAR")))
    return transfersDF

def add_dyadVi(transfersDF):
    '''Returns 1 if both providers of the dyad in a given year have the same SYSID and 0 otherwise'''
    transfersDF = transfersDF.withColumn("dyadVi", F.when( 
						        (~F.col("fromproviderSysId").isNull()) & 
                                                        (~F.col("toproviderSysId").isNull()) &
                                                        (F.col("fromproviderSysId")==F.col("toproviderSysId")), 1)
                                                    .otherwise(0))
    return transfersDF

def add_dyadTransferVol(transfersDF):
    '''Adds a dyadTransferVol column with the number of transfers for the specific dyad in a given year.
    Because the dyad already includes the year (see add_dyad), partitioning by dyad alone is sufficient
    for a per-(fromNode, toNode, year) count.'''
    eachDyad = Window.partitionBy("dyad")
    transfersDF = transfersDF.withColumn("dyadTransferVol", F.count( F.col("fromCLAIMNO") ).over(eachDyad))
    return transfersDF

def add_dyadProportionTransfersOut(transfersDF):
    '''The proportion of all transfers out from the sending organization that go to the receiving organization by year.
    This column should have a minimum of 0. and a maximum of 1.'''
    transfersDF = transfersDF.withColumn("dyadProportionTransfersOut", 
                                         F.when( F.col("nodeOutVol")!=0, F.col("dyadTransferVol")/F.col("nodeOutVol") ).otherwise(F.lit(0.))) 
    return transfersDF

def add_dyadProportionTransfersIn(transfersDF):
    '''The proportion of all transfers in to the receiving organization that came from the sending organization in the dyad by year.
    This column should have a minimum of 0. and a maximum of 1.'''
    transfersDF = transfersDF.withColumn("dyadProportionTransfersIn",
                                         F.when( F.col("nodeInVol")!=0, F.col("dyadTransferVol")/F.col("nodeInVol") ).otherwise(F.lit(0.)))
    return transfersDF

def add_dyad_evt_info(transfersDF):
    '''Adds evt related columns about each dyad'''
    eachDyad = Window.partitionBy("dyad")
    transfersDF = (transfersDF.withColumn("dyadEvtVol", F.sum( F.col("toevt") ).over(eachDyad))
                              .withColumn("dyadEvtMean", F.mean( F.col("toevt")).over(eachDyad))
                              .withColumn("dyadIncludesEvt", F.max( F.col("toevt") ).over(eachDyad)))
    return transfersDF

def add_dyad_tpa_info(transfersDF):
    '''Adds tpa related columns about each dyad'''
    eachDyad = Window.partitionBy("dyad")
    transfersDF = (transfersDF.withColumn("dyadTpaVol", F.sum( F.col("transfertpa") ).over(eachDyad))
                              .withColumn("dyadTpaMean", F.mean( F.col("transfertpa")).over(eachDyad))
                              .withColumn("dyadIncludesTpa", F.max( F.col("transfertpa") ).over(eachDyad)))
    return transfersDF

def add_dyad_stroke_treatment_info(transfersDF):
    '''Adds various columns for each dyad regarding stroke treatments.'''
    transfersDF = add_dyad_tpa_info(transfersDF)
    transfersDF = add_dyad_evt_info(transfersDF)
    return transfersDF

def add_dyadAcrossCounties(transfersDF):
    '''Adds a flag to indicate whether the from and to nodes are located in different counties or not.'''
    transfersDF = transfersDF.withColumn("dyadAcrossCounties", 
                                         F.when( (F.col("toproviderFIPS")!=F.col("fromproviderFIPS")) &
                                                 (~F.col("toproviderFIPS").isNull()) &
                                                 (~F.col("fromproviderFIPS").isNull()), 1)
                                          .when( (F.col("toproviderFIPS").isNull()) | 
                                                 (F.col("fromproviderFIPS").isNull()), F.lit(None)) 
                                          .otherwise(0))
    return transfersDF

def add_dyadAcrossStates(transfersDF):
    '''Adds a flag to indicate whether the from and to nodes are located in different states or not.'''
    transfersDF = transfersDF.withColumn("dyadAcrossStates",
                                         F.when( (F.col("toproviderStateFIPS")!=F.col("fromproviderStateFIPS")) &
                                                 (~F.col("toproviderStateFIPS").isNull()) &
                                                 (~F.col("fromproviderStateFIPS").isNull()), 1)
                                          .when( (F.col("toproviderStateFIPS").isNull()) |
                                                 (F.col("fromproviderStateFIPS").isNull()), F.lit(None))
                                          .otherwise(0))
    return transfersDF

def add_nodeHhi(transfersDF):
    '''Calculates the Herfindahl-Hirschman index (HHI) of sending hospital's transfer destinations.
    It is unique to each organization and year and reflects the competitiveness of the receiving hospitals for transfers from this sending organization.
    The minimum is 1/N (where N is the number of distinct destinations from the sending organization in that year),
    achieved when transfers are evenly distributed across destinations; the maximum is 1, achieved when all transfers
    from the sending organization go to a single destination.'''
    eachDyad = Window.partitionBy(["dyad", "fromTHRU_DT_YEAR"]).orderBy("fromTHRU_DT_YEAR") #dyad includes the year too but I also need to orderBy...
    eachFromProvider = Window.partitionBy(["fromORGNPINM","fromTHRU_DT_YEAR"])
    transfersDF = (transfersDF
                    .withColumn("dyadRowNumber", F.row_number().over(eachDyad)) #use this to get the first row from each dyad to avoid double counting
                    .withColumn("dyadProportionTransfersOutSquared", 
                                  F.when(F.col("dyadRowNumber")==1, F.pow(F.col("dyadProportionTransfersOut"), 2)).otherwise(F.lit(0.)))
                    .withColumn("nodeHhi", F.sum(F.col("dyadProportionTransfersOutSquared")).over(eachFromProvider))
                    .drop("dyadRowNumber", "dyadProportionTransfersOutSquared"))
    return transfersDF

def add_node_stroke_treatment_info(transfersDF):
    '''Stroke treatment refers to evt, tpa columns, adds stroke treatment infor volume and mean for both from and to nodes.
    The to-side window keys on fromTHRU_DT_YEAR for year-basis consistency with the dyad (see add_dyad).'''
    eachFromProvider = Window.partitionBy(["fromORGNPINM","fromTHRU_DT_YEAR"])
    eachToProvider = Window.partitionBy(["toORGNPINM","fromTHRU_DT_YEAR"])
    transfersDF = (transfersDF.withColumn("nodeFromEvtVol", F.sum( F.col("toevt") ).over(eachFromProvider))
                              .withColumn("nodeFromTpaVol", F.sum( F.col("transfertpa") ).over(eachFromProvider))
                              .withColumn("nodeFromEvtMean", F.mean( F.col("toevt")).over(eachFromProvider))
                              .withColumn("nodeFromTpaMean", F.mean( F.col("transfertpa")).over(eachFromProvider))
                              .withColumn("nodeToEvtVol", F.sum( F.col("toevt") ).over(eachToProvider))
                              .withColumn("nodeToTpaVol", F.sum( F.col("transfertpa") ).over(eachToProvider))
                              .withColumn("nodeToEvtMean", F.mean( F.col("toevt")).over(eachToProvider))
                              .withColumn("nodeToTpaMean", F.mean( F.col("transfertpa")).over(eachToProvider)))
    return transfersDF
 
def add_node_revenue_info(transfersDF):
    '''Revenue info refers to the ed, ct, mri columns, adds revenue info volume and mean for both from and to nodes.
    The to-side window keys on fromTHRU_DT_YEAR for year-basis consistency with the dyad (see add_dyad).'''
    eachFromProvider = Window.partitionBy(["fromORGNPINM","fromTHRU_DT_YEAR"])
    eachToProvider = Window.partitionBy(["toORGNPINM","fromTHRU_DT_YEAR"])
    transfersDF = (transfersDF.withColumn("nodeFromEdVol", F.sum( F.col("fromed") ).over(eachFromProvider))
                              .withColumn("nodeFromEdMean", F.mean( F.col("fromed") ).over(eachFromProvider))
                              .withColumn("nodeFromCtVol", F.sum( F.col("fromct") ).over(eachFromProvider))
                              .withColumn("nodeFromCtMean", F.mean( F.col("fromct") ).over(eachFromProvider))
                              .withColumn("nodeFromMriVol", F.sum( F.col("frommri") ).over(eachFromProvider))
                              .withColumn("nodeFromMriMean", F.mean( F.col("frommri") ).over(eachFromProvider))
                              .withColumn("nodeToEdVol", F.sum( F.col("toed") ).over(eachToProvider))
                              .withColumn("nodeToEdMean", F.mean( F.col("toed") ).over(eachToProvider))
                              .withColumn("nodeToCtVol", F.sum( F.col("toct") ).over(eachToProvider))
                              .withColumn("nodeToCtMean", F.mean( F.col("toct") ).over(eachToProvider))
                              .withColumn("nodeToMriVol", F.sum( F.col("tomri") ).over(eachToProvider))
                              .withColumn("nodeToMriMean", F.mean( F.col("tomri") ).over(eachToProvider)))
    return transfersDF

def add_node_from_to_info(transfersDF):
    '''Adds columns about the connections each node has.
    For example, each FROM node has transfers to potentially more than one TO node, so this function adds the set of all TO nodes where each FROM node transfers to (set of NPI numbers).
    The columns related to size are the sizes of the corresponding set, eg, based on the example above, the size of the set with all TO NPI numbers.'''
    eachFromProvider = Window.partitionBy(["fromORGNPINM","fromTHRU_DT_YEAR"])
    eachToProvider = Window.partitionBy(["toORGNPINM","fromTHRU_DT_YEAR"])
    transfersDF = (transfersDF.withColumn("nodeFromSetOfToNodes", F.collect_set( F.col("toORGNPINM")).over(eachFromProvider))
                              .withColumn("nodeFromSizeOfToNodes", F.size( F.col("nodeFromSetOfToNodes") ))
                              .withColumn("nodeToSetOfFromNodes", F.collect_set( F.col("fromORGNPINM")).over(eachToProvider))
                              .withColumn("nodeToSizeOfFromNodes", F.size( F.col("nodeToSetOfFromNodes") )))
    return transfersDF

def add_column_prior(transfersDF, column, who, when):
    '''Adds the column's value from the prior year at the transfers grain.
    Thin wrapper over utilitiesF.add_column_prior; the shared logic lives in utilities.py.
    Callers pass who/when explicitly. `who` must match the grain of `column`:
    who="fromORGNPINM" for provider/node-level columns keyed on the sending provider
    (eg nodeHhi), and who=["fromORGNPINM","toORGNPINM"] -- the year-less dyad pair -- for
    dyad-level columns (eg dyadProportionTransfersOut/In) so the prior value comes from the
    same dyad a year earlier rather than an arbitrary partner. when=fromTHRU_DT_YEAR is the
    transfer's originating year.'''
    return utilitiesF.add_column_prior(transfersDF, column=column, who=who, when=when)

def add_node_hhi_info(transfersDF):
    '''Adds the HHI for each provider and year and the one for the year prior.'''
    transfersDF = add_nodeHhi(transfersDF)
    transfersDF = add_column_prior(transfersDF, column="nodeHhi", who="fromORGNPINM", when="fromTHRU_DT_YEAR")
    return transfersDF

def add_dyad_info(transfersDF):
    '''Adds various columns about each dyad'''
    transfersDF = add_dyad(transfersDF)
    transfersDF = add_dyadVi(transfersDF)
    transfersDF = add_dyadTransferVol(transfersDF)
    transfersDF = add_dyadProportionTransfersOut(transfersDF)
    transfersDF = add_dyadProportionTransfersIn(transfersDF)
    transfersDF = add_dyadAcrossCounties(transfersDF)
    transfersDF = add_dyadAcrossStates(transfersDF)
    transfersDF = add_column_prior(transfersDF, column="dyadProportionTransfersOut", who=["fromORGNPINM","toORGNPINM"], when="fromTHRU_DT_YEAR")
    transfersDF = add_column_prior(transfersDF, column="dyadProportionTransfersIn", who=["fromORGNPINM","toORGNPINM"], when="fromTHRU_DT_YEAR")
    return transfersDF

def add_node_info(transfersDF):
    '''Adds various columns about each node'''
    transfersDF = add_node_volume_info(transfersDF)
    transfersDF = add_node_from_to_info(transfersDF)
    transfersDF = add_node_revenue_info(transfersDF)
    return transfersDF

def add_node_and_dyad_info(transfersDF):
    '''Adds various columns about nodes and dyads'''
    transfersDF = add_node_info(transfersDF)
    transfersDF = add_dyad_info(transfersDF)
    transfersDF = add_node_hhi_info(transfersDF)
    return transfersDF
















 
