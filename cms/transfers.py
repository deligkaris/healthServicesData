from enum import IntEnum
from functools import reduce
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import cms.base as baseF
import cms.stays as staysF
import cms.utilities as cmsUtilitiesF
import geocoding as geocodingF
import utilities as utilitiesF

class TransferType(IntEnum):
    '''Origin->destination setting of a transfer. The destination is always an inpatient admission (the
    to stay); the members distinguish the sending setting. Stored as the integer transferType column
    (see add_transferType) -- IntEnum so the column holds a plain int for Spark comparison/grouping
    while call sites and downstream logic use the named members.'''
    edToInpatient = 1
    inpatientToInpatient = 2

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

def get_transfers(*, fromStaysDf, toStaysDf, transferType):
    '''Takes two dataframes, one that represents stays in the from provider and another one that represents stays in the to provider.
    Merges the two dataframes to create transfers from the from provider to the to provider.
    transferType is a TransferType member describing the origin->destination setting of this whole
    dataframe -- it is a property of fromStaysDf (all-OP-ED vs all-IP), known here to the caller, not
    derivable per row -- and is stamped on every transfer (see add_transferType).
    firstTransfer and the node/dyad columns describe this transfer set only; they stay per-set after a
    union of sets, so do not recompute them on the union.'''
    # rename the dataframe columns because pandas cannot handle two columns with the same name, just in case pd is used in analysis
    toStaysDf = toStaysDf.toDF(*("to"+c for c in toStaysDf.columns))
    fromStaysDf = fromStaysDf.toDF(*("from"+c for c in fromStaysDf.columns))
    transfersDF = fromStaysDf.join(toStaysDf,
                             on = [(F.col("fromDSYSRTKY")==F.col("toDSYSRTKY")) &
                                   (F.col("fromTHRU_DT_DAY")<=F.col("toADMSN_DT_DAY")) &
                                   (F.col("fromTHRU_DT_DAY")>=F.col("toADMSN_DT_DAY")-1) &
                                   (F.col("fromORGNPINM") != F.col("toORGNPINM"))],
                             how = "inner") #drop any claims that do not match
    transfersDF = get_clean_transfers(transfersDF)
    transfersDF = add_transferType(transfersDF, transferType)
    transfersDF = add_firstTransfer(transfersDF)
    transfersDF = add_node_and_dyad_info(transfersDF)
    return transfersDF

def get_union(*transfersDFs):
    '''Returns the union of transfer sets from get_transfers, each keeping its own node/dyad columns (see get_transfers).'''
    #the from stays of each set come from different claim types, so the columns differ; a missing one is NULL
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), transfersDFs)

def add_transferType(transfersDF, transferType):
    '''Stamps transferType (a TransferType member) as the integer transferType column on every row.
    The origin setting is a property of the fromStaysDf these transfers were built from (all-OP-ED vs
    all-IP, see get_transfers), constant across the dataframe and known to the caller, not derivable
    per row.'''
    return transfersDF.withColumn("transferType", F.lit(int(transferType)))

def add_firstTransfer(transfersDF):
    '''Adds firstTransfer: 1 for the patient's earliest transfer (min fromTHRU_DT_DAY) within this set of
    transfers -- the one get_transfers call / transferType -- not across all transfer types, so after an
    edToInpatient + inpatientToInpatient union a patient can have firstTransfer=1 in each set.
    Assumes that get_clean_transfers has been run on the transferDF.'''
    eachDsysrtky=Window.partitionBy("fromDSYSRTKY")
    transfersDF = transfersDF.withColumn("firstTransfer", (F.col("fromTHRU_DT_DAY") == F.min(F.col("fromTHRU_DT_DAY")).over(eachDsysrtky)).cast('int') )
    return transfersDF

#transfer-level stroke flags

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

def add_transferEvtOrTpa(transfersDF):
    '''Flags transfers that received EVT or tPA. Uses toevt (the receiving stay's EVT flag, the same
    column summed in add_node_stroke_treatment_info) and transfertpa (add_transfertpa). Requires both.'''
    return transfersDF.withColumn("transferEvtOrTpa", F.when( (F.col("toevt")==1) | (F.col("transfertpa")==1), 1).otherwise(0))

def add_stroke_info(transfersDF):
    '''Adds transfer-level stroke flags and their node/dyad aggregates. Run it on each transfer set before
    any union so the aggregates stay per-set, like those get_transfers adds.'''
    transfersDF = add_transferct(transfersDF)
    transfersDF = add_transfermri(transfersDF)
    transfersDF = add_transfertpa(transfersDF)
    transfersDF = add_transferEvtOrTpa(transfersDF)
    transfersDF = add_transfernihss(transfersDF)
    transfersDF = add_transfernihssGroup(transfersDF)
    transfersDF = add_node_stroke_treatment_info(transfersDF)
    transfersDF = add_dyad_stroke_treatment_info(transfersDF)
    return transfersDF

#node measures: one value per provider and transfer year, on each of its transfers

def add_node_volume_info(transfersDF):
    '''Adds nodeOutVol, the number of transfers the sending provider sent, and nodeInVol, the number the receiving
    provider received, in the transfer year; add_node_prior_info adds their prior-year values.
    Both partitions key on fromTHRU_DT_YEAR (the transfer's originating year) so that node- and dyad-level
    aggregations share a single year basis -- see add_dyad.'''
    eachFromProvider = Window.partitionBy(["fromORGNPINM","fromTHRU_DT_YEAR"])
    eachToProvider = Window.partitionBy(["toORGNPINM","fromTHRU_DT_YEAR"])
    transfersDF = (transfersDF.withColumn("nodeOutVol", F.count( F.col("fromCLAIMNO") ).over(eachFromProvider))
                              .withColumn("nodeInVol", F.count( F.col("toCLAIMNO") ).over(eachToProvider)))
    return transfersDF

def add_providerAnnualStays_info(transfersDF, staysDF):
    '''Adds fromproviderAnnualDischarges, fromproviderAnnualAdmissions and toproviderAnnualAdmissions: the
    sending provider's stays in staysDF discharged / admitted in the transfer's year (fromTHRU_DT_YEAR) and
    the receiving provider's stays admitted in it, one value per provider-year. staysDF is inpatient stays,
    one row per stay (staysF.get_stays), carrying THRU_DT_YEAR and ADMSN_DT_YEAR. Raises ValueError if
    transfersDF already has any of these.'''
    # staysF.add_providerAnnual{Discharges,Admissions} run on stays before get_transfers ride along as
    # from/to columns under these exact names and would collide (on ED->IP rows they would also count ED
    # visits); so would a rerun
    newColumns = ["fromproviderAnnualDischarges", "fromproviderAnnualAdmissions", "toproviderAnnualAdmissions"]
    existing = [c for c in newColumns if c in transfersDF.columns]
    if existing:
        raise ValueError(f"transfersDF already has {', '.join(existing)}. staysF.add_providerAnnual{{Discharges,"
                         "Admissions}} are for stays; for transfers, drop these and call "
                         "add_providerAnnualStays_info(transfersDF, staysDF).")
    # one row per provider-year, its year renamed to the transfer year it is matched to
    def lookup(side, counts, column, year):
        return (counts.select(F.col("ORGNPINM").alias(side + "ORGNPINM"),
                              F.col(year).alias("fromTHRU_DT_YEAR"),
                              F.col(column).alias(side + column))
                      .distinct())
    discharges = staysF.add_providerAnnualDischarges(staysDF)
    admissions = staysF.add_providerAnnualAdmissions(staysDF)
    # the transfer year is the year the sending visit/stay ended, which is the receiving stay's admission
    # year: a receiving stay crossing New Year is admitted in the transfer year but discharged in the next
    # left_outer keeps every transfer; a provider-year with no stays in staysDF gets 0
    transfersDF = (transfersDF
                   .join(lookup("from", discharges, "providerAnnualDischarges", "THRU_DT_YEAR"),  on=["fromORGNPINM", "fromTHRU_DT_YEAR"], how="left_outer")
                   .join(lookup("from", admissions, "providerAnnualAdmissions", "ADMSN_DT_YEAR"), on=["fromORGNPINM", "fromTHRU_DT_YEAR"], how="left_outer")
                   .join(lookup("to",   admissions, "providerAnnualAdmissions", "ADMSN_DT_YEAR"), on=["toORGNPINM",   "fromTHRU_DT_YEAR"], how="left_outer")
                   .fillna(0, subset=newColumns))
    return transfersDF

def add_nodeProportionTransferredOut(transfersDF):
    '''Adds nodeProportionTransferredOut, the fraction of the sending provider's patients that year it
    transferred out within the row's transfer set: nodeOutVol / fromproviderAnnualDischarges for
    inpatientToInpatient rows, nodeOutVol / (fromproviderAnnualAdmissions + nodeOutVol) for edToInpatient
    rows. Requires transferType, nodeOutVol (add_node_volume_info) and add_providerAnnualStays_info.'''
    # IP->IP: the transfer year is the sending stay's discharge year and that stay is among the sender's
    # discharges, so they are the whole denominator. ED->IP: the transfer year is the ED visit's, so the
    # patients the sender admitted that year count by admission year, plus the ED patients it sent out,
    # who were never admitted there
    ipDenominator = F.col("fromproviderAnnualDischarges")
    edDenominator = F.col("fromproviderAnnualAdmissions") + F.col("nodeOutVol")
    # an ip denominator of 0 means staysDF lacks the sender-year (a cohort mismatch) -> null, and an ip
    # value > 1 that staysDF misses stays the transfers include; the ed denominator is >= 1 since
    # nodeOutVol >= 1 on a transfer row
    proportion = (F.when(F.col("transferType") == int(TransferType.inpatientToInpatient),
                         F.when(ipDenominator != 0, F.col("nodeOutVol") / ipDenominator))
                   .when(F.col("transferType") == int(TransferType.edToInpatient),
                         F.col("nodeOutVol") / edDenominator))
    return transfersDF.withColumn("nodeProportionTransferredOut", proportion)

def _nodeInVolOfTypes(*transferTypes):
    '''Windowed count of the rows whose transferType is one of transferTypes, per receiving provider and
    transfer year (the to-side node basis, see add_node_volume_info).'''
    eachToProvider = Window.partitionBy(["toORGNPINM","fromTHRU_DT_YEAR"])
    return F.sum(F.when(F.col("transferType").isin([int(t) for t in transferTypes]), 1).otherwise(0)).over(eachToProvider)

def add_nodeToProportionFromEd(transfersDF):
    '''For each receiving provider and year, the fraction of transfers it received that came from the ED
    (edToInpatient) out of those from either setting (edToInpatient + inpatientToInpatient):
    count(transferType == edToInpatient) / count(transferType in {edToInpatient, inpatientToInpatient}).

    Requires transferType (add_transferType / get_transfers) and assumes the edToInpatient and
    inpatientToInpatient transfers have been unioned into one dataframe -- a single transfersDF carries
    one constant transferType, so before the union either the numerator or denominator is degenerate.
    Windowed on (toORGNPINM, fromTHRU_DT_YEAR), the to-side / transfer-year basis every other node
    metric uses (see add_node_volume_info). The denominator is the receiver-year's ED + IP total across
    both sets, so it is not nodeInVol, which stays per-set (see get_transfers); it is counted by
    transferType so a future third TransferType member is excluded rather than folded in. Returns null
    for a receiver-year with no ed/inpatient-origin transfers (denominator 0) -- an undefined
    proportion, not a zero.'''
    edVol = _nodeInVolOfTypes(TransferType.edToInpatient)
    edOrIpVol = _nodeInVolOfTypes(TransferType.edToInpatient, TransferType.inpatientToInpatient)
    transfersDF = transfersDF.withColumn(
        "nodeToProportionFromEd",
        F.when(edOrIpVol != 0, edVol / edOrIpVol).otherwise(F.lit(None)))
    return transfersDF

def add_nodeInVolFromEd(transfersDF):
    '''Adds nodeInVolFromEd: edToInpatient transfers received by the receiving provider that year.'''
    # within one set get_clean_transfers leaves each to-claim once, so rows are admissions
    return transfersDF.withColumn("nodeInVolFromEd", _nodeInVolOfTypes(TransferType.edToInpatient))

def add_nodeInVolFromIp(transfersDF):
    '''Adds nodeInVolFromIp: inpatientToInpatient transfers received by the receiving provider that year.'''
    return transfersDF.withColumn("nodeInVolFromIp", _nodeInVolOfTypes(TransferType.inpatientToInpatient))

def add_nodeInVolFromEdOrIp(transfersDF):
    '''Adds nodeInVolFromEdOrIp: admissions the receiving provider received that year by an edToInpatient
    or inpatientToInpatient transfer. Meant for the union of both sets.'''
    eachToProvider = Window.partitionBy(["toORGNPINM","fromTHRU_DT_YEAR"])
    # one admission can be the to-claim of a transfer in both sets, so count distinct admissions, not
    # rows; other types map to null, which collect_set drops
    admission = F.when(F.col("transferType").isin(int(TransferType.edToInpatient), int(TransferType.inpatientToInpatient)),
                       F.struct("toDSYSRTKY", "toCLAIMNO", "toTHRU_DT_DAY"))
    return transfersDF.withColumn("nodeInVolFromEdOrIp", F.size(F.collect_set(admission).over(eachToProvider)))

def _proportionOfToAdmissions(volume):
    # toproviderAnnualAdmissions counts all of the receiver's admissions, transferred-in ones included, so
    # it is the whole denominator. It is 0 only when staysDF of add_providerAnnualStays_info lacks the
    # receiver-year (a cohort mismatch) -> null; a value > 1 likewise means staysDF misses admissions
    # the transfers include.
    return F.when(F.col("toproviderAnnualAdmissions") != 0,
                  F.col(volume) / F.col("toproviderAnnualAdmissions")).otherwise(F.lit(None))

def add_nodeProportionTransferredInFromEd(transfersDF):
    '''Adds nodeProportionTransferredInFromEd: nodeInVolFromEd / toproviderAnnualAdmissions.
    Requires add_nodeInVolFromEd and add_providerAnnualStays_info.'''
    return transfersDF.withColumn("nodeProportionTransferredInFromEd", _proportionOfToAdmissions("nodeInVolFromEd"))

def add_nodeProportionTransferredInFromIp(transfersDF):
    '''Adds nodeProportionTransferredInFromIp: nodeInVolFromIp / toproviderAnnualAdmissions.
    Requires add_nodeInVolFromIp and add_providerAnnualStays_info.'''
    return transfersDF.withColumn("nodeProportionTransferredInFromIp", _proportionOfToAdmissions("nodeInVolFromIp"))

def add_nodeProportionTransferredInFromEdOrIp(transfersDF):
    '''Adds nodeProportionTransferredInFromEdOrIp: nodeInVolFromEdOrIp / toproviderAnnualAdmissions.
    Requires add_nodeInVolFromEdOrIp and add_providerAnnualStays_info.'''
    return transfersDF.withColumn("nodeProportionTransferredInFromEdOrIp", _proportionOfToAdmissions("nodeInVolFromEdOrIp"))

def add_node_in_by_type_info(transfersDF, *, transferTypes):
    '''Adds the receiving provider's transfer volumes and proportions transferred in for the TransferType
    members in transferTypes, the types transfersDF holds (known to the caller, as in get_transfers).
    Works on one transfer set or their union. Run after add_providerAnnualStays_info.'''
    transferTypes = set(transferTypes)
    if not transferTypes or not all(isinstance(t, TransferType) for t in transferTypes):
        raise ValueError("transferTypes must be a non-empty list of TransferType members")
    # only the columns the given types support: a type absent from transfersDF would read as a false 0
    if TransferType.edToInpatient in transferTypes:
        transfersDF = add_nodeInVolFromEd(transfersDF)
        transfersDF = add_nodeProportionTransferredInFromEd(transfersDF)
    if TransferType.inpatientToInpatient in transferTypes:
        transfersDF = add_nodeInVolFromIp(transfersDF)
        transfersDF = add_nodeProportionTransferredInFromIp(transfersDF)
    # the pooled total and the ED share need both sets
    if {TransferType.edToInpatient, TransferType.inpatientToInpatient} <= transferTypes:
        transfersDF = add_nodeInVolFromEdOrIp(transfersDF)
        transfersDF = add_nodeProportionTransferredInFromEdOrIp(transfersDF)
        transfersDF = add_nodeToProportionFromEd(transfersDF)
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
    transfersDF = add_node_evtOrTpa_info(transfersDF)
    return transfersDF

def add_node_evtOrTpa_info(transfersDF):
    '''Adds per-node (provider-year) volume and mean of transfers that received EVT or tPA.
    Requires transferEvtOrTpa (add_transferEvtOrTpa). Mirrors add_node_stroke_treatment_info: the to-side
    window keys on fromTHRU_DT_YEAR for year-basis consistency with the dyad (see add_dyad).'''
    eachFromProvider = Window.partitionBy(["fromORGNPINM","fromTHRU_DT_YEAR"])
    eachToProvider = Window.partitionBy(["toORGNPINM","fromTHRU_DT_YEAR"])
    transfersDF = (transfersDF.withColumn("nodeFromEvtOrTpaVol", F.sum( F.col("transferEvtOrTpa") ).over(eachFromProvider))
                              .withColumn("nodeFromEvtOrTpaMean", F.mean( F.col("transferEvtOrTpa") ).over(eachFromProvider))
                              .withColumn("nodeToEvtOrTpaVol", F.sum( F.col("transferEvtOrTpa") ).over(eachToProvider))
                              .withColumn("nodeToEvtOrTpaMean", F.mean( F.col("transferEvtOrTpa") ).over(eachToProvider)))
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

#dyad measures: one value per (sending, receiving) pair and transfer year, on each of its transfers

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

def add_newDyad(transfersDF):
    '''Adds a newDyad column: 1 if this is the earliest year the (fromORGNPINM, toORGNPINM) pair is
    observed in the transfers, 0 otherwise. Partitions on the year-less pair -- not the `dyad` array
    (which bakes in fromTHRU_DT_YEAR, so every dyad would trivially be its own minimum) -- so the flag
    marks the first year the sending->receiving relationship appears. Mirrors add_firstTransfer.
    Note: this is left-censored by the loaded year range (see utilitiesF.yearMin) -- a pair first seen
    in the earliest loaded year is flagged new even if it existed in an unloaded prior year. Also, a
    pair that drops out and returns after a gap is NOT re-flagged new; only its first-ever year is.'''
    eachDyadPair = Window.partitionBy(["fromORGNPINM","toORGNPINM"])
    transfersDF = transfersDF.withColumn("newDyad",
                    (F.col("fromTHRU_DT_YEAR") == F.min(F.col("fromTHRU_DT_YEAR")).over(eachDyadPair)).cast('int'))
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

#prior-year values of the node and dyad measures, all through add_column_prior

def add_column_prior(transfersDF, column, who, when, gapFill=None, observed=None):
    '''Adds the column's value from the prior year at the transfers grain.
    Thin wrapper over utilitiesF.add_column_prior; the shared logic lives in utilities.py.
    Callers pass who/when explicitly. `who` must match the grain of `column`:
    who="fromORGNPINM" for node-level columns keyed on the sending provider (eg nodeHhi,
    nodeOutVol), who="toORGNPINM" for node-level columns keyed on the receiving provider
    (eg nodeInVol), and who=["fromORGNPINM","toORGNPINM"] -- the year-less dyad pair -- for
    dyad-level columns (eg dyadProportionTransfersOut/In) so the prior value comes from the
    same dyad a year earlier rather than an arbitrary partner. when=fromTHRU_DT_YEAR is the
    transfer's originating year -- and it is the right `when` even for columns keyed on the
    receiving provider (nodeInVol), because those are counted per (toORGNPINM,fromTHRU_DT_YEAR)
    too (see add_node_volume_info); `when` must be the year the column was partitioned by or
    `column` would not be constant within each (who,when) group and the max() broadcast below
    would pick an arbitrary one of several values.
    A null in the prior column means unobserved (the node's first observed year), not
    zero -- do not coalesce it to 0 downstream. Pass gapFill=0 only for columns where a >1-year
    gap records a true 0 (see add_column_prior): nodeOutVol and dyadTransferVol (counts) and
    dyadProportionTransfers* (a 0 numerator over the node's out volume). nodeHhi keeps the default None because a gap year
    there is an undefined distribution, not a zero. Dyad-level columns pass observed=_fromOrToNodeSeenBefore(), see
    add_dyad_prior_info.'''
    return utilitiesF.add_column_prior(transfersDF, column=column, who=who, when=when, gapFill=gapFill,
                                       observed=observed)

def add_node_prior_info(transfersDF):
    '''Adds nodeOutVolPrior, nodeInVolPrior and nodeHhiPrior, the node columns of the same provider a year earlier.
    Requires add_node_volume_info and add_nodeHhi.'''
    #each lag keys on the provider its column is counted for, in the transfer year (see add_node_volume_info). gapFill=0
    #for the volumes: a provider-year appears in transfers only when the provider sent (received) at least 1 transfer,
    #so a >1-year gap means it sent (received) zero that year; nodeHhi keeps None, as a gap year there is an undefined
    #distribution, not a zero. The provider's first observed year stays null (unobserved, not zero)
    transfersDF = add_column_prior(transfersDF, column="nodeOutVol", who="fromORGNPINM", when="fromTHRU_DT_YEAR", gapFill=0)
    transfersDF = add_column_prior(transfersDF, column="nodeInVol", who="toORGNPINM", when="fromTHRU_DT_YEAR", gapFill=0)
    transfersDF = add_column_prior(transfersDF, column="nodeHhi", who="fromORGNPINM", when="fromTHRU_DT_YEAR")
    return transfersDF

def _fromOrToNodeSeenBefore():
    '''True where the sending or the receiving provider has a transfer in an earlier year. Requires add_node_prior_info.'''
    #nodeOutVolPrior/nodeInVolPrior are null only in the provider's first observed year (0 after a gap)
    return F.col("nodeOutVolPrior").isNotNull() | F.col("nodeInVolPrior").isNotNull()

def add_dyad_prior_info(transfersDF):
    '''Adds dyadTransferVolPrior, dyadProportionTransfersOutPrior and dyadProportionTransfersInPrior, the dyad columns
    of the same pair a year earlier, 0 when the pair had no transfer that year. Requires add_dyad_info and
    add_node_prior_info.'''
    #gapFill=0: a dyad row exists only when A->B had >=1 transfer that year, so a year without one, after a gap or before
    #the pair's first year, means the pair had zero transfers -- dyadTransferVol is a true 0, and so is the proportions'
    #numerator, i.e. a true 0/nodeOutVol (and add_dyadProportionTransfers* already codes the empty 0/0 case as 0.).
    #A provider of the dyad seen in an earlier year shows that year is in the data; with neither seen it may be outside
    #the loaded years, so the prior stays null. Pass the same observed to any other dyad-level prior
    observed = _fromOrToNodeSeenBefore()
    transfersDF = add_column_prior(transfersDF, column="dyadTransferVol", who=["fromORGNPINM","toORGNPINM"], when="fromTHRU_DT_YEAR", gapFill=0, observed=observed)
    transfersDF = add_column_prior(transfersDF, column="dyadProportionTransfersOut", who=["fromORGNPINM","toORGNPINM"], when="fromTHRU_DT_YEAR", gapFill=0, observed=observed)
    transfersDF = add_column_prior(transfersDF, column="dyadProportionTransfersIn", who=["fromORGNPINM","toORGNPINM"], when="fromTHRU_DT_YEAR", gapFill=0, observed=observed)
    return transfersDF

#assembly: the node and dyad columns get_transfers adds

def add_node_info(transfersDF):
    '''Adds various columns about each node'''
    transfersDF = add_node_volume_info(transfersDF)
    transfersDF = add_node_from_to_info(transfersDF)
    transfersDF = add_node_revenue_info(transfersDF)
    return transfersDF

def add_dyad_info(transfersDF):
    '''Adds various columns about each dyad; add_dyad_prior_info adds their prior-year values.'''
    transfersDF = add_dyad(transfersDF)
    transfersDF = add_dyadVi(transfersDF)
    transfersDF = add_newDyad(transfersDF)
    transfersDF = add_dyadTransferVol(transfersDF)
    transfersDF = add_dyadProportionTransfersOut(transfersDF)
    transfersDF = add_dyadProportionTransfersIn(transfersDF)
    transfersDF = add_dyadAcrossCounties(transfersDF)
    transfersDF = add_dyadAcrossStates(transfersDF)
    return transfersDF

def add_node_and_dyad_info(transfersDF):
    '''Adds various columns about nodes and dyads, and their prior-year values, computed within one transfer set (see
    get_transfers).'''
    transfersDF = add_node_info(transfersDF)
    transfersDF = add_dyad_info(transfersDF)
    #a node column, but from the dyad proportions
    transfersDF = add_nodeHhi(transfersDF)
    transfersDF = add_node_prior_info(transfersDF)
    transfersDF = add_dyad_prior_info(transfersDF)
    return transfersDF

#patient-level columns from each transfer's first inpatient admission

# the side holding the episode's first inpatient admission: an edToInpatient from stay is the ED visit, so the to
# stay; an inpatientToInpatient from stay is already one, so the from stay, which leaves the receiving hospital's
# dgns (they come after it) out of its lookbacks
_FIRST_ADMISSION_SIDE = {TransferType.edToInpatient: "to", TransferType.inpatientToInpatient: "from"}

def _per_transfer_type(columnByType):
    '''Returns a column that is, on each row, the column columnByType ({TransferType: column name}) gives its
    transferType, NULL for another type.'''
    (firstType, firstColumn), *rest = columnByType.items()
    column = F.when(F.col("transferType") == int(firstType), F.col(firstColumn))
    for transferType, c in rest:
        column = column.when(F.col("transferType") == int(transferType), F.col(c))
    return column

def _on_first_admission(transfersDF, columns, caller, add, sources=None):
    '''Returns add(transfersDF) with columns added un-prefixed from each transfer's first inpatient admission
    (see _FIRST_ADMISSION_SIDE) for add to read, and dropped after. sources ({name: {TransferType: transfersDF
    column}}) adds more such columns, or replaces some of these.'''
    if "transferType" not in transfersDF.columns:
        raise ValueError(f"{caller}: needs transferType (see add_transferType) to pick each transfer's first admission")
    # picked per row so one call covers the union of both sets; another transfer type gets NULLs, so NULL outputs
    sources = {**{c: {t: side + c for t, side in _FIRST_ADMISSION_SIDE.items()} for c in columns}, **(sources or {})}
    transfersDF = transfersDF.withColumns({c: _per_transfer_type(columnByType) for c, columnByType in sources.items()})
    return add(transfersDF).drop(*sources)

def add_prior_hospitalization_info(transfersDF, ipBaseDF):
    '''Adds hospitalizationsIn12Months, hospitalizedIn12Months, hospitalizationsIn6Months and
    hospitalizedIn6Months (see baseF.add_prior_hospitalization_info), un-prefixed, for the first inpatient
    admission of each transfer: the to stay of edToInpatient, the from stay of inpatientToInpatient. Works on one
    transfer set or their union. Not needed before add_comorbidity_info, which adds these too.'''
    return _on_first_admission(transfersDF, ["DSYSRTKY", "CLAIMNO", "ffsFirstMonth", "ADMSN_DT_DAY", "ADMSN_DT_MONTH"],
                               "add_prior_hospitalization_info",
                               lambda df: baseF.add_prior_hospitalization_info(df, ipBaseDF, claimType="ip"))

def add_days_at_home_info(transfersDF, snfBaseDF, hhaBaseDF, hospBaseDF, ipBaseDF, lastObservableDay):
    '''For the transfers dataframe, this function adds columns about the number of days the patients stayed home.
    baseF.add_days_at_home_info reads its date/status columns by canonical (un-prefixed) name, so we rename only
    the specific to-side columns it consults, then restore them after the call. Output columns it adds
    (homeDays90, homeDays365, etc.) describe the receiving stay and are kept un-prefixed.
    lastObservableDay is passed through, see baseF.get_homeDays.'''
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
    transfersDF = baseF.add_days_at_home_info(transfersDF, snfBaseDF, hhaBaseDF, hospBaseDF, ipBaseDF, lastObservableDay)
    transfersDF = _rename_columns(transfersDF, {dst: src for src, dst in toToCanonical.items()})
    return transfersDF

def add_comorbidity_info(transfersDF, ipBase, opBase, *, method="Glasheen2019", lookbackFromReceiving=False):
    '''Adds the prior hospitalization columns, the comorbidity condition flags and comorbidityIndex (see
    staysF.add_comorbidity_info), un-prefixed, for the first inpatient admission of each transfer: the to stay of
    edToInpatient, the from stay of inpatientToInpatient. Works on one transfer set or their union.
    lookbackFromReceiving=True makes the inpatientToInpatient flags look back from the to stay's through date,
    as edToInpatient ones do, leaving out the principal dgns of both stays; the prior hospitalization columns
    stay on the from stay.'''
    columns = ["DSYSRTKY", "CLAIMNO", "THRU_DT_DAY", "ffsFirstMonth", "ADMSN_DT_DAY", "ADMSN_DT_MONTH"]
    if not lookbackFromReceiving:
        return _on_first_admission(transfersDF, columns, "add_comorbidity_info",
                                   lambda df: staysF.add_comorbidity_info(df, ipBase, opBase, claimType="ip", method=method))
    ed, ip = TransferType.edToInpatient, TransferType.inpatientToInpatient
    # an ip->ip episode runs from the from admission to the to discharge: the from admission columns keep the prior
    # hospitalizations (and so the comorbidities' coverage gate) on the from stay, the to THRU_DT_DAY moves the dgns
    # lookback, and the principal dgns through from the from discharge on are left out, both stays being for the
    # episode's reason. On ed->ip rows that range is the to discharge alone, as without the flag
    sources = {"THRU_DT_DAY": {ed: "toTHRU_DT_DAY", ip: "toTHRU_DT_DAY"},
               "principalFromDay": {ed: "toTHRU_DT_DAY", ip: "fromTHRU_DT_DAY"}}
    return _on_first_admission(transfersDF, columns, "add_comorbidity_info",
                               lambda df: staysF.add_comorbidity_info(df, ipBase, opBase, claimType="ip", method=method,
                                                                      principalFromDay="principalFromDay"),
                               sources=sources)

def get_lookups(transfersDF, cmsDFS):
    '''Returns cmsUtilitiesF.get_lookups for the patients of transfersDF, to pass to add_prior_hospitalization_info,
    add_comorbidity_info and add_days_at_home_info in place of the full bases.'''
    #get_transfers joins on fromDSYSRTKY==toDSYSRTKY, so either side identifies the patient
    return cmsUtilitiesF.get_lookups(cmsDFS, transfersDF.select(F.col("toDSYSRTKY").alias("DSYSRTKY")))

def add_comorbidity_and_days_at_home_info(transfersDF, lookups, lastObservableDay, *, method="Glasheen2019",
                                          lookbackFromReceiving=False):
    '''Adds add_comorbidity_info (on each transfer's first inpatient admission, lookbackFromReceiving passed to it)
    and add_days_at_home_info (on the to stay) to transfersDF. lookups is get_lookups(transfersDF, cmsDFS), or the
    full bases keyed like cmsDFS.'''
    #both read lookups["ipBase"] and add_days_at_home_info counts over the lookups, so materialize them first
    #(eg utilitiesF.write_and_read_parquet) or each read re-scans the bases
    transfersDF = add_comorbidity_info(transfersDF, lookups["ipBase"], lookups["opBase"], method=method,
                                       lookbackFromReceiving=lookbackFromReceiving)
    transfersDF = add_days_at_home_info(transfersDF, lookups["snfBase"], lookups["hhaBase"], lookups["hospBase"],
                                        lookups["ipBase"], lastObservableDay)
    return transfersDF

#network lists, and joins of provider and dyad tables onto the transfers

def get_edgeList(transfersDF):
    '''Returns the distinct directed edges of the transfer network: one row per unique
    (fromORGNPINM, toORGNPINM) sending/receiving provider pair. Unlike the `dyad` column added by
    add_dyad -- which is the triple [fromORGNPINM, toORGNPINM, fromTHRU_DT_YEAR] -- this edge list is
    year-less: a structural who-transfers-to-whom across all years. Does not modify transfersDF;
    intended to be written out as the edge list for graph analysis.'''
    return transfersDF.select(F.col("fromORGNPINM"), F.col("toORGNPINM")).distinct()

def get_nodeListWithAddress(transfersDF):
    '''Returns the distinct nodes of the transfer network with their address: one row per unique
    (ORGNPINM, providerAddress) pair. A provider can appear on the from side, the to side, or both,
    so both sides are pooled and de-duplicated -- every provider seen in any transfer is represented.
    Pooling via union (rather than exploding an array of [npi, address] pairs) keeps each column's
    own type instead of coercing npi and address to a common element type. Does not modify
    transfersDF; intended to be written out as the node list for graph analysis.'''
    fromNodes = transfersDF.select(F.col("fromORGNPINM").alias("ORGNPINM"),
                                   F.col("fromproviderAddress").alias("providerAddress"))
    toNodes = transfersDF.select(F.col("toORGNPINM").alias("ORGNPINM"),
                                 F.col("toproviderAddress").alias("providerAddress"))
    return fromNodes.union(toNodes).distinct()

def add_dyad_distance_info(transfersDF, nodesDF, pathToData, maxCalls=None, keyFilename=None):
    '''Adds fromproviderLat, fromproviderLng, toproviderLat and toproviderLng from nodesDF (get_nodeListWithAddress then
    geocoding.add_geocode_info(nodesDF, "providerAddress", pathToData, "provider")), dyadGeodesicDistanceKm, and
    dyadDrivingDistanceKm, dyadDrivingTimeMin and dyadDrivingStatus from the Google Routes API.'''
    #transfersDF can be get_edgeList or the transfers themselves: only distinct coordinate pairs are routed, so all the
    #transfers along a dyad share one call; the route is directed, from the sending to the receiving hospital, as A->B and
    #B->A can differ (one way streets, ramps)
    transfersDF = _add_from_and_to_columns(transfersDF, nodesDF.select("ORGNPINM", "providerLat", "providerLng"), ["ORGNPINM"])
    transfersDF = transfersDF.withColumn("dyadGeodesicDistanceKm",
                                         geocodingF.get_geodesicDistanceKm(F.col("fromproviderLat"), F.col("fromproviderLng"),
                                                                           F.col("toproviderLat"), F.col("toproviderLng")))
    transfersDF = geocodingF.add_driving_info(transfersDF, "fromproviderLat", "fromproviderLng", "toproviderLat",
                                              "toproviderLng", pathToData, "dyad", maxCalls=maxCalls, keyFilename=keyFilename)
    return transfersDF

def _add_from_and_to_columns(transfersDF, providerDF, keys):
    '''Joins every column of providerDF but its keys (["ORGNPINM"] or ["ORGNPINM", "THRU_DT_YEAR"]) twice, prefixed from
    and to, onto the sending and the receiving provider of each transfer. Raises ValueError if providerDF lacks a key or
    transfersDF already has a column it would add.'''
    #a THRU_DT_YEAR key is matched to the transfer year on both sides, so the receiving provider is described in the
    #year of the transfer, as in add_providerAnnualStays_info
    transfersKeys = {"ORGNPINM": "{side}ORGNPINM", "THRU_DT_YEAR": "fromTHRU_DT_YEAR"}
    missing = [k for k in keys if k not in providerDF.columns]
    if missing:
        raise ValueError(f"providerDF has no {', '.join(missing)}")
    columns = [c for c in providerDF.columns if c not in keys]
    existing = [side + c for side in ["from", "to"] for c in columns if side + c in transfersDF.columns]
    if existing:
        raise ValueError(f"transfersDF already has {', '.join(existing)}")
    for side in ["from", "to"]:
        #providerDF must hold one row per key or the join would duplicate transfers; a provider table is small, so it is
        #broadcast, and a join condition rather than on=names keeps the keys from moving to the first columns
        sideDF = providerDF.select(*[F.col(k).alias("join" + k) for k in keys], *[F.col(c).alias(side + c) for c in columns])
        condition = reduce(lambda a, b: a & b, [F.col(transfersKeys[k].format(side=side)) == F.col("join" + k) for k in keys])
        transfersDF = (transfersDF.join(F.broadcast(sideDF), on=condition, how="left_outer")
                                  .drop(*["join" + k for k in keys]))
    return transfersDF

def add_provider_annual_info(transfersDF, providerAnnualInfoDF):
    '''Adds every column of providerAnnualInfoDF (one row per ORGNPINM and THRU_DT_YEAR, eg the annual volumes and
    capabilities of the providers from their stays), prefixed from and to, for the sending and the receiving provider in
    the year of the transfer.'''
    return _add_from_and_to_columns(transfersDF, providerAnnualInfoDF, ["ORGNPINM", "THRU_DT_YEAR"])

def add_provider_block_group_info(transfersDF, nodesDF):
    '''Adds fromproviderCensusBlockGroup, fromproviderAdiNatRank, fromproviderAdiNatRankGroup, fromproviderAdiStaRank and
    the same to columns: the census block group of the sending and the receiving provider and its ADI, from nodesDF
    (get_nodeListWithAddress, then geocoding.add_geocode_info, geocoding.add_census_geography_info and
    utilities.add_adi_info, all with prefix "provider").'''
    columns = ["providerCensusBlockGroup", "providerAdiNatRank", "providerAdiNatRankGroup", "providerAdiStaRank"]
    return _add_from_and_to_columns(transfersDF, nodesDF.select("ORGNPINM", *columns), ["ORGNPINM"])

def add_dyad_annual_info(transfersDF, dyadAnnualInfoDF, suffix):
    '''Adds every column of dyadAnnualInfoDF but dyad (one row per dyad, eg the dyad and node columns of another
    transfer set), with suffix appended to its name. Raises ValueError if transfersDF already has any of them.'''
    #suffix names the transfer set the info was computed on, eg "AllTransfers" for all IP->IP transfers when transfersDF
    #is a cohort of them, so the cohort's own dyadTransferVol and the wider dyadTransferVolAllTransfers sit side by side
    if not suffix:
        raise ValueError("suffix must name the transfer set dyadAnnualInfoDF was computed on, eg AllTransfers")
    columns = [c for c in dyadAnnualInfoDF.columns if c != "dyad"]
    existing = [c + suffix for c in columns if c + suffix in transfersDF.columns]
    if existing:
        raise ValueError(f"transfersDF already has {', '.join(existing)}")
    infoDF = dyadAnnualInfoDF.select(F.col("dyad").alias("joinDyad"), *[F.col(c).alias(c + suffix) for c in columns])
    #a join condition rather than on="dyad", which would move dyad to the first column
    return transfersDF.join(infoDF, on=F.col("dyad") == F.col("joinDyad"), how="left_outer").drop("joinDyad")
