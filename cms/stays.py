from functools import reduce
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
import pyspark.sql.functions as F
import cms.revenue as revenueF
import cms.claims as claimsF
import cms.comorbidities as comorbiditiesF
import utilities as utilitiesF

def add_providerAnnualCapability(staysDF, col="imv"):
    '''Adds a binary column flagging whether the organization performed `col` at least once in that
    year -- provider<Col>AnnualCapability (e.g. col="imv" -> providerImvAnnualCapability).
    The column provided must be a binary column as well.
    This is a per-year flag: the window partitions on ORGNPINM AND THRU_DT_YEAR, so each year is
    evaluated independently and the flag does NOT carry forward. A hospital that performed `col` in
    one year but not the next gets 1 for the first year's stays and 0 for the next year's -- it is
    not a durable, once-capable-always-capable attribute.'''
    eachProvider = Window.partitionBy(["ORGNPINM","THRU_DT_YEAR"])
    colName = "provider" + col[0].upper() + col[1:] + "AnnualCapability"
    staysDF = staysDF.withColumn(colName, F.max( F.col(col) ).over(eachProvider))
    return staysDF

def add_providerAnnualVolume(staysDF, col="anyStroke"):
    '''Adds a column with the annual volume (sum of `col`) the organization accrued in that year --
    provider<Col>AnnualVolume (e.g. col="anyStroke" -> providerAnyStrokeAnnualVolume).
    The column provided must be numeric (typically a binary flag, so the sum counts stays).
    This is the volume counterpart to add_providerAnnualCapability: same per-year window
    (ORGNPINM AND THRU_DT_YEAR), so each year is evaluated independently and the count does NOT
    carry forward. Call it directly with the flag you want, e.g. col="anyStroke" for stroke
    volume or col="septicShock" for septic-shock volume.'''
    eachProvider = Window.partitionBy(["ORGNPINM","THRU_DT_YEAR"])
    colName = "provider" + col[0].upper() + col[1:] + "AnnualVolume"
    staysDF = staysDF.withColumn(colName, F.sum( F.col(col) ).over(eachProvider))
    return staysDF

def add_providerAnnualStays(staysDF):
    '''Adds providerAnnualStays -- the number of stays the organization had in that year.
    Assumes staysDF is at stay granularity (post get_unique_stays), so each row is one stay and
    the count is a plain row count. Same per-year window as add_providerAnnualVolume
    (ORGNPINM AND THRU_DT_YEAR): each year is evaluated independently and the count does NOT carry
    forward. Unlike add_providerAnnualVolume, which sums a per-stay flag, this counts every stay.'''
    eachProvider = Window.partitionBy(["ORGNPINM","THRU_DT_YEAR"])
    staysDF = staysDF.withColumn("providerAnnualStays", F.count(F.lit(1)).over(eachProvider))
    return staysDF

def add_providerEverCapability(staysDF, col="imv"):
    '''Adds a binary column flagging whether the organization has EVER performed `col`, up to and
    including the current year -- provider<Col>EverCapability (e.g. col="imv" -> providerImvEverCapability).
    The column provided must be a binary column as well.
    This is a forward-propagating flag: the window partitions on ORGNPINM only and orders by
    THRU_DT_YEAR, taking a cumulative max across all years up to the current one. Once a hospital
    performs `col` in any year, every year from then on is flagged 1 -- it never resets, but it does
    NOT propagate backward (years before the first capable year stay 0). A hospital capable in 2018
    but idle in 2019 still gets 1 for 2019's stays. Contrast with add_providerAnnualCapability, which
    evaluates each year independently and does not carry forward.'''
    eachProvider = Window.partitionBy(["ORGNPINM"]).orderBy("THRU_DT_YEAR")
    colName = "provider" + col[0].upper() + col[1:] + "EverCapability"
    staysDF = staysDF.withColumn(colName, F.max( F.col(col) ).over(eachProvider))
    return staysDF

def add_provider_stroke_treatment_info(staysDF, inpatient=True):
    '''For each hospital and year, it adds various columns associated with stroke treatment.
    Outpatient or inpatient stays are different though.'''
    eachProvider = Window.partitionBy(["ORGNPINM","THRU_DT_YEAR"])
    staysDF = (staysDF.withColumn("providerTpaMean", F.mean( F.col("tpa") ).over(eachProvider))
                      .withColumn("providerTpaVol", F.sum( F.col("tpa") ).over(eachProvider)))
    if (inpatient):
        staysDF = (staysDF.withColumn("providerEvtMean", F.mean( F.col("evt") ).over(eachProvider))
                          .withColumn("providerEvtVol", F.sum( F.col("evt") ).over(eachProvider)))
    return staysDF

def add_provider_stroke_info(staysDF, inpatient=True, stroke="anyStroke"):
    '''Adds various columns about stroke for each hospital and year. The volume column is named
    after `stroke` via add_providerAnnualVolume (e.g. stroke="anyStroke" -> providerAnyStrokeAnnualVolume).'''
    staysDF = add_provider_stroke_treatment_info(staysDF, inpatient=inpatient)
    staysDF = add_providerAnnualVolume(staysDF, col=stroke)
    return staysDF

def add_provider_capability_and_volume_info(staysDF, col="imv"):
    '''Adds the three provider-level columns for `col` (a per-stay binary flag), for each hospital
    and year: provider<Col>AnnualCapability (per-year, does not carry forward),
    provider<Col>EverCapability (cumulative, once-capable-always-capable) and
    provider<Col>AnnualVolume (per-year count). E.g. col="evt" -> providerEvtAnnualCapability,
    providerEvtEverCapability, providerEvtAnnualVolume.'''
    staysDF = add_providerAnnualCapability(staysDF, col=col)
    staysDF = add_providerEverCapability(staysDF, col=col)
    staysDF = add_providerAnnualVolume(staysDF, col=col)
    return staysDF

def add_provider_septic_shock_info(staysDF):
    '''Adds columns about septic shock for each hospital and year. The volume column is
    providerSepticShockAnnualVolume, added via add_providerAnnualVolume(col="septicShock").'''
    staysDF = add_providerAnnualVolume(staysDF, col="septicShock")
    return staysDF

def add_provider_revenue_info(staysDF):
    '''Must be called using the revenue summary.'''
    eachProvider = Window.partitionBy(["ORGNPINM","THRU_DT_YEAR"])
    staysDF = (staysDF.withColumn("providerEdMean", F.mean( F.col("ed") ).over(eachProvider))
                      .withColumn("providerEdVol", F.sum( F.col("ed") ).over(eachProvider))
                      .withColumn("providerCtMean", F.mean( F.col("ct") ).over(eachProvider))
                      .withColumn("providerCtVol", F.sum( F.col("ct") ).over(eachProvider))
                      .withColumn("providerMriMean", F.mean( F.col("mri") ).over(eachProvider))
                      .withColumn("providerMriVol", F.sum( F.col("mri") ).over(eachProvider)))
    return staysDF

# Date components added by add_admission_date_info / add_through_date_info /
# add_discharge_date_info / add_death_date_info. They are enricher-added
# numerics that would otherwise satisfy schema-diff, but they MUST NOT be
# propagated across a stay because:
#   * THRU_DT_DAY is the orderBy key for get_unique_stays' dedup. Maxing it
#     collapses the desc(THRU_DT_DAY) clause to a tie, which falls through to
#     asc(CLAIMNO) and picks the interim claim instead of the final discharge
#     -- the exact bug commit 7eb53f1 fixed.
#   * The other date components are identity-bearing per-row data, not flags;
#     max() across the stay would corrupt the surviving row's date metadata.
_DEDUP_BREAKING_DATE_COLUMNS = frozenset({
    "ADMSN_DT_DAY", "ADMSN_DT_YEAR", "ADMSN_DT_MONTH",
    "THRU_DT_DAY", "THRU_DT_YEAR", "THRU_DT_MONTH",
    "DSCHRGDT_DAY", "DSCHRGDT_YEAR",
    "DEATH_DT_DAY",
})


def get_columns_to_propagate(fields, claimType="op", exclude=()):
    '''Returns the names of the columns in `fields` that are safe to max()
    across a stay -- the enricher-added numeric columns minus the date
    components used by the get_unique_stays dedup.

    "Enricher-added" means the column is NOT in the raw
    cms.schemas.schemas[claimType+"Base"] schema, so add_ishStroke /
    add_septicShock / add_anyStroke / revenue summary columns
    (ed/ct/mri/icu) / etc. are all included by construction. Raw CMS fields
    (CLAIMNO, PRNCPAL_DGNS_CD, REV_CNTR, ...) are filtered out because the
    surviving row should keep its own identity.

    Restricting to numeric types avoids erroring on array enricher outputs
    (dgnsCodeAll, prcdrCodeAll, ...).

    Date-derived columns (THRU_DT_DAY, ADMSN_DT_DAY, ...) are always skipped
    -- see _DEDUP_BREAKING_DATE_COLUMNS for the full set and why.

    `fields`: iterable of StructField objects (e.g. claimsDF.schema.fields).
    Passing the schema fields rather than the DataFrame keeps this helper
    pure / independently testable -- and it only needs the names and types.
    `exclude`: optional iterable of additional column names to skip. Use for
    the rare enricher-added numeric where max() is not the right
    aggregation (e.g. a sum-style metric).'''
    from pyspark.sql.types import (
        IntegerType, LongType, ShortType, ByteType, DoubleType, FloatType)
    from cms.schemas import schemas
    raw_cols = set(schemas[claimType + "Base"].fieldNames())
    excluded = _DEDUP_BREAKING_DATE_COLUMNS | set(exclude)
    numeric = (IntegerType, LongType, ShortType, ByteType, DoubleType, FloatType)
    return [f.name for f in fields
            if f.name not in raw_cols
            and f.name not in excluded
            and isinstance(f.dataType, numeric)]


def propagate_stay_info(claimsDF, claimType="op", exclude=()):
    '''For each enricher-added numeric column (see get_columns_to_propagate),
    replace every row's value with max() over the stay's window so that all
    claims belonging to the same stay carry the union of flags before dedup.

    Assumes claimType either op or ip.
    This function has not been tested for use with snf, hha, or hosp claims.'''
    #some claims may have same dsysrtky, provider, npi, admission date, and one of the 2 or more claims will have a null discharge date,
    #perhaps because when the claim was created they did not know the discharge date
    eachIpStay = Window.partitionBy("DSYSRTKY","PROVIDER", "ORGNPINM", "ADMSN_DT_DAY") # "DSCHRGDT_DAY")
    eachOpStay = Window.partitionBy("DSYSRTKY","PROVIDER", "ORGNPINM", "THRU_DT_DAY")
    eachStay = eachIpStay if claimType=="ip" else eachOpStay
    columnsToPropagate = get_columns_to_propagate(claimsDF.schema.fields, claimType=claimType, exclude=exclude)
    for col in columnsToPropagate:
        claimsDF = claimsDF.withColumn(col, F.max(F.col(col)).over(eachStay))
    print("Note: columns aggregated over all claims that comprise a single stay/visit are..."
          + ",".join(columnsToPropagate) + "...")
    return claimsDF

def get_unique_stays(claimsDF, claimType="op"):
    '''A facility stay might be broken in more than one claim.
    This function propagates information from all claims that are probably part of the same facility stay
    and returns a single row that represents more accurately the facility stay.

    The representative row is the claim with the latest THRU_DT_DAY within the stay (i.e. the final
    billed portion of the stay), with CLAIMNO as a deterministic tiebreaker among rows that already
    share THRU_DT_DAY. Selecting by max(THRU_DT_DAY) preserves the true final discharge of the stay
    and avoids the prior bug where min(CLAIMNO) -- a per-year sequence that resets every year -- could
    pick a claim from a different year than the rest of the stay (e.g. interim-billed IP stays where
    the 2021 final bill has a smaller CLAIMNO than the 2017 first bill). The tiebreaker is safe because
    tied rows share THRU_DT_DAY and therefore THRU_DT_YEAR.

    For IP stays (partitioned by ADMSN_DT_DAY) the rule picks the interim claim with the latest
    discharge. For OP stays (partitioned by THRU_DT_DAY) all rows in a partition already share the
    discharge day, so the rule falls through to the CLAIMNO tiebreaker -- safe because they also
    share THRU_DT_YEAR.'''
    claimsDF = propagate_stay_info(claimsDF, claimType=claimType)
    #some claims may have same dsysrtky, provider, npi, admission date, and one of the 2 or more claims will have a null discharge date,
    #perhaps because when the claim was created they did not know the discharge date
    eachIpStay = Window.partitionBy("DSYSRTKY","PROVIDER", "ORGNPINM", "ADMSN_DT_DAY") #, "DSCHRGDT_DAY")
    eachOpStay = Window.partitionBy("DSYSRTKY","PROVIDER", "ORGNPINM", "THRU_DT_DAY")
    eachStay = eachIpStay if claimType=="ip" else eachOpStay
    #latest THRU first so the surviving row's discharge is the true final discharge of the stay;
    #asc(CLAIMNO) breaks ties only among rows that share THRU_DT_DAY (and therefore the same year)
    eachStayOrdered = eachStay.orderBy(F.desc("THRU_DT_DAY"), F.asc("CLAIMNO"))
    staysDF = (claimsDF.withColumn("stayRowNumber", F.row_number().over(eachStayOrdered))
                        .filter(F.col("stayRowNumber") == 1)
                        .drop("stayRowNumber"))
    return staysDF

def get_stays(baseDF, summaryDF, claimType="op", opBase=None, opRevenue=None): 
    '''Inputs are the claims, base and revenue summary, whereas outputs are the stays/visits.
    Each stay/visit might include more then a single claim, so this function attempts to create the dataframe of stay/visits from the claim information.'''
    claimsDF = claimsF.get_claims(baseDF, summaryDF)
    if (opBase is not None) & (opRevenue is not None):
        claimsDF = claimsF.update_nonPPS_revenue_info(claimsDF, opBase, opRevenue)
    staysDF = get_unique_stays(claimsDF, claimType=claimType)
    return staysDF

def get_admittedStays(staysDF, transfersDF):
    '''Returns staysDF (inpatient) with the transferred-in admissions removed -- the directly-admitted
    patients, i.e. the ip stays that were NOT the receiving (to) side of any transfer. This is the
    population add_providerAnnualStays should count so that providerAnnualStays is disjoint from the
    transfers, which transfersF.add_providerAnnualStays_info / add_nodeProportionTransferred* assume.

    Removal is a left_anti join on the ip stay identity (DSYSRTKY, ORGNPINM, ADMSN_DT_DAY) against the
    to-prefixed columns of transfersDF (get_transfers prefixes every to-stays column with "to"). For
    an outpatient->inpatient transfer only the to (receiving) stay is an inpatient admission, so only
    the to side is matched -- the from side is an op visit with no admission date and is not part of
    an inpatient stays df. A stay is dropped iff its (DSYSRTKY, ORGNPINM, ADMSN_DT_DAY) equals the
    receiving admission of some transfer.'''
    transferStays = transfersDF.select(
        F.col("toDSYSRTKY").alias("DSYSRTKY"),
        F.col("toORGNPINM").alias("ORGNPINM"),
        F.col("toADMSN_DT_DAY").alias("ADMSN_DT_DAY")).distinct()
    staysDF = staysDF.join(transferStays, on=["DSYSRTKY", "ORGNPINM", "ADMSN_DT_DAY"], how="left_anti")
    return staysDF

def add_onDayOfFirstStay(staysDF):
    '''Finds the first admission date for each beneficiary in the dataframe.'''
    eachDsysrtky = Window.partitionBy("DSYSRTKY")
    staysDF = staysDF.withColumn("onDayOfFirstStay", ( F.col("ADMSN_DT_DAY")==F.min(F.col("ADMSN_DT_DAY")).over(eachDsysrtky) ).cast('int'))
    return staysDF

def add_onDayOfFirstStaySum(staysDF):
    eachDsysrtky=Window.partitionBy("DSYSRTKY")
    staysDF = (staysDF.withColumn("onDayOfFirstStaySum", F.sum(F.col("onDayOfFirstStay")).over(eachDsysrtky)))
    return staysDF

def add_first_stay_info(staysDF):
    '''Column firstStay marks as 1 the stays that were definitely the first stay for that beneficiary and as 0 all other stays,
    including the ones for which we cannot know with this data.'''
    eachDay = Window.partitionBy(["DSYSRTKY","ADMSN_DT"])
    staysDF = add_onDayOfFirstStay(staysDF)
    staysDF = add_onDayOfFirstStaySum(staysDF)
    staysDF = (staysDF.withColumn( "singleDayStay", (F.col("los")==1).cast('int') )
                      .withColumn( "singleDayStaySum", F.sum( F.col("singleDayStay") ).over(eachDay) ) #how many admissions with los=1 on that day
                      .withColumn( "firstStay", F.when( (F.col("onDayOfFirstStay")==1) & (F.col("onDayOfFirstStaySum")==1), 1 ) #definitely first stay
                                                 .when( (F.col("onDayOfFirstStay")==1) & (F.col("onDayOfFirstStaySum")>1) &  #definitely first stay
                                                        (F.col("singleDayStay")==1) & (F.col("singleDayStaySum")==1), 1 ) #since discharged from los=1 first
                                                 .otherwise(0) )) #the value of 0 includes both true 'not first admissions' but also 'impossible to know if first admission'
    return staysDF
                                    
def add_column_prior(staysDF, column="providerSepticShockAnnualVolume", who="ORGNPINM", when="THRU_DT_YEAR", gapFill=None):
    '''Adds the column's value from the prior year at the provider (stays) grain.
    Thin wrapper over utilitiesF.add_column_prior with provider-level defaults
    (who=ORGNPINM, when=THRU_DT_YEAR); the shared logic lives in utilities.py.
    A null in the prior column means unobserved -- the provider's first observed year,
    where we cannot tell "no activity" from "not yet in the data". For a >1-year gap the
    provider provably existed (it bills on both sides), so the missing year had a real
    volume of 0: pass gapFill=0 for count columns (the provider*AnnualVolume columns)
    to record that. Leave gapFill=None (the default) for proportion/index columns, where a
    gap year is undefined rather than zero. Do not coalesce first-year nulls to 0 downstream.'''
    return utilitiesF.add_column_prior(staysDF, column=column, who=who, when=when, gapFill=gapFill)
     
def add_orgnpinm_column_prior_year(staysDF, column="providerSepticShockAnnualVolume", gapFill=None):
    '''For each hospital and year it addes a column of the variable column for that hospital but from the prior year.
    Pass gapFill=0 for count/volume columns so a >1-year gap records a true 0 rather than null
    (see add_column_prior); leave gapFill=None for proportion/index columns (e.g. provider*Mean).'''
    return add_column_prior(staysDF, column=column, who="ORGNPINM", when="THRU_DT_YEAR", gapFill=gapFill)


def _stay_keys(claimType):
    '''Stay-identity columns for a given claim type. Matches the partitioning used by
    propagate_stay_info / get_unique_stays so a "stay" means the same thing across this
    module. IP stays span multiple interim claims that share ADMSN_DT_DAY; the other
    claim types are keyed on THRU_DT_DAY.'''
    if claimType == "ip":
        return ["DSYSRTKY", "PROVIDER", "ORGNPINM", "ADMSN_DT_DAY"]
    return ["DSYSRTKY", "PROVIDER", "ORGNPINM", "THRU_DT_DAY"]


def _per_stay_index(baseDF, claimType, label):
    '''Reduces a base claim DF to one row per stay for the source/destination index.
    Carries (a) the union of losDays across the stay's constituent claims and (b) a
    stay-identity struct used by add_source_and_destination_info for self-exclusion.
    Deliberately does NOT call get_unique_stays: the index only needs the days covered
    and a stable stay key, so groupBy + array_distinct(flatten(...)) is sufficient and
    skips the propagate_stay_info + row_number pass. Requires losDays on the input
    (see baseF.add_losDays).

    The 4th stay key (ADMSN_DT_DAY for ip, THRU_DT_DAY otherwise) is aliased to a
    uniform "stayDay" field inside otherStayKey so the six per-source indices share a
    struct schema and can be unionByName'd.'''
    keys = _stay_keys(claimType)
    return (baseDF.groupBy(*keys)
                  .agg(F.array_distinct(F.flatten(F.collect_list("losDays"))).alias("losDays"))
                  .select("DSYSRTKY",
                          F.struct(F.col(keys[0]).alias("DSYSRTKY"),
                                   F.col(keys[1]).alias("PROVIDER"),
                                   F.col(keys[2]).alias("ORGNPINM"),
                                   F.col(keys[3]).alias("stayDay")).alias("otherStayKey"),
                          "losDays",
                          F.lit(label).alias("claimType")))


_SOURCE_DEST_PRIORITY = ("hosp", "ipRehab", "snf", "ipLtc", "ipOther", "hha")


def _resolve_setting(otherCol):
    '''Reduces the array of overlapping claim-type labels to a single label using the
    fixed priority order in _SOURCE_DEST_PRIORITY. Returns "home" when no other stay
    overlaps the day.'''
    expr = F.when((F.col(otherCol).isNull()) | (F.size(F.col(otherCol)) == 0), F.lit("home"))
    for label in _SOURCE_DEST_PRIORITY:
        expr = expr.when(F.array_contains(F.col(otherCol), label), F.lit(label))
    return expr


def get_otherStays(cmsDFS, persistFlag=False, persistLocation=StorageLevel.MEMORY_AND_DISK):
    '''Returns one row per beneficiary with an otherStays array holding every stay they had in any
    setting: cmsDFS["ipBase"] (split into ipRehab / ipLtc / ipOther by the rehabilitation and
    ltcHospital flags), cmsDFS["snfBase"], cmsDFS["hhaBase"] and cmsDFS["hospBase"] (hospice). Each
    source is collapsed to one row per stay via _per_stay_index, so every entry carries its own stay
    identity (otherStayKey) and the days it covered (losDays).

    This is the index add_source_and_destination_info probes to decide what a stay was admitted from
    and discharged to. It depends only on cmsDFS -- not on staysDF and not on claimType -- so it is
    the same index for every stays dataframe of a project. Building it aggregates the whole ip + snf +
    hha + hospice base, so a project that calls add_source_and_destination_info more than once (e.g.
    once for the sending stays and once for the receiving stays of a transfer) should build it once
    here and pass it to each call as otherStaysDF rather than have every call rebuild it.

    persistFlag: persist the index before returning it, for exactly that case -- it is then computed
    once and read from the cache by every call it is passed to. No count() is done to force it: the
    cache fills on the first action that needs it anyway, and forcing it here would only add a spark
    job. The index holds one array per beneficiary of every stay they ever had, with the days each
    covered, so it is wider than its row count suggests; MEMORY_AND_DISK spills rather than fails,
    but check the storage tab if it spills heavily -- the cache can then cost more than the rebuild.

    Assumes every source DF in cmsDFS has losDays (baseF.add_losDays), and that cmsDFS["ipBase"] has
    the rehabilitation and ltcHospital flags.'''
    ip = cmsDFS["ipBase"]
    sources = [
        _per_stay_index(ip.filter(F.col("rehabilitation") == 1), "ip", "ipRehab"),
        _per_stay_index(ip.filter(F.col("ltcHospital") == 1),    "ip", "ipLtc"),
        _per_stay_index(ip.filter((F.col("rehabilitation") == 0) &
                                  (F.col("ltcHospital") == 0)),  "ip", "ipOther"),
        _per_stay_index(cmsDFS["snfBase"],  "snf",  "snf"),
        _per_stay_index(cmsDFS["hhaBase"],  "hha",  "hha"),
        _per_stay_index(cmsDFS["hospBase"], "hosp", "hosp"),
    ]
    otherStaysDF = (reduce(lambda x, y: x.unionByName(y), sources)
                    .groupBy("DSYSRTKY")
                    .agg(F.collect_list(F.struct("otherStayKey", "losDays", "claimType"))
                          .alias("otherStays")))
    if (persistFlag):
        otherStaysDF.persist(persistLocation)
    return otherStaysDF


def add_source_and_destination_info(staysDF, cmsDFS, claimType="ip", otherStaysDF=None):
    '''Adds admissionSource and thruDestination to staysDF by checking, per stay,
    whether the beneficiary has any OTHER stay whose days cover the stay's source
    window or its destination window.

    The rule depends on claimType:
      * ip (and any type with a real admission date): source = the single day ADMSN_DT_DAY, destination =
        the single day THRU_DT_DAY. An other stay counts when its losDays contains that day. This is the
        original behavior.
      * op: a visit is a single day (its through date, THRU_DT_DAY) with no admission date. An other stay
        counts as the source only if its losDays contains BOTH THRU_DT_DAY - 1 AND THRU_DT_DAY -- i.e. it
        spans continuously from the day before into the ED day, so the patient was in that setting right up
        to the visit. Symmetrically it counts as the destination only if its losDays contains BOTH
        THRU_DT_DAY AND THRU_DT_DAY + 1 -- it covers the ED day and continues past it. A stay that merely
        ends the day before (lacks THRU_DT_DAY) or merely begins the day after (lacks THRU_DT_DAY) is NOT
        counted. Because the two conditions look in opposite directions, admissionSource and thruDestination
        can differ for op. op stays need no ADMSN_DT_DAY column.

    The per-beneficiary index of those other stays comes from get_otherStays (see there for how it is
    built). The current stay is then excluded by comparing otherStayKey against the current row's
    stay-key struct -- without this, every stay would trivially "overlap itself" and
    admissionSource/thruDestination could never be "home" for the row's own claim type. (op is not in
    the index, so an op staysDF never self-collides; the exclusion still matters when staysDF is one of
    the indexed types.)

    Resolution priority when multiple settings overlap a day (see
    _SOURCE_DEST_PRIORITY):
        hosp > ipRehab > snf > ipLtc > ipOther > hha
    falling back to "home" when nothing overlaps.

    Assumes:
      * staysDF is at stay granularity (post get_unique_stays) and has DSYSRTKY, PROVIDER, ORGNPINM,
        THRU_DT_DAY, and -- for ip -- ADMSN_DT_DAY.
      * Every source DF in cmsDFS has losDays (baseF.add_losDays).
      * cmsDFS["ipBase"] has the rehabilitation and ltcHospital flags.
      * claimType selects the stay key used to identify staysDF rows (and therefore
        the self-exclusion key) and the admission-day column. Defaults to "ip".
      * otherStaysDF (optional) is a prebuilt index from get_otherStays. Pass it when
        this function is called more than once in a project, so the index -- an
        aggregation over the whole ip/snf/hha/hospice base -- is built once instead of
        once per call. When it is None the index is built here from cmsDFS, which is
        the previous behavior; cmsDFS is then the only source of the index and is
        otherwise unused.

    Limitation: for ip, day-level overlap is symmetric -- a prior stay that ends on the
    current admission day and a concurrent stay that starts on it are not
    distinguishable here. op sidesteps this by widening each side toward its own edge.

    Note on the get_unique_stays dedup: admissionSource/thruDestination are added here,
    AFTER the dedup, and propagate_stay_info would skip them anyway (it only max()es
    numeric columns, and these are strings). That is fine for ip: the source lookup is
    keyed on ADMSN_DT_DAY, which is the ip stay partition key, so every claim of the stay
    would resolve the same admissionSource and the surviving row's value is the stay's
    value. For op the windows are keyed on THRU_DT_DAY, the op stay partition key, so the
    same holds.'''
    otherStaysDF = get_otherStays(cmsDFS) if otherStaysDF is None else otherStaysDF

    keys = _stay_keys(claimType)
    current_stay_key = F.struct(F.col(keys[0]).alias("DSYSRTKY"),
                                F.col(keys[1]).alias("PROVIDER"),
                                F.col(keys[2]).alias("ORGNPINM"),
                                F.col(keys[3]).alias("stayDay"))
    not_self = lambda x: x.otherStayKey != current_stay_key

    #the source (admission) and destination (through) conditions on another stay's losDays. op has no
    #admission date and is a single-day visit, so a stay qualifies only if it spans continuously across the
    #boundary -- both the day before and the ED day for a source, both the ED day and the day after for a
    #destination (see docstring). everything else tests its single real admission/through day.
    if claimType == "op":
        thru = F.col("THRU_DT_DAY")
        onAdmission = lambda x: F.array_contains(x.losDays, thru - 1) & F.array_contains(x.losDays, thru)
        onThru      = lambda x: F.array_contains(x.losDays, thru) & F.array_contains(x.losDays, thru + 1)
    else:
        onAdmission = lambda x: F.array_contains(x.losDays, F.col("ADMSN_DT_DAY"))
        onThru      = lambda x: F.array_contains(x.losDays, F.col("THRU_DT_DAY"))

    staysDF = (staysDF
               .join(otherStaysDF, on="DSYSRTKY", how="left_outer")
               .withColumn("otherStaysOnAdmission",
                   F.array_distinct(
                       F.filter(F.col("otherStays"),
                                lambda x: not_self(x) & onAdmission(x))
                        .getField("claimType")))
               .withColumn("admissionSource", _resolve_setting("otherStaysOnAdmission"))
               .withColumn("otherStaysOnThru",
                   F.array_distinct(
                       F.filter(F.col("otherStays"),
                                lambda x: not_self(x) & onThru(x))
                        .getField("claimType")))
               .withColumn("thruDestination", _resolve_setting("otherStaysOnThru")))
    return staysDF


def add_comorbidity_info(staysDF, ipBase, opBase, claimType="ip", method="Glasheen2019"):
    '''Adds the comorbidity condition flags (myocardialInfraction, congestiveHeartFailure, ...),
    aids, and comorbidityIndex to staysDF.

    Requires staysDF to have DSYSRTKY, THRU_DT_DAY, and hospitalizationsIn12Months. The third
    is added by baseF.add_prior_hospitalization_info (or transfersF.add_prior_hospitalization_info
    for dyad tables) and must be present on staysDF before calling.

    The day-level dgns indexes are built inline from ipBase/opBase via comorbiditiesF.get_dayDgnsDF.
    If add_comorbidity_info is called multiple times in the same script, prefer building the
    indexes once and calling comorbiditiesF.get_conditions directly to avoid duplicated work.

    claimType ("ip" or "op"): passed through to comorbiditiesF.get_conditions. "ip" excludes the
    IP principal dgns falling on the stay's THRU_DT_DAY (the admission reason isn't a comorbidity
    of itself). For "op", no principal exclusion is applied, so an incidental same-day IP discharge
    counts as a real comorbidity for the index OP visit.'''
    opDayDgnsDF = comorbiditiesF.get_dayDgnsDF(opBase)
    ipDayDgnsDF = comorbiditiesF.get_dayDgnsDF(ipBase)
    conditions = comorbiditiesF.get_conditions(staysDF, opDayDgnsDF, ipDayDgnsDF,
                                               claimType=claimType, method=method)
    staysDF = staysDF.join(conditions,
                           on=["DSYSRTKY", "THRU_DT_DAY", "hospitalizationsIn12Months"],
                           how="left_outer")
    return staysDF
