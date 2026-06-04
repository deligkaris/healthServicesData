from functools import reduce
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import cms.revenue as revenueF
import cms.claims as claimsF
import cms.comorbidities as comorbiditiesF
import utilities as utilitiesF

def add_providerStrokeVol(staysDF, stroke="anyStroke"):
    '''Calculates the stroke volume, number of rows, for each hospital and year'''
    eachProvider = Window.partitionBy(["ORGNPINM","THRU_DT_YEAR"])
    staysDF = staysDF.withColumn("providerStrokeVol", F.sum( F.col(stroke) ).over(eachProvider))
    return staysDF

def add_providerSepticShockVol(staysDF):
    '''Adds a column with the annual volume of septic shock diagnosis for each organization.'''
    eachProvider = Window.partitionBy(["ORGNPINM","THRU_DT_YEAR"])
    staysDF = staysDF.withColumn("providerSepticShockVol", F.sum( F.col("septicShock") ).over(eachProvider))
    return staysDF

def add_provider_capability_info(staysDF, col="imv"):
    '''Adds a binary column with the capability for each organization regarding the column specified in the function call.
    The column provided must be a binary column as well.'''
    eachProvider = Window.partitionBy(["ORGNPINM","THRU_DT_YEAR"])
    colName = "provider" + col[0].upper() + col[1:] + "Capability"
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
    '''Adds various columns about stroke for each hospital and year.'''
    staysDF = add_provider_stroke_treatment_info(staysDF, inpatient=inpatient)
    staysDF = add_providerStrokeVol(staysDF, stroke=stroke)
    return staysDF

def add_provider_septic_shock_info(staysDF):
    '''Adds columns about septic shock for each hospital and year'''
    staysDF = add_providerSepticShockVol(staysDF)
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
                                    
def add_column_prior(staysDF, column="providerSepticShockVol", who="ORGNPINM", when="THRU_DT_YEAR"):
    '''Adds the column's value from the prior year at the provider (stays) grain.
    Thin wrapper over utilitiesF.add_column_prior with provider-level defaults
    (who=ORGNPINM, when=THRU_DT_YEAR); the shared logic lives in utilities.py.
    A null in the prior column means unobserved (first year or a year gap), not zero --
    do not coalesce it to 0 downstream.'''
    return utilitiesF.add_column_prior(staysDF, column=column, who=who, when=when)
     
def add_orgnpinm_column_prior_year(staysDF, column="providerSepticShockVol"):
    '''For each hospital and year it addes a column of the variable column for that hospital but from the prior year.'''
    return add_column_prior(staysDF, column=column, who="ORGNPINM", when="THRU_DT_YEAR")


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


def add_source_and_destination_info(staysDF, cmsDFS, claimType="ip"):
    '''Adds admissionSource and thruDestination to staysDF by checking, per stay,
    whether the beneficiary has any OTHER stay whose days cover ADMSN_DT_DAY (source)
    or THRU_DT_DAY (destination).

    The per-beneficiary index is built from cmsDFS["ipBase"] (split into ipRehab /
    ipLtc / ipOther by the rehabilitation and ltcHospital flags), cmsDFS["snfBase"],
    cmsDFS["hhaBase"], and cmsDFS["hospBase"] (where "hosp" is hospice). Each source
    is collapsed to one row per stay via _per_stay_index, so each entry carries its
    own stay identity. The current stay is then excluded by comparing otherStayKey
    against the current row's stay-key struct -- without this, every stay would
    trivially "overlap itself" and admissionSource/thruDestination could never be
    "home" for the row's own claim type.

    Resolution priority when multiple settings overlap a day (see
    _SOURCE_DEST_PRIORITY):
        hosp > ipRehab > snf > ipLtc > ipOther > hha
    falling back to "home" when nothing overlaps.

    Assumes:
      * staysDF is at stay granularity (post get_unique_stays) and has DSYSRTKY,
        PROVIDER, ORGNPINM, ADMSN_DT_DAY, THRU_DT_DAY.
      * Every source DF in cmsDFS has losDays (baseF.add_losDays).
      * cmsDFS["ipBase"] has the rehabilitation and ltcHospital flags.
      * claimType selects the stay key used to identify staysDF rows (and therefore
        the self-exclusion key). Defaults to "ip".

    Limitation: day-level overlap is symmetric -- a prior stay that ends on the
    current admission day and a concurrent stay that starts on it are not
    distinguishable here.'''
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

    allDF = (reduce(lambda x, y: x.unionByName(y), sources)
             .groupBy("DSYSRTKY")
             .agg(F.collect_list(F.struct("otherStayKey", "losDays", "claimType"))
                   .alias("otherStays")))

    keys = _stay_keys(claimType)
    current_stay_key = F.struct(F.col(keys[0]).alias("DSYSRTKY"),
                                F.col(keys[1]).alias("PROVIDER"),
                                F.col(keys[2]).alias("ORGNPINM"),
                                F.col(keys[3]).alias("stayDay"))
    not_self = lambda x: x.otherStayKey != current_stay_key

    staysDF = (staysDF
               .join(allDF, on="DSYSRTKY", how="left_outer")
               .withColumn("otherStaysOnAdmission",
                   F.array_distinct(
                       F.filter(F.col("otherStays"),
                                lambda x: not_self(x) &
                                          F.array_contains(x.losDays, F.col("ADMSN_DT_DAY")))
                        .getField("claimType")))
               .withColumn("admissionSource", _resolve_setting("otherStaysOnAdmission"))
               .withColumn("otherStaysOnThru",
                   F.array_distinct(
                       F.filter(F.col("otherStays"),
                                lambda x: not_self(x) &
                                          F.array_contains(x.losDays, F.col("THRU_DT_DAY")))
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
