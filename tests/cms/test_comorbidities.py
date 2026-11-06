'''cms/comorbidities.py: the code lists, the inpatient and outpatient rules, the lookback, the hierarchies and the index.
add_comorbidity_info's own wiring is in test_stays.py.'''
import re

import pytest
from pyspark.sql.types import IntegerType, StructField, StructType

import cms.comorbidities as comorbiditiesF
from cms.schemas import schemas
from utilities import icd10Day

#known ICD-10 code -> the conditions it belongs to; the code lists overlap on purpose in a few
glasheenCodes = {
    "I219": {"myocardialInfraction"}, "I509": {"congestiveHeartFailure"}, "I739": {"peripheralVascular"},
    "I639": {"cerebrovascular"}, "G309": {"dementia"}, "J449": {"chronicPulmonary"}, "M069": {"rheumatic"},
    "K259": {"pepticUlcer"}, "K7460": {"liverMild"}, "E119": {"diabetesWithoutCC"}, "N184": {"renalMild"},
    "E1122": {"diabetesWithCC"}, "G8190": {"hemiplegia"}, "C349": {"malignancy"}, "K7290": {"liverSevere"},
    "N186": {"renalSevere"}, "B20": {"hiv"}, "C7951": {"metastaticSolidTumor"}, "B3781": {"infectionOrCancerDueToAids"},
    "I130": {"congestiveHeartFailure", "renalMild"}, "I132": {"congestiveHeartFailure", "renalSevere"},
    "C8190": {"malignancy", "infectionOrCancerDueToAids"}}
quanCodes = {
    "I219": {"myocardialInfraction"}, "I509": {"congestiveHeartFailure"}, "I739": {"peripheralVascular"},
    "I639": {"cerebrovascular"}, "G309": {"dementia"}, "J449": {"chronicPulmonary"}, "M069": {"rheumatic"},
    "K259": {"pepticUlcer"}, "K7460": {"mildLiver"}, "E119": {"diabetesWithoutCC"}, "N184": {"renal"},
    "E1122": {"diabetesWithCC"}, "G8190": {"hemiplegia"}, "C349": {"malignancy"}, "K7290": {"severeLiver"},
    "C7951": {"metastaticSolidTumor"}, "B20": {"hivAids"}}
#not a comorbidity in either list: hypertension, a routine exam, Parkinson's, angina
noCondition = ["I10", "Z0000", "G20", "I209"]

@pytest.mark.parametrize("method,codes", [("Glasheen2019", glasheenCodes), ("Quan2005", quanCodes)], ids=["Glasheen", "Quan"])
def test_codes_find_their_conditions(method, codes):
    patterns = comorbiditiesF.get_codes_regexp(comorbiditiesF.get_codes(method))
    #a new condition needs a code here
    assert set().union(*codes.values()) == set(patterns)
    found = {code: {c for c, pattern in patterns.items() if re.search(pattern, code)} for code in [*codes, *noCondition]}
    assert found == {**codes, **{code: set() for code in noCondition}}

def test_codes_regexp_match_the_start_of_a_code():
    assert comorbiditiesF.get_codes_regexp({"condition": ["I21X", "C0XX", "I252"]}) == {"condition": "^I21|^C0|^I252"}

def test_day_dgns_keeps_one_row_per_beneficiary_with_the_codes_it_has(spark):
    schema = StructType(schemas["ipBase"].fields + [StructField("THRU_DT_DAY", IntegerType())])
    rows = [{"DSYSRTKY": 1, "THRU_DT_DAY": 10, "ICD_DGNS_CD1": "I219", "ICD_DGNS_CD25": "E119"},
            {"DSYSRTKY": 1, "THRU_DT_DAY": 20, "ICD_DGNS_CD2": "N184"}]
    base = spark.createDataFrame([{name: row.get(name) for name in schema.fieldNames()} for row in rows], schema=schema)
    result = comorbiditiesF.get_dayDgnsDF(base).collect()
    assert len(result) == 1
    assert sorted(tuple(s) for s in result[0]["dayDgnsStruct"]) == [(10, "E119", 0), (10, "I219", 1), (20, "N184", 0)]

@pytest.mark.parametrize("dgnsList,inpatient,expected", [
    (["I219"], True, 1),
    ([], True, 0),
    (["I219"], False, 0),
    (["I219", "I219"], False, 1),
    #outpatient needs the same code twice, two codes of one condition are not enough
    (["I219", "I221"], False, 0),
    ([], False, 0)])
def test_inpatient_needs_one_code_outpatient_the_same_code_twice(spark, dgnsList, inpatient, expected):
    dgns = spark.createDataFrame([(1, dgnsList)], "DSYSRTKY int, dgnsList array<string>")
    patterns = {"myocardialInfraction": "^I21|^I22"}
    result = comorbiditiesF.get_conditions_from_dgnsDF(dgns, list(patterns), patterns, inpatient=inpatient)
    assert result.columns == ["DSYSRTKY", "myocardialInfraction"]
    assert result.first()["myocardialInfraction"] == expected

#late enough for a full lookback in the ICD-10 era
stayDay = icd10Day + 1000

def get_conditions(spark, ip=(), op=(), day=stayDay, hospitalizations=0, **options):
    '''The row of a stay on day of beneficiary 1, whose ip and op dgns are (thruDay, dgnsCode, principal).'''
    stays = spark.createDataFrame([(1, day, hospitalizations)], "DSYSRTKY int, THRU_DT_DAY int, hospitalizationsIn12Months int")
    def make_dgns(events):
        return spark.createDataFrame([(1, list(events))],
                                      "DSYSRTKY int, dayDgnsStruct array<struct<thruDay:int,dgnsCode:string,principal:int>>")
    result = comorbiditiesF.get_conditions(stays, make_dgns(op), make_dgns(ip), **options).collect()
    assert len(result) == 1
    return result[0]

def ip_codes(*codes):
    return [(stayDay - 10, code, 0) for code in codes]

def test_outpatient_codes_count_when_repeated(spark):
    assert get_conditions(spark, op=[(stayDay - 10, "I219", 0)])["myocardialInfraction"] == 0
    assert get_conditions(spark, op=[(stayDay - 10, "I219", 0), (stayDay - 20, "I219", 0)])["myocardialInfraction"] == 1

@pytest.mark.parametrize("thruDay,expected", [(stayDay, 1), (stayDay - 360, 1), (stayDay - 361, 0), (stayDay + 1, 0)])
def test_lookback_is_the_360_days_up_to_the_stay(spark, thruDay, expected):
    assert get_conditions(spark, ip=[(thruDay, "I219", 0)])["myocardialInfraction"] == expected

@pytest.mark.parametrize("event,claimType,expected", [
    ((stayDay, "I219", 1), "ip", 0),
    ((stayDay, "I219", 0), "ip", 1),
    ((stayDay - 1, "I219", 1), "ip", 1),
    #an ip discharge on the day of an op visit is not the reason for the visit
    ((stayDay, "I219", 1), "op", 1)],
    ids=["principalThatDay", "secondaryThatDay", "principalEarlier", "opStay"])
def test_reason_for_the_stay_is_not_a_comorbidity(spark, event, claimType, expected):
    assert get_conditions(spark, ip=[event], claimType=claimType)["myocardialInfraction"] == expected

def test_principalFromDay_widens_the_reason_to_the_whole_stay(spark):
    stays = spark.createDataFrame([(1, stayDay, 0, stayDay - 5)],
                                  "DSYSRTKY int, THRU_DT_DAY int, hospitalizationsIn12Months int, principalFrom int")
    def make_dgns(events):
        return spark.createDataFrame([(1, events)],
                                     "DSYSRTKY int, dayDgnsStruct array<struct<thruDay:int,dgnsCode:string,principal:int>>")
    ip = make_dgns([(stayDay - 5, "I219", 1), (stayDay - 6, "I509", 1)])
    result = comorbiditiesF.get_conditions(stays, make_dgns([]), ip, principalFromDay="principalFrom").first()
    assert (result["myocardialInfraction"], result["congestiveHeartFailure"]) == (0, 1)
    with pytest.raises(ValueError):
        comorbiditiesF.get_conditions(stays, make_dgns([]), ip, claimType="op", principalFromDay="principalFrom")

@pytest.mark.parametrize("mildCode,severeCode,mild,severe,index", [
    ("I639", "G8190", "cerebrovascular", "hemiplegia", 2),
    ("K7460", "K7290", "liverMild", "liverSevere", 3),
    ("E119", "E1122", "diabetesWithoutCC", "diabetesWithCC", 2),
    ("N184", "N186", "renalMild", "renalSevere", 3),
    ("C349", "C7951", "malignancy", "metastaticSolidTumor", 6)])
def test_only_the_severe_condition_counts(spark, mildCode, severeCode, mild, severe, index):
    result = get_conditions(spark, ip=ip_codes(mildCode, severeCode))
    assert (result[mild], result[severe], result["comorbidityIndex"]) == (0, 1, index)

@pytest.mark.parametrize("codes,hiv,aids,index", [
    (["B20"], 1, 0, 3),
    (["B20", "B3781"], 0, 1, 6),
    (["B3781"], 0, 0, 0)],
    ids=["hiv", "aids", "infectionAlone"])
def test_aids_is_hiv_with_an_infection_or_cancer(spark, codes, hiv, aids, index):
    result = get_conditions(spark, ip=ip_codes(*codes))
    assert (result["hiv"], result["aids"], result["comorbidityIndex"]) == (hiv, aids, index)
    assert "infectionOrCancerDueToAids" not in result.asDict()

def test_index_adds_the_conditions(spark):
    assert get_conditions(spark, ip=ip_codes("I219", "I509", "E1122", "N186"))["comorbidityIndex"] == 1 + 1 + 2 + 3

@pytest.mark.parametrize("day,hospitalizations,known", [
    (stayDay, None, False),
    (icd10Day + 359, 0, False),
    (icd10Day - 10, 0, False),
    (icd10Day + 360, 0, True)],
    ids=["noFullYearOfFfs", "dayBeforeFullIcd10Year", "icd9", "fullIcd10Year"])
def test_conditions_are_null_without_a_full_lookback(spark, day, hospitalizations, known):
    result = get_conditions(spark, ip=[(day - 1, "I219", 0)], day=day, hospitalizations=hospitalizations)
    assert result["myocardialInfraction"] == (1 if known else None)
    assert result["comorbidityIndex"] == (1 if known else None)

#the Glasheen2019 points: its text gives 11 of the 19 conditions 1 point, diabetes 1 or 2, renal 1 or 3, HIV 3 and AIDS 6
#https://pmc.ncbi.nlm.nih.gov/articles/PMC6684052/
weights = {"myocardialInfraction": 1, "congestiveHeartFailure": 1, "peripheralVascular": 1, "cerebrovascular": 1, "dementia": 1,
           "chronicPulmonary": 1, "rheumatic": 1, "pepticUlcer": 1, "liverMild": 1, "diabetesWithoutCC": 1, "renalMild": 1,
           "diabetesWithCC": 2, "hemiplegia": 2, "malignancy": 2, "renalSevere": 3, "liverSevere": 3, "hiv": 3,
           "metastaticSolidTumor": 6, "aids": 6}

@pytest.mark.parametrize("condition", weights)
def test_index_points(spark, condition):
    columns = list(weights)
    conditions = spark.createDataFrame([tuple(int(c == condition) for c in columns)], columns)
    assert comorbiditiesF.get_comorbidityIndex(conditions).first()["comorbidityIndex"] == weights[condition]

def test_index_points_cover_the_conditions():
    #hiv and aids come from infectionOrCancerDueToAids, the rest of the code list has a weight
    assert set(comorbiditiesF.get_codes()) - {"infectionOrCancerDueToAids"} | {"aids"} == set(weights)

def test_index_is_null_for_quan(spark):
    #only the Glasheen2019 index is implemented
    conditions = spark.createDataFrame([(1,)], ["hiv"])
    assert comorbiditiesF.get_comorbidityIndex(conditions, method="Quan2005").first()["comorbidityIndex"] is None
