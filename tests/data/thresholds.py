'''Floors for the rates the data tests measure, and the known exceptions to their hard checks.
Set each from the metrics table a run prints (its check and key columns name the entry), with a # reason.'''

#minimum rate, keyed "<check> <key>" as the metrics table prints them; a rate with no entry must only be >= 0
rateFloors = {}

def get_floor(check, key):
    return rateFloors.get(f"{check} {key}", 0.0)

#flag prevalence may change by at most this factor from one year to the next (test_cms_values.py)
prevalenceYearRatio = 3.0

#(claimTypePart, year) -> {column: rows allowed to turn null when cast to their schema type}
#from the notes at the top of cms/schemas.py
castLossAllowed = {("carLine", 2016): {"SRVC_CNT": 4},
                   ("hhaBase", 2015): {"ORGNPINM": 1},
                   ("hhaBase", 2016): {"ORGNPINM": 1},
                   ("hospBase", 2016): {"ORGNPINM": 1},
                   ("snfBase", 2015): {"ORGNPINM": 2, "AT_NPI": 1}}

#claimTypePart -> schema columns allowed in some loaded years and not others (eg a J/K format difference)
columnsMissingSomeYears = {}

#(source, column) -> rows allowed not to match the key format (test_external.py)
keyFormatExceptions = {}

#claimTypePart -> added columns allowed to be all null in a year (test_pipeline.py)
nullableAddedColumns = {}

#added columns allowed to break the lower case first rule (test_pipeline.py)
namingExceptions = {"GNRL_CNTL_TYPE_CD",              #a raw POS field, joined by base.add_provider_pos_info
                    #the mortality flags start with their window, renaming them would break the analysis code
                    "30DaysAfterAdmissionDateDead", "90DaysAfterAdmissionDateDead", "365DaysAfterAdmissionDateDead",
                    "90DaysAfterThroughDateDead", "365DaysAfterThroughDateDead"}
