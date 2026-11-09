'''Floors for the rates the data tests measure, and the known exceptions to their hard checks.
Set each from the metrics table a run prints (its check and key columns name the entry), with a # reason.'''

#minimum rate, keyed "<check> <key>" as the metrics table prints them; a rate with no entry must only be >= 0
rateFloors = {}

def get_floor(check, key):
    return rateFloors.get(f"{check} {key}", 0.0)

#(low, high) of each comparison of test_benchmarks.py, keyed "<check> <key>" as the metrics table prints them (less its
#lowest/highest). Provisional until the first OSC run: set each from the lowest and highest it prints, with a # reason,
#after looking into anything far off rather than widening the band around it
benchmarkBands = {
    #the Original Medicare population rebuilt from the mbsf against BENES_OM_CNT, 100% of the mbsf
    "geographic variation omCountRatio": (0.98, 1.02),
    #BENE_AVG_AGE is rounded to whole years
    "geographic variation meanAgeDifference": (-1.0, 1.0),
    "geographic variation femaleShareDifference": (-0.01, 0.01),
    "geographic variation dualShareDifference": (-0.02, 0.02),
    #stays are rebuilt from the claims (continuous periods of one service type, covered days only), see benchmarks.py
    "geographic variation ipStaysRelativeDifference": (-0.15, 0.15),
    "geographic variation erVisitsRelativeDifference": (-0.15, 0.15),
    #the library's ffs and CMS's Original Medicare differ by design (see test_benchmarks.py), loose bands that only show a
    #sudden change
    "ffs definitions cmsOmAlsoFfs": (0.9, 1.0),
    "ffs definitions ffsNotCmsOm": (0.0, 0.1),
    #CMS divides by the ever enrolled eligibles, 2.5-3% above the enrolled ones
    "ma penetration meanAbsoluteDifference": (0.0, 0.03),
    "ma penetration countiesWithin3Points": (0.8, 1.0),
    "hospital coverage posWithClaim acute": (0.9, 1.0),
    "hospital coverage posWithClaim cah": (0.9, 1.0),
    "hospital coverage claimsInPos acute": (0.95, 1.0),
    "hospital coverage claimsInPos cah": (0.95, 1.0)}

def get_band(check, key):
    return benchmarkBands[f"{check} {key}"]

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
