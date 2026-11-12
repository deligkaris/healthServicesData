'''Floors for the rates the data tests measure, and the known exceptions to their hard checks.
Set each from the metrics table a run prints (its check and key columns name the entry), with a # reason.'''

#minimum rate, keyed "<check> <key>" as the metrics table prints them; a rate with no entry must only be >= 0
rateFloors = {}

def get_floor(check, key):
    return rateFloors.get(f"{check} {key}", 0.0)

#(low, high) of each comparison of test_benchmarks.py, keyed "<check> <key>" as the metrics table prints them (less its
#lowest/highest). Set from the OSC runs (2018-2019, 11/2026) at about two to three times the gap they showed, so a year
#to year change passes and a regression does not
benchmarkBands = {
    #the Original Medicare population rebuilt from the mbsf against BENES_OM_CNT, 100% of the mbsf: 0.997-1.005
    "geographic variation omCountRatio": (0.99, 1.01),
    #ours is about a year below CMS's in every state (-1.56 to -0.51): uniform, so a definition, not the data; CMS rounds
    #BENE_AVG_AGE to whole years and likely takes the age at another point of the year than the mbsf AGE
    "geographic variation meanAgeDifference": (-2.0, 0.0),
    #-0.0005 to 0.0006
    "geographic variation femaleShareDifference": (-0.002, 0.002),
    #-0.0028 to 0.0031
    "geographic variation dualShareDifference": (-0.01, 0.01),
    #stays rebuilt from the claims (continuous periods of one service type, covered days only), see benchmarks.py:
    #-2.0% to +0.7%
    "geographic variation ipStaysRelativeDifference": (-0.05, 0.05),
    #-1.7% to +0.9%
    "geographic variation erVisitsRelativeDifference": (-0.05, 0.05),
    #the library's ffs and CMS's Original Medicare differ by design (see test_benchmarks.py): 0.9982-0.9996 of CMS's are ffs
    "ffs definitions cmsOmAlsoFfs": (0.99, 1.0),
    #0.014-0.043 of the library's ffs are not in CMS's Original Medicare, by state
    "ffs definitions ffsNotCmsOm": (0.0, 0.1),
    #with the counties of states 01-09, which join since their FIPS is padded (prep_maPenetrationDF): 0.0055-0.0057 apart
    #on average, and 0.976-0.978 of the counties within 3 points; CMS divides by the ever enrolled eligibles, 2.5-3% above
    #the enrolled ones
    "ma penetration meanAbsoluteDifference": (0.0, 0.015),
    "ma penetration countiesWithin3Points": (0.93, 1.0),
    #0.989-0.991 and 0.992-0.993 of the POS hospitals active that year have an ip claim
    "hospital coverage posWithClaim acute": (0.97, 1.0),
    "hospital coverage posWithClaim cah": (0.97, 1.0),
    #0.9988 and 0.9985 of the claims' CCNs are POS hospitals active that year
    "hospital coverage claimsInPos acute": (0.995, 1.0),
    "hospital coverage claimsInPos cah": (0.995, 1.0)}

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

#the counts of bad values allowed per year loaded, the tests multiply them by the years of the run: the values the 2018-2019
#run found (11/2026), a year, at about twice

#(part or source, column) -> rows a year allowed not to match the key format (test_cms_values.py, test_external.py)
keyFormatExceptions = {("carLine", "PRF_NPI"): 40,     #41 in 2018-2019, NPIs of 9 digits
                       ("carLine", "HCPCS_CD"): 5,     #5, eg '3'
                       ("ipRevenue", "HCPCS_CD"): 170, #165, eg '885', 'GY', '1'
                       ("opRevenue", "HCPCS_CD"): 270, #266, eg '0636', 'N BV', 'GZ'
                       ("aha", "MCRNUM"): 2}           #2 'Y', no CCN, so they match no claim

#claimTypePart -> ICD codes a year allowed outside the unpunctuated upper case format, all columns together
#(test_cms_values.py): 3 ip procedure codes in 2018-2019, truncated (eg '0TY1'), and 240 op codes
icdFormatExceptions = {"ipBase": 3, "opBase": 240}

#source -> rows allowed with a value wrapped in quotes (test_external.py), of the one file each loads: 172 and 4
quoteWrappedAllowed = {"npi": 350, "npiMedicareXw": 10}

#claimTypePart -> {column: values a cast to the schema type may turn null} (test_files.py): the mbsf writes a month that
#is not dual as 'NA' in 2018-2019 (about 49 M beneficiaries a month), which mbsf.add_medicaidEver reads as not dual; 0.2%
#of them have the state buy-in, test_dual_values reports it
castLossAllowedValues = {"mbsf": {f"DUAL_{m:02d}": {"NA"} for m in range(1, 13)}}

#(claimTypePart, year) of the files that store PROVIDER as a number (test_files.py), whose leading 0 cms.utilities.pad_zeros
#restores, which test_provider_keeps_leading_zero checks
providerStoredAsNumber = {("hhaBase", 2018), ("hhaBase", 2019)}

#claimTypePart -> added columns allowed to be all null in a year (test_pipeline.py)
nullableAddedColumns = {}

#added columns allowed to break the lower case first rule (test_pipeline.py)
namingExceptions = {"GNRL_CNTL_TYPE_CD",              #a raw POS field, joined by base.add_provider_pos_info
                    #the mortality flags start with their window, renaming them would break the analysis code
                    "30DaysAfterAdmissionDateDead", "90DaysAfterAdmissionDateDead", "365DaysAfterAdmissionDateDead",
                    "90DaysAfterThroughDateDead", "365DaysAfterThroughDateDead"}
