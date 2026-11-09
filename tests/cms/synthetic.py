'''Writes the fake CMS and external files tests/cms/test_end_to_end.py runs the library on, at the paths the library reads
them from, so nothing real is needed. Every value is invented; the file's cohort is what that test's answers come from.'''
import csv
import datetime
import random
import re
from pathlib import Path

import cms.utilities as cmsUtilitiesF
import utilities as utilitiesF
from cms.longToShortXW import longToShortXW
from cms.schemas import schemas

(yearI, yearF) = (2018, 2019)

#an Ohio hospital that sends patients and one that receives them, both in Franklin county (SSA 36250, FIPS 39049);
#their external values differ so that a join crossing them shows
(ssaState, ssaCounty, fipsCounty) = ("36", "250", "39049")
sending = {"npi": 1000000001, "ccn": "360001", "zip": "43201", "name": "Sending Hospital", "beds": 200, "icuBeds": 0, "cmi": 1.5}
receiving = {"npi": 1000000002, "ccn": "360002", "zip": "43202", "name": "Receiving Hospital", "beds": 500, "icuBeds": 100, "cmi": 2.5}
hospitals = [sending, receiving]

def write_csv(path, header, rows=(), delimiter=","):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=delimiter)
        writer.writerow(header)
        writer.writerows(rows)

def write_parquet(spark, path, columns, rows):
    spark.createDataFrame(rows, columns).write.mode("overwrite").parquet(path)

def file_year(path):
    #from the file's name alone, a temporary folder can hold a year too
    return int(re.findall(r"20\d\d", Path(path).name)[-1])

def write_npi(path):
    taxonomyColumns = [c for x in range(1, 16) for c in (f"Healthcare Provider Taxonomy Code_{x}",
                                                         f"Healthcare Provider Primary Taxonomy Switch_{x}")]
    header = ["NPI", "Provider Organization Name (Legal Business Name)", "Provider Other Organization Name",
              "Provider First Line Business Practice Location Address", "Provider Second Line Business Practice Location Address",
              "Provider Business Practice Location Address City Name", "Provider Business Practice Location Address State Name",
              "Provider Business Practice Location Address Postal Code", *taxonomyColumns]
    #282N00000X is a general acute care hospital, see utilities.add_gach
    rows = [[h["npi"], h["name"], "", "1 Main St", "", "Columbus", "OH", h["zip"], "282N00000X", "Y", *[""] * 28] for h in hospitals]
    write_csv(path, header, rows)

def write_aha(path):
    year = int(re.search(r"FY(\d{4})", path).group(1))
    header = ["MCRNUM", "MAPP3", "MAPP5", "MAPP8", "MAPP18", "BDH", "HOSPBD", "FTERES", "LAT", "LONG", "MSICBD", "MSICHOS", "CBSATYPE",
              "CNTRL", "MHSMEMB", "SYSID", "STRCHOS", "STRCSYS", "STRCVEN", "EICUHOS", "EICUSYS", "EICUVEN"]
    #the ICU beds change with the year, so ahaBedsIcuPrior shows which survey it came from
    rows = [[h["ccn"], 2, 2, 2, 2, h["beds"], h["beds"], 10, 39.9, -83.0, h["icuBeds"] + year - 2000, 1, "Metro", 21, 1, 1234,
             0, 0, 0, 0, 0, 0] for h in hospitals]
    write_csv(path, header, rows)

def write_external_files(spark, pathToData, pathToAHAData):
    '''Every file utilities.get_data opens: real rows for what get_cms_data joins, a stub for the rest.'''
    #the survey of the year before yearI is optional, see utilities.get_filenames, so its path comes from a year earlier
    for path in utilitiesF.get_filenames(pathToData, pathToAHAData, yearI - 1, yearF)["aha"]:
        write_aha(path)
    filenames = utilitiesF.get_filenames(pathToData, pathToAHAData, yearI, yearF)
    for path in filenames["sdoh"]:
        write_csv(path, ["COUNTYFIPS", "YEAR", "ACS_MEDIAN_HH_INC", "POS_MEDIAN_DIST_ED", "POS_MEDIAN_DIST_MEDSURG_ICU",
                         "POS_MEDIAN_DIST_TRAUMA"], [[fipsCounty, file_year(path), 60000, 3.5, 7.5, 20.5]])
    for path in filenames["chspHosp"]:
        write_csv(path, ["compendium_hospital_id", "ccn", "health_sys_id"], [["CHSP1", sending["ccn"], "SYS1"], ["CHSP2", receiving["ccn"], ""]])
    for path in filenames["cmi"]:
        write_csv(path, ["provider", "year", "cases", "casemixindex", "sumcasemix"],
                  [[h["ccn"], file_year(path), 100, h["cmi"], 100 * h["cmi"]] for h in hospitals])
    write_npi(filenames["npi"][0])
    write_csv(filenames["cbsa"][0], ["ssacounty", "fipscounty", "countyname", "state"], [[ssaState + ssaCounty, fipsCounty, "FRANKLIN", "OH"]])
    write_csv(filenames["ersRucc"][0], ["FIPS", "RUCC_2013"], [[fipsCounty, 1]])
    for path in filenames["census"]:
        write_csv(path, ["state", "county", "B19013_001E", "B01001_001E"], [["39", "049", 61000, 1300000]])
    for path in filenames["zipToHrr"]:
        twoDigitYear = re.search(r"ZipHsaHrr(\d\d)", path).group(1)
        write_csv(path, [f"zipcode{twoDigitYear}", "hsanum", "hsacity", "hsastate", "hrrnum", "hrrcity", "hrrstate"],
                  [[h["zip"], 36001, "Columbus", "OH", 330, "Columbus", "OH"] for h in hospitals])
    for path in filenames["maPenetration"]:
        write_csv(path, ["FIPS", "Penetration", "Year"], [[fipsCounty, "35.5", file_year(path)]])
    #two active acute hospitals since 1966, and a CAH closed in 2017 (tests/data/benchmarks.get_pos_active)
    write_parquet(spark, filenames["pos"][0], ["PRVDR_NUM", "providerFIPS", "providerStateFIPS", "GNRL_CNTL_TYPE_CD", "posCah", "posIsRural",
                                               "posHospital", "posHospitalType", "PGM_TRMNTN_CD", "ORGNL_PRTCPTN_DT", "TRMNTN_EXPRTN_DT"],
                  [(h["ccn"], fipsCounty, "39", "04", 0, 0, 1, "acute", "00", "19660701", None) for h in hospitals] +
                  [("361301", fipsCounty, "39", "04", 1, 1, 1, "cah", "01", "20010101", "20170630")])
    write_parquet(spark, filenames["hcris"][0],
                  ["PRVDR_NUM", "hcrisYear", "providerHcrisBedsIcu", "providerHcrisBedsCriticalCare", "providerHcrisBedsTotal",
                   "providerHcrisBedDaysIcu", "providerHcrisBedDaysCriticalCare", "providerHcrisBedDaysTotal",
                   "providerHcrisResidents", "providerHcrisResidentToBedRatio", "providerHcrisIsRural", "hcrisReportDays"],
                  [(h["ccn"], year, h["icuBeds"], h["icuBeds"], h["beds"], 365 * h["icuBeds"], 365 * h["icuBeds"], 365 * h["beds"],
                    10.0, 0.05, 0, 365) for h in hospitals for year in range(yearI, yearF + 1)])
    write_parquet(spark, filenames["ccnStrokeCertification"][0],
                  ["posCcn", "jcBestProgram", "jcCertificationConfidence", "jcSites", "jcCcnSource"],
                  [(receiving["ccn"], "Comprehensive Stroke Center", 4, 1, "current")])
    #get_data reads these but get_cms_data joins none of them: one row with the columns their prep_* functions read
    write_csv(filenames["zipToCounty"][0], ["zip", "county", "bus_ratio", "tot_ratio"], [["43201", fipsCounty, 1, 1]])
    write_csv(filenames["adi"][0], ["FIPS", "ADI_NATRANK", "ADI_STATERNK"], [["390490001001", 50, 5]])
    write_csv(filenames["aamcHospitals"][0], ["FY22 COTH Member", "Teaching Status", "Major Teaching Status"],
              [["N", "Non-Teaching", "Non-Teaching"]])
    write_csv(filenames["acgmeSites"][0], ["Institution Name", "Institution Postal Code"], [["Sending Hospital", "43201"]])
    write_csv(filenames["acgmePrograms"][0], ["Program Name", "Program Postal Code"], [["Neurology Program", "43201"]])
    write_csv(filenames["strokeCentersCamargo"][0], ["CCN"], [[receiving["ccn"]]])
    write_csv(filenames["procedureClasses"][0], ["ICD-10-PCS-CODE"], [["'0016070'"]])
    write_csv(filenames["gazetteer2020"][0], ["GEOID", "NAME"], [[fipsCounty, "Franklin"]], delimiter="\t")
    #no prep function reads these, they only have to be there
    for source in ["shpCounty", "ersPeople", "ersJobs", "ersIncome", "cbiHospitals", "cbiDetails", "npiMedicareXw", "medicareHospitalInfo"]:
        write_csv(filenames[source][0], ["stub"], [[1]])

def mbsf(beneficiary, year, hmoMonths=0, deathDate=None, state=ssaState, county=ssaCounty):
    '''A beneficiary with parts A and B every month, on Medicare Advantage the first hmoMonths months (HMOIND C, a lock-in
    plan).'''
    return {"DSYSRTKY": beneficiary, "RFRNC_YR": year, "STATE_CD": state, "CNTY_CD": county, "SEX": 1, "RACE": 1, "AGE": 75,
            "HMO_MO": hmoMonths, "DEATH_DT": deathDate, "V_DOD_SW": "V" if deathDate else None,
            **{f"BUYIN{month}": "3" for month in range(1, 13)}, **{f"MDCR_STUS_CD_{month:02d}": 10 for month in range(1, 13)},
            **{f"HMOIND{month}": "C" if month <= hmoMonths else "0" for month in range(1, 13)},
            **{f"DUAL_{month:02d}": 0 for month in range(1, 13)}}

def to_date(yyyymmdd):
    return datetime.date(yyyymmdd // 10000, yyyymmdd // 100 % 100, yyyymmdd % 100)

def facility_claim(beneficiary, claimNumber, hospital, admission, through, codes=(), procedures=(), status=1):
    row = {"DSYSRTKY": beneficiary, "CLAIMNO": claimNumber, "ORGNPINM": hospital["npi"], "PROVIDER": hospital["ccn"],
           "ADMSN_DT": admission, "DSCHRGDT": through, "THRU_DT": through, "FAC_TYPE": 1, "TYPESRVC": 1, "PRSTATE": ssaState,
           "STATE_CD": ssaState, "CNTY_CD": ssaCounty, "PPS_IND": 2, "STUS_CD": status, "PTNTSTUS": str(status),
           #Medicare covered days, the nights of the stay and at least one (tests/data/benchmarks.get_ip_stays reads them)
           "UTIL_DAY": max(1, (to_date(through) - to_date(admission)).days) if admission else 1}
    row.update({f"ICD_DGNS_CD{i}": code for i, code in enumerate(codes, 1)})
    row.update({f"ICD_PRCDR_CD{i}": code for i, code in enumerate(procedures, 1)})
    return {**row, "PRNCPAL_DGNS_CD": codes[0]} if codes else row

def revenue_lines(beneficiary, claimNumber, through, *codes):
    return [{"DSYSRTKY": beneficiary, "CLAIMNO": claimNumber, "THRU_DT": through, "CLM_LN": i, "REV_CNTR": code}
            for i, code in enumerate(codes, 1)]

#a post-acute provider none of the hospital files know
postAcute = {"npi": 1000000003, "ccn": "365001"}

def get_cohort():
    '''claimTypePart -> year -> rows, see the table of beneficiaries in test_end_to_end.py.'''
    (tpa, evt, stroke, nihss12) = ("3E03317", "03CG3ZZ", "I639", "R29712")
    return {
        "mbsf": {year: [mbsf(1, year, deathDate=20190330 if year == 2019 else None),
                        #as the double the files of some years hold, see cms.utilities.pad_zeros
                        mbsf(2, year, state="36.0", county="250.0"),
                        mbsf(3, year, hmoMonths=12),
                        mbsf(4, year, deathDate=20190401 if year == 2019 else None),
                        mbsf(5, year), mbsf(6, year)] for year in (2018, 2019)},
        "ipBase": {2018: [facility_claim(5, 51, sending, 20180901, 20180905, ["I509", "I219"])],
                   2019: [facility_claim(1, 11, sending, 20190310, 20190310, [stroke, nihss12], [tpa], status=2),
                          facility_claim(1, 12, receiving, 20190310, 20190320, [stroke], [evt]),
                          facility_claim(2, 21, receiving, 20190501, 20190505, ["A419", "R6521"], status=3),
                          facility_claim(3, 31, sending, 20190601, 20190603, [stroke]),
                          facility_claim(4, 41, sending, 20190201, 20190203, ["J449"]),
                          facility_claim(4, 42, sending, 20190410, 20190412, ["J449"]),
                          facility_claim(5, 52, sending, 20190701, 20190701, [stroke], status=2),
                          facility_claim(5, 53, receiving, 20190702, 20190708, [stroke]),
                          facility_claim(6, 61, receiving, 20190815, 20190820, [stroke])]},
        "ipRevenue": {2018: revenue_lines(5, 51, 20180905, 100),
                      2019: [*revenue_lines(1, 11, 20190310, 450, 350), *revenue_lines(1, 12, 20190320, 610, 200),
                             *revenue_lines(2, 21, 20190505, 200), *revenue_lines(3, 31, 20190603, 450),
                             *revenue_lines(4, 41, 20190203, 100), *revenue_lines(5, 52, 20190701, 100),
                             *revenue_lines(5, 53, 20190708, 100), *revenue_lines(6, 61, 20190820, 100)]},
        "opBase": {2019: [facility_claim(6, 62, sending, None, 20190815, [stroke])]},
        "opRevenue": {2019: revenue_lines(6, 62, 20190815, 450)},
        "snfBase": {2019: [facility_claim(2, 22, postAcute, 20190505, 20190515)]},
        "hhaBase": {2019: [{**facility_claim(2, 23, postAcute, None, 20190530), "HHSTRTDT": 20190516}]},
        "hospBase": {2019: [{**facility_claim(4, 43, postAcute, None, 20190401), "HSPCSTRT": 20190301}]},
        "carBase": {2019: [{"DSYSRTKY": 6, "CLAIMNO": 63, "THRU_DT": 20190720, "STATE_CD": ssaState, "CNTY_CD": ssaCounty, "PMTDNLCD": "1"}]},
        "carLine": {2019: [{"DSYSRTKY": 6, "CLAIMNO": 63, "THRU_DT": 20190720, "CLM_LN": 1, "HCFASPCL": "11", "HCPCS_CD": "99213", "PRCNGIND": "A"}]}}

#stored with the long CMS names, as some files are, see cms.utilities.enforce_short_names
longNameFiles = {"hhaBase"}

def write_cms_files(spark, pathCMS, seed=None):
    '''One parquet per claim part and year, at the paths and on the schemas the library reads, empty where the cohort has no rows.
    With a seed, each file's rows are shuffled and split into 3 part files, the same claims arriving in another order.'''
    cohort = get_cohort()
    for claimTypePart, paths in cmsUtilitiesF.get_filenames(pathCMS, yearI, yearF).items():
        schema = schemas[claimTypePart]
        for path in paths:
            rows = [tuple(row.get(name) for name in schema.fieldNames()) for row in cohort.get(claimTypePart, {}).get(file_year(path), [])]
            if seed is None:
                df = spark.createDataFrame(rows, schema=schema)
            else:
                random.Random(seed).shuffle(rows)
                df = spark.createDataFrame(spark.sparkContext.parallelize(rows, 3), schema=schema)
            if claimTypePart in longNameFiles:
                longNames = {short: long for long, short in longToShortXW[claimTypePart].items()}
                df = df.toDF(*[longNames[c] for c in df.columns])
            df.write.mode("overwrite").parquet(path)
