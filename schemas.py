from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

#schemas of the non-CMS data sources that are read from headerless files, so the schema is what names
#the columns (the CMS claim schemas live in cms/schemas.py)

#column order of the HCRIS hospital 2552-10 flat files, taken from HCRIS_Data_model.pdf in HOSPITAL2010-DOCUMENTATION.ZIP
#(https://www.cms.gov/data-research/statistics-trends-and-reports/cost-reports/hospital-2552-2010-form)
#the files carry no header row so the schema is what names the columns, and PRVDR_NUM must stay a string
#or the leading zero of the low-numbered state codes (eg Alabama, 010001) is lost and the CCN no longer
#matches the PROVIDER column of the claims
hcrisRptSchema = StructType([StructField("RPT_REC_NUM", LongType()),
                             StructField("PRVDR_CTRL_TYPE_CD", StringType()),
                             StructField("PRVDR_NUM", StringType()),
                             StructField("NPI", StringType()),
                             StructField("RPT_STUS_CD", StringType()),
                             StructField("FY_BGN_DT", StringType()),
                             StructField("FY_END_DT", StringType()),
                             StructField("PROC_DT", StringType()),
                             StructField("INITL_RPT_SW", StringType()),
                             StructField("LAST_RPT_SW", StringType()),
                             StructField("TRNSMTL_NUM", StringType()),
                             StructField("FI_NUM", StringType()),
                             StructField("ADR_VNDR_CD", StringType()),
                             StructField("FI_CREAT_DT", StringType()),
                             StructField("UTIL_CD", StringType()),
                             StructField("NPR_DT", StringType()),
                             StructField("SPEC_IND", StringType()),
                             StructField("FI_RCPT_DT", StringType())])

hcrisNmrcSchema = StructType([StructField("RPT_REC_NUM", LongType()),
                              StructField("WKSHT_CD", StringType()),
                              StructField("LINE_NUM", StringType()),
                              StructField("CLMN_NUM", StringType()),
                              StructField("ITM_VAL_NUM", DoubleType())])
