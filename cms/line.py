from cms.SCHEMAS.car_schema import carLineSchema, carLineLongToShortXW
import pyspark.sql.functions as F

def prep_lineDF(lineDF, claim="car"):

    lineDF = clean_line(lineDF, claim=claim)
    lineDF = enforce_schema(lineDF, claim=claim)

    return lineDF

def clean_line(lineDF, claim="car"):

    #mbsf, op, ip files were cleaned and this line was removed in them, but the rest of the files still include the first row
    #that essentially repeats the column names
    if ( (claim=="car") ):
        lineDF = lineDF.filter(~(F.col("DESY_SORT_KEY")=="DESY_SORT_KEY"))

    return lineDF

def enforce_schema(lineDF, claim="car"):

    if claim=="car":
        lineDF = lineDF.select([ (F.col(field.name).cast(field.dataType)).alias(carLineLongToShortXW[field.name]) for field in carLineSchema.fields])

    return lineDF
