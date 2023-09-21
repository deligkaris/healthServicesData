from cms.SCHEMAS.car_schema.py import carLineSchema, carLineLongToShortXW

def prep_lineDF(lineDF, claim="car"):

    lineDF = enforce_schema(lineDF, claim=claim)

    return lineDF

def enforce_schema(lineDF, claim="car"):

    if claim=="car":
        lineDF = lineDF.select([ (F.col(field.name).cast(field.dataType)).alias(carLineLongToShortXW[field.name]) for field in carLineSchema.fields])

    return lineeDF
