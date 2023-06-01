def get_claimsDF(baseDF,revenueSummaryDF): #assumes I have already summarized the revenue dataframe

    # join base and revenue information for each patient and claim
    claimsDF = baseDF.join(revenueSummaryDF,
                          on=["DSYSRTKY","CLAIMNO"],
                          # because we collapsed all revenue records to a single summary revenue record, there should be 1-to-1 match
                          how = "left_outer")

    return claimsDF
