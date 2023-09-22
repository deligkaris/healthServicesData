def get_claimsDF(baseDF,summaryDF): #assumes I have already summarized the revenue dataframe

    #CLAIMNO resets every year so need to include the THRU_DT as well
    claimsDF = baseDF.join(summaryDF,
                          on=["DSYSRTKY","CLAIMNO","THRU_DT"],
                          # because we collapsed all revenue records to a single summary revenue record, there should be 1-to-1 match
                          how = "left_outer")
    return claimsDF
