import pyspark.sql.functions as F
from pyspark.sql.window import Window

def get_codes(method="Glasheen2019"):
    '''Returns a dictionary with conditions and their codes based on Glasheen2019 or Quan2005
    Glasheen2019: https://pmc.ncbi.nlm.nih.gov/articles/PMC6684052/'''

    #codes with an X at the end: X can be any number (only initial part of code must match the claim code)
    if (method=="Glasheen2019"):
        codesDict = {       
            "myocardialInfraction": ["I21X","I22X","I252"],
            "congestiveHeartFailure": [ "I43X","I50X","I110","I130","I132","I255","I420",
                                        "I425","I426","I427","I428","I429","P290"],
            "peripheralVascular": ["I70X","I71X","I731","I738","I739","I771","I790","I791",
                                   "I798","K551","K558","K559", "Z958","Z959"],
            "cerbrovascular": ["G45X","G46X","I60X","I61X","I62X","I63X","I64X","I65X","I66X","I67X","I68X",
                               "H340X","H341X","H342X"],
            "dementia": ["F01X","F02X","F03X", "F04","F05","G30X","G94","R54",
                         "F061","F068","G132","G138","G310X","G311","G312","G914",
                         "R4181"],
            "chronicPulmonary": ["J40X","J41X","J42X","J43X","J44X","J45X","J46X","J47X","J60X","J61X",
                                 "J62X","J63X","J64X","J65X","J66X","J67X",
                                 "J684","J701","J703"],
            "rheumatic": ["M05X","M06X","M32X","M33X","M34X",
                          "M315","M351","M353","M360"],
            "pepticUlcer": ["K25X","K26X","K27X","K28X"],
            "liverMild": ["B18X","K73X","K74X",
                          "K700","K701","K702","K703","K709","K713","K714","K715",
                          "K717","K760","K762","K763","K764","K768","K769","Z944"],
            "diabetesWithoutCC": [ "E080X","E090X","E100X","E110X","E130X",
                                   "E081X","E091X","E101X","E111X","E131X",
                                   "E086X","E096X","E106X","E116X","E136X",
                                   "E088X","E098X","E108X","E118X","E138X",
                                   "E089X","E099X","E109X","E119X","E139X"],
            "renalMild": ["N03X","N05X",
                          "I129","I130","N181","N182","N183","N184","N189","Z940","I1310"],
            "diabetesWithCC": ["E082X","E092X","E102X","E112X","E132X",
                               "E083X","E093X","E103X","E113X","E133X",
                               "E084X","E094X","E104X","E114X","E134X",
                               "E085X","E095X","E105X","E115X","E135X"],
            "hemiplegia": ["G81X","G82X","G83X","G041","G114","G800","G801","G802"],
            "malignancy": ["C0XX","C1XX","C2XX","C9XX", 
                           "C30X","C31X","C32X","C33X","C34X",
                           "C37X","C38X","C39X","C40X","C41X","C43X","C45X","C46X",
                           "C47X","C48X","C49X","C50","C51X","C52X","C53X","C54X",
                           "C55X","C56X","C57X","C58X","C60X","C61X","C62X","C63X",
                           "C76X","C81X","C82X","C83X","C84X","C85X","C88X","C801"],
            "liverSevere": ["I850X","I864","K704X","K711X","K721X","K729X","K765","K766","K767"],
            "renalSevere": ["N19X","Z49X","I120","I132","N185","N186","N250","Z992","I1311"],
            "hiv": ["B20X","B20X"], #come back to this
            "metastaticSolidTumor": ["C77X","C78X","C79X","C800","C802"],
            #infectionOrCancerDueToAids is not a condition by itself but I use this in order to find below whether AIDS
            #is present or not (see Glasheen2019 for more details) because AIDS = HIV + aidsInfectionOrCancer
            "infectionOrCancerDueToAids": [ "B37X","C53X","B38X","B45X","B25X","B00","B39X","C46X","C81","C82","C83",
                                            "C84","C85","C86","C87","C88","C89","C90","C91","C92","C93","C94",
                                            "C95","C96","A31X","A15","A16","A17","A18","A19","B59","B58X","R64",
                                            "A072","G934X","A073","A812","A021",
                                            "Z8701"] }

    elif (method=="Quan2005"):
        codesDict = {       
            "myocardialInfraction": ["I21X","I22X","I252"],
            "congestiveHeartFailure": [ "I099","I43X","I50X","I110","I130","I132","I255","I420",
                                        "I425","I426","I427","I428","I429","P290"],
            "peripheralVascular": ["I70X","I71X","I731","I738","I739","I771","I790","I792","K551","K558","K559","Z958","Z959"],
            "cerbrovascular": ["G45X","G46X","I60X", "I61X","I62X","I63X","I64X","I65X","I66X","I67X","I68X","I69X","H340X"],
            "dementia": ["F00X","F01X","F02X","F03X", "F051","G30X","G311"],
            "chronicPulmonary": [ "I278","I279",
                                  "J40X","J41X","J42X","J43X","J44X","J45X","J46X","J47X",
                                  "J60X","J61X","J62X","J63X","J64X","J65X","J66X","J67X",
                                  "J684","J701","J703"],
            "rheumatic": ["M05X","M06X","M32X","M33X","M34X","M315","M351","M353","M360"],
            "pepticUlcer": ["K25X","K26X","K27X","K28X"],
            "mildLiver": [ "B18X", 
                           "K700","K701","K702","K703","K709",
                           "K713","K714","K715","K717","K73X","K74X",
                           "K760", "K762","K763","K764","K768","K769","Z944"],
            "diabetesWithoutCC": [ "E100","E110","E120","E130","E140",
                                   "E101","E111","E121","E131","E141",
                                   "E106","E116","E126","E136","E146",
                                   "E108","E118","E128","E138","E148",
                                   "E109","E119","E129","E139","E149"],
            "renal": [ "I120","I131",
                       "N032","N033","N034","N035","N036","N037",
                       "N052","N053","N054","N055","N056","N057",
                       "N18X","N19X","N250","Z490","Z491","Z492","Z940","Z992"],
            "diabetesWithCC": ["E102","E112","E122","E132","E142",
                               "E103","E113","E123","E133","E143",
                               "E104","E114","E124","E134","E144",
                               "E105","E115","E125","E135","E145",
                               "E107","E117","E127","E137","E147"],
            "hemiplegia": ["G81X","G82X","G830","G831","G832","G833","G834","G839","G041","G114","G801","G802"],
            "malignancy": ["C0XX","C1XX","C21X","C22X","C23X","C24X","C25X","C26X",#I modified this one
                           "C30X","C31X","C32X","C33X","C34X","C37X","C38X","C39X","C40X","C41X","C43X",
                           "C45X","C46X","C47X","C48X","C49X","C50","C51X","C52X","C53X","C54X",
                           "C55X","C56X","C57X","C58X","C6XX","C70X","C71X","C72X","C73X","C74X","C75X","C76X",
                           "C81X","C82X","C83X","C84X","C85X","C88X","C90X","C91X","C92X","C93X","C94X","C95X","C96X","C97X"],
             "severeLiver": ["I850","I859","I864","I982","K704","K711","K721","K729","K765","K766","K767"],
             "metastaticSolidTumor": ["C77X","C78X","C79X","C80X"],
             "hivAids": ["B20X", "B21X","B22X","B24X"] }

    return codesDict

def get_codes_regexp(codesDict):
    '''Uses the ICD10 codes and creates regular expressions for each code.
    Returns a dictionary where the keys are conditions and values are lists with regular expressions as list elements.'''  

    conditionsList = list(codesDict.keys()) # get the conditions

    #build a regular expression for each condition
    #regular expression pattern is different for each code, depending on whether there is an X at the end or not
    codesRegexp = {} #dictionary with all regular expressions

    for iCondition in conditionsList:
        codesRegexp[iCondition] = ""
        for iCode in codesDict[iCondition]:
            if iCode[-1] == "X": 
                if iCode[-2] == "X": #if the code ends with XX
                    #replace both X with \d (any digit), * is the 0 or more quantifier, must match beginning and end of string, add OR operator
                    #note: using \d instead of [0-9] creates issues due to python inserting a second backsplash
                    codesRegexp[iCondition] = codesRegexp[iCondition] + f'^{iCode[:-2]}[0-9][0-9]*?$|'
                else: #if the code ends with X
                    #replace X with \d (any digit), * is the 0 or more quantifier, must match beginning and end of string, add OR operator
                    #note: using \d instead of [0-9] creates issues due to python inserting a second backsplash
                    codesRegexp[iCondition] = codesRegexp[iCondition] + f'^{iCode[:-1]}[0-9]*$|'
            else:
                #must match beginning and end of string, add OR operator
                codesRegexp[iCondition] = codesRegexp[iCondition] + f'^{iCode}$|'
        codesRegexp[iCondition] = codesRegexp[iCondition][:-1] #remove last OR operator
    
    return codesRegexp

def get_conditions_from_dgnsDF(dgnsDF,conditionsList,codesRegexp,inpatient=True):
    '''dgnsDF: a dataframe with DSYSRTKY and an array of DGNS codes
       inpatient: defines if the DGNS codes were obtained from inpatient or outpatient base dataframes
       When using inpatient claims a single diagnostic code is enough to detect the condition whereas with outpatient claims the code must be
       present at least 2 times in order to detect the condition
       Returns a dataframe that is based on dgnsDF with added columns that correspond to the comorbidity conditions.'''

    if inpatient:
        print("Now processing the inpatient claims...")
    else:
        print("Now processing the outpatient claims...")

    maxCutoff = 0 if inpatient else 1

    conditionsFromDgns = dgnsDF #start the conditions dataframe using dgnsDF

    #for every condition, search the array of dgns codes and keep only the codes found in comorbidities,
    #then count their frequencies and keep the max frequency for that condition, if the max is 2 or more,
    #assign that comorbidity condition to the beneficiary
    for i, iCondition in enumerate(conditionsList, start=1):
        #print("Condition: ", iCondition, "...",i,"/",len(conditionsList)) #now this goes fast so no need to print
    
        #some column names
        codeColumn = iCondition+"Codes"
        codeFrequencyColumn = iCondition+"CodesFrequencies"
        codeMaxColumn = iCondition+"Max"
    
        conditionsFromDgns = (conditionsFromDgns
                               .withColumn(
                                   codeColumn, #keeps codes that match the regexp pattern
                                       F.expr(f'filter(dgnsList, x -> x rlike "{codesRegexp[iCondition]}")'))
                                       #F.expr(f'filter(dgnsList, x -> x in {testCodes})')
                               .withColumn(
                                   codeFrequencyColumn, #counts frequencies of code appearances
                                       #remove duplicates
                                       #https://docs.databricks.com/sql/language-manual/functions/array_distinct.html
                                       #apply lambda function to distinct codes
                                       #https://docs.databricks.com/sql/language-manual/functions/transform.html
                                       #for every distinct code, aggregate in the accumulator (acc) variable
                                       #the number of times it appears, returns an integer
                                       #https://docs.databricks.com/sql/language-manual/functions/aggregate.html
                                       F.expr(f"""array_sort(
                                                    transform(
                                                        array_distinct({codeColumn}), 
                                                        x-> aggregate({codeColumn}, 
                                                        0,(acc,t)->acc+IF(t=x,1,0))))"""))
                               .withColumn(codeMaxColumn, F.array_max(codeFrequencyColumn)) #keep max of frequencies
                               .withColumn(iCondition, F.when(F.col(codeMaxColumn)>maxCutoff,1).otherwise(0)) #boolean, has condition or not
                               .drop(codeColumn,codeFrequencyColumn,codeMaxColumn)) #no longer need these
                      
    conditionsFromDgns = conditionsFromDgns.drop("dgnsList") #at the end this is no longer needed
    conditionsFromDgns.persist()
    conditionsFromDgns.count()
    
    return conditionsFromDgns

def get_dayDgnsDF(baseDF):
    '''Takes an inpatient or outpatient base dataframe and returns a dataframe with columns DSYSRTKY and a STRUCT list (column name is dayDgnsStruct)
       where each element is a tuple of the THRU_DT_DAY and one of the 25 DGNS codes (we keep only the non null DGNS codes)
       Each row of this dataframe corresponds to a single DSYSRTKY and each DSYSRTKY will be found in a single row
       The tuple has two elements, the thruDay and the dgnsCode.
       The dataframe that will be returned from this function can be stored on the disk and then loaded using a new notebook to save memory usage.'''
    
     dgnsCodeColumns = [f"ICD_DGNS_CD{x}" for x in range(1,26)]
     dgnsStruct = [F.struct(F.col("THRU_DT_DAY").alias("thruDay"), F.col(c).alias("dgnsCode")) for c in dgnsCodeColumns]

     baseDF = (baseDF
                .select("DSYSRTKY", "THRU_DT_DAY", *dgnsCodeColumns)
                .withColumn("dayDgnsStruct", F.filter( F.array(dgnsStruct), lambda x: x.getItem("dgnsCode").isNotNull()))
                .groupBy("DSYSRTKY")
                .agg(F.flatten(F.collect_list("dayDgnsStruct")).alias("dayDgnsStruct")))

     return baseDF

def get_conditions(baseDF, opDayDgnsDF, ipDayDgnsDF, method="Glasheen2019"):
    '''opDayDgnsDF: dataframe created from outpatient base, includes DSYSRTKY and a STRUCT with (thruDay, dgnsCode) from all non null ICD10 codes
       ipDayDgnsDF: similar to above from inpatient base
       baseDF: dataframe with DSYSRTKY and THRU_DT_DAY, will use this df to built the comorbidities
       note: method Quan2005 IS NOT fully implemented'''

    codesDict = get_codes(method)

    conditionsList = list(codesDict.keys()) #get the conditions 

    codesRegexp = get_codes_regexp(codesDict)

    ipDgnsDF = (ipDayDgnsDF.join(baseDF, #for inpatient claims
                                 on = ["DSYSRTKY"],
                                 how = "inner") #will duplicate rows from ipDayDgnsDF for all rows of the same DSYSRTKY (with probably different through dates)
                           .withColumn("dayDgnsStruct", #keep dgns codes within the last year
                                       F.filter( F.col("dayDgnsStruct"), 
                                       lambda x: (F.col("THRU_DT_DAY") - x.getItem("thruDay") >= 0) & (F.col("THRU_DT_DAY") - x.getItem("thruDay") <= 360)))
                           .withColumn("dgnsList", F.col("dayDgnsStruct").getItem("dgnsCode")) #keep the dgns codes only (no through dates)
                           .drop("dayDgnsStruct"))                                          

    opDgnsDF = (opDayDgnsDF.join(baseDF, #similarly for outpatient claims
                                 on = ["DSYSRTKY"],
                                 how = "inner")
                           .withColumn("dayDgnsStruct",
                                       F.filter( F.col("dayDgnsStruct"),
                                       lambda x: (F.col("THRU_DT_DAY") - x.getItem("thruDay") >= 0) & (F.col("THRU_DT_DAY") - x.getItem("thruDay") <= 360)))
                           .withColumn("dgnsList", F.col("dayDgnsStruct").getItem("dgnsCode"))
                           .drop("dayDgnsStruct"))

    conditionsInpatient = get_conditions_from_dgnsDF(ipDgnsDF,conditionsList,codesRegexp,inpatient=True)
    conditionsOutpatient = get_conditions_from_dgnsDF(opDgnsDF,conditionsList,codesRegexp, inpatient=False)

    conditions = conditionsOutpatient.union(conditionsInpatient) # combine comorbidities from all inpatient and outpatient claims

    eachDsysrtkyDay = Window.partitionBy(["DSYSRTKY", "THRU_DT_DAY"]) #every DSYSRTKY and THROUGH_DT_DAY will have potentially different conditions
    for iCondition in conditionsList:
        conditions = conditions.withColumn(iCondition, #overwrite each condition column
                                F.max(F.col(iCondition)).over(eachDsysrtkyDay)) #keep the max for each beneficiary and through date

    # AIDS = HIV + infection, see Glasheen2019, so a beneficiary has AIDS if both
    # HIV and infectionOrCancerDueToAids columns are true
    conditions = (conditions.withColumn("aids", F.col("hiv")*F.col("infectionOrCancerDueToAids"))
                            .drop(F.col("infectionOrCancerDueToAids")) #no longer needed
                            #because windows broadcast the values to both rows for each beneficiary (the one from outpatient and the one from
                            #inpatient condition dataframes), keep only one for each beneficiary
                            .distinct())

    conditions.persist() #now that I am done, store it in memory
    conditions.count() #and now actually do it

    return conditions

