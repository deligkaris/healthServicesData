import pyspark.sql.functions as F
from pyspark.sql.window import Window

def getCodes(method="Glasheen2019"):

    #dictionary with conditions and their codes based on Glasheen2019 or Quan2005
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

def getCodesRegexp(codesDict):

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

def getConditionsFromBase(baseDF,conditionsList,codesRegexp,inpatient=True):

    maxCutoff = 0 if inpatient else 1

    dgnsColumnList = [f"ICD_DGNS_CD{x}" for x in range(1,26)] #all 25 DGNS columns

    #make a dataframe of two columns: dsysrtky and an array of all dgns codes found in their claims
    conditionsFromBase = (baseDF
                              .groupBy("DSYSRTKY")
                              .agg(F.flatten(F.collect_list(F.array(dgnsColumnList))).alias("dgnsList"))
                              .select(F.col("DSYSRTKY"),F.col("dgnsList"))
                              .persist())#comorbidity calculation will call on this several times so make it stay in memory
    
    conditionsFromBase.count() #now actually make it stay in memory

    #for every condition, search the array of dgns codes and keep only the codes found in comorbidities,
    #then count their frequencies and keep the max frequency for that condition, if the max is 2 or more,
    #assign that comorbidity condition to the beneficiary
    for iCondition in conditionsList:
        print(iCondition)
    
        #some column names
        codeColumn = iCondition+"Codes"
        codeFrequencyColumn = iCondition+"CodesFrequencies"
        codeMaxColumn = iCondition+"Max"
    
        conditionsFromBase = (conditionsFromBase
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
                               .withColumn(
                                   codeMaxColumn, #keep max of frequencies
                                       F.array_max(codeFrequencyColumn))
                               .withColumn(
                                   iCondition, #boolean, has condition or not
                                       F.when(F.col(codeMaxColumn)>maxCutoff,1)
                                        .otherwise(0))
                               .drop(codeColumn,codeFrequencyColumn,codeMaxColumn)) #no longer need these
                      
        #conditionsFromBase.persist()
        #conditionsFromBase.checkpoint()
        #conditionsFromBase.count()

    conditionsFromBase = conditionsFromBase.drop("dgnsList") #at the end this is no longer needed
    conditionsFromBase.persist()
    conditionsFromBase.count()
    
    return conditionsFromBase

#note: method Quan2005 IS NOT fully implemented
def getConditions(outpatientBaseDF,inpatientBaseDF,method="Glasheen2019"):

    codesDict = getCodes(method)

    conditionsList = list(codesDict.keys()) #get the conditions 

    codesRegexp = getCodesRegexp(codesDict)
 
    conditionsInpatient = getConditionsFromBase(inpatientBaseDF,conditionsList,codesRegexp,inpatient=True)
    conditionsOutpatient = getConditionsFromBase(outpatientBaseDF,conditionsList,codesRegexp, inpatient=False)

    # combine comorbidities from all inpatient and outpatient claims
    conditions = conditionsOutpatient.union(conditionsInpatient)

    eachDsysrtky = Window.partitionBy(["DSYSRTKY"])

    for iCondition in conditionsList:
        conditions = conditions.withColumn(iCondition, #overwrite each condition column
                                F.max(F.col(iCondition)).over(eachDsysrtky)) #keep the max for each beneficiary

    # AIDS = HIV + infection, see Glasheen2019, so a beneficiary has AIDS if both
    # HIV and infectionOrCancerDueToAids columns are true
    conditions = (conditions.withColumn("aids",
                                    F.col("hiv")*F.col("infectionOrCancerDueToAids")))
    conditions = conditions.drop(F.col("infectionOrCancerDueToAids")) #no longer needed

    #because windows broadcast the values to both rows for each beneficiary (the one from outpatient and the one from
    #inpatient condition dataframes), keep only one for each beneficiary
    conditions = conditions.distinct() 
    conditions.persist() #now that I am done, store it in memory
    conditions.count() #and now actually do it

    return conditions

###############

#ALL OF THESE ARE ATTEMPTS TO FIND COMORBIDITIES
#WHILE THEY ARE MEANINGFUL CODE, THEIR PERFORMANCE IS SUBOPTIMAL

#3min using 2016, 2016-2017 or 2016-2020 data, but without checkpoing, persist, count
#8min if I include persist in the inner loop (vs 3min without this) but I get stackoverflow later with count
#3.5min if i include persist in outer loop but failed with  org.apache.spark.SparkException: Job aborted due to stage failure: Task 1 in stage 26.1 failed 4 times, most recent failure: Lost task 1.3 in stage 26.1 (TID 2478, 10.4.1.35, executor 3): ExecutorLostFailure (executor 3 exited caused by one of the running tasks) Reason: Remote RPC client disassociated. Likely due to containers exceeding thresholds, or network issues. Check driver logs for WARN messages.

##############################

#to find comorbidities in outpatient claims, I need to search every code separately (because a code in 
#outpatient claims would have to appear twice in order to be counted)
#here I set the tests that will search claims for the codes
#codeTests = {}
#for iCondition in conditions:
#    for iCode in codes[iCondition]:
#        if iCode[-1] == "X":
#            lengthICode = len(iCode[:-1])
#            codeTests[f'code{iCode}'] = '(' + '|'.join(
#                #only the first portion of the code must match
#                f'(F.substring(F.col("ICD_DGNS_CD{x}"),1,{lengthICode}) == "{iCode[:-1]}")' for x in range(1,26)
#                ) +')'
#        else:
#            codeTests[f'code{iCode}'] = '(' + '|'.join(
#                #code must match in its entirety
#                f'(F.col("ICD_DGNS_CD{x}") == "{iCode}")' for x in range(1,26)
#                ) +')'

#################################

#dictionary with one test for each condition
#conditionTestsOutpatient = {}

#for iCondition in conditions:
#    print(iCondition)
#    for iCode in codes[iCondition]:
#        print(iCode)
#        conditionsOutpatient = (conditionsOutpatient
#                               .withColumn(
#                                   iCondition+iCode, #some codes appear in more than 1 condition
#                                   F.expr(f'size(filter(dgnsList, x -> x in ("{iCode}")))')
#                               ))
        
    #make the test for this condition that detects the presence of the condition in the claims    
#    conditionTestsOutpatient[f'{iCondition}'] =  '(' + '|'.join(
#                                 f'(F.col("{iCondition+iCode}")>=2)' for iCode in codes[f'{iCondition}']) +')'
    
    #see if this condition is present 
#    conditionsOutpatient = (conditionsOutpatient
#                                 .withColumn(iCondition,
#                                            F.when(eval(conditionTestsOutpatient[f'{iCondition}']),1)
#                                            .otherwise(0)))  
#    conditionsOutpatient.persist()
#    conditionsOutpatient.checkpoint()
#    conditionsOutpatient.count()

##############################

#dictionary with one test for each condition
#conditionTestsOutpatient = {}

#if the loops overwhelm the execution plan, try persist, checkpoint and materialize the dataframe that is 
#augmented during the loops before the loops start and after each iteration
#outpatientClaims.persist()
#outpatientClaims.checkpoint()
#outpatientClaims.count()

#for iCondition in conditions:
#    print(iCondition)
#    for iCode in codes[iCondition]:
#        print(iCode)
        #see if the code is present in the claim
#        outpatientClaims = (outpatientClaims
#                                   .withColumn(
#                                        iCondition+iCode, #some codes appear in more than 1 condition
#                                        F.when(eval(codeTests[f'code{iCode}']),1)
#                                        .otherwise(0)))
                            
#        outpatientClaims = (outpatientClaims
#                                    .withColumn(f'{iCondition+iCode}Sum',
#                                        F.sum(F.col(iCondition+iCode)).over(eachDsysrtky)))
        
        #do this in each iteration
#        outpatientClaims.persist()
#        outpatientClaims.checkpoint()
#        outpatientClaims.count()
        
    #make the test for this condition that detects the presence of the condition in the claims    
#    conditionTestsOutpatient[f'{iCondition}'] =  '(' + '|'.join(
#                                 f'(F.col("{iCondition+iCode}Sum")>=2)' for iCode in codes[f'{iCondition}']) +')'
    
    #see if this condition is present 
#    outpatientClaims = (outpatientClaims
#                                 .withColumn(iCondition,
#                                            F.when(eval(conditionTestsOutpatient[f'{iCondition}']),1)
#                                            .otherwise(0)))  
    
    #I no longer need these columns
    #for iCode in codes[iCondition]:
    #    conditionsOutpatient = conditionsOutpatient.drop(F.col(iCondition+iCode))
    
    #do this in each iteration
    #outpatientClaims.persist()
    #outpatientClaims.checkpoint()
    #outpatientClaims.count()
    
##########################################

#%%time
#1node, 20 workers, CPU times: user 8.33 s, sys: 2.2 s, total: 10.5 s, Wall time: 3h 15min 56s
#2nodes, 1 worker
#start dataframe with all beneficiaries in outpatient claims so that I can start building codes
#conditionsOutpatient = outpatientClaims.select(F.col("DSYSRTKY")).distinct() 

#conditionsOutpatient.persist()
#conditionsOutpatient.checkpoint()
#conditionsOutpatient.count()

#conditionTestsOutpatient = {}

#for iCondition in conditions:
#    print(iCondition)
#    for iCode in codes[iCondition]:
#        conditionsOutpatient = (conditionsOutpatient
#                               .join(
#                                   outpatientClaims
#                                   .withColumn(
#                                        iCondition+iCode, #some codes appear in more than 1 condition
#                                        F.when(eval(codeTests[f'code{iCode}']),1)
#                                        .otherwise(0))
#                                   .groupBy(
#                                         F.col("DSYSRTKY"))
#                                   .agg(
#                                         #pyspark renames it hence need to alias
#                                         F.sum(F.col(iCondition+iCode)).alias(iCondition+iCode))  
#                                   .filter(
#                                        F.col(iCondition+iCode) >=2 )
#                                   .select(
#                                       F.col("DSYSRTKY"),
#                                       F.col(iCondition+iCode)),
#                                   on = "DSYSRTKY",
#                                   how = "left"))
        
#    conditionsOutpatient = conditionsOutpatient.fillna(value=0, subset=[iCondition+iCode])
    
#    conditionTestsOutpatient[f'{iCondition}'] =  '(' + '|'.join(
#                                 f'(F.col("{iCondition+iCode}")>=2)' for iCode in codes[f'{iCondition}']) +')'
    
#    conditionsOutpatient = (conditionsOutpatient
#                                 .withColumn(iCondition,
#                                            F.when(eval(conditionTestsOutpatient[f'{iCondition}']),1)
#                                            .otherwise(0)))   
    
#    for iCode in codes[iCondition]:
#        conditionsOutpatient = conditionsOutpatient.drop(F.col(iCondition+iCode))
    
#    conditionsOutpatient.persist()
#    conditionsOutpatient.checkpoint()
#    conditionsOutpatient.count()

#############################

#%%time
#start dataframe with all beneficiaries in outpatient claims so that I can start building codes
#conditionsInpatient = inpatientClaims.select(F.col("DSYSRTKY")).distinct() 

#conditionsInpatient.persist()
#conditionsInpatient.checkpoint()
#conditionsInpatient.count()

#conditionTestsInpatient = {}

#for iCondition in conditions:
#    print(iCondition)
#    for iCode in codes[iCondition]:
#        conditionsInpatient = (conditionsInpatient
#                               .join(
#                                   inpatientClaims
#                                   .withColumn(
#                                        iCondition+iCode, #some codes appear in more than 1 condition
#                                        F.when(eval(codeTests[f'code{iCode}']),1)
#                                        .otherwise(0))
#                                   .groupBy(
#                                         F.col("DSYSRTKY"))
#                                   .agg(
#                                         #pyspark renames it hence need to alias
#                                         F.max(F.col(iCondition+iCode)).alias(iCondition+iCode))  
#                                   .filter(
#                                         F.col(iCondition+iCode) >=1 )
#                                   .select(
#                                       F.col("DSYSRTKY"),
#                                       F.col(iCondition+iCode)),
#                                   on = "DSYSRTKY",
#                                   how = "left"))
#        
#    conditionsInpatient = conditionsInpatient.fillna(value=0, subset=[iCondition+iCode])
    
   # conditionTestsInpatient[f'{iCondition}'] =  '(' + '|'.join(
    #                             f'(F.col("{iCondition+iCode}")>=1)' for iCode in codes[f'{iCondition}']) +')'
    
#    conditionsInpatient = (conditionsInpatient
#                                 .withColumn(iCondition,
#                                            F.when(eval(conditionTestsInpatient[f'{iCondition}']),1)
#                                            .otherwise(0))) 
    
#    for iCode in codes[iCondition]:
#        conditionsInpatient = conditionsInpatient.drop(F.col(iCondition+iCode))
        
#    conditionsInpatient.persist()
#    conditionsInpatient.checkpoint()
#    conditionsInpatient.count()

#############################

#conditionTestsOutpatient = {}
#for iCondition in conditions:
#    conditionTestsOutpatient[f'{iCondition}'] =  '(' + '|'.join(
#                                 f'(F.col("{iCondition+iCode}")>=2)' for iCode in codes[f'{iCondition}']) +')'

#for iCondition in conditions:
#    print(iCondition)
#    conditionsOutpatient = (conditionsOutpatient
#                                 .withColumn(iCondition,
#                                            F.when(eval(conditionTestsOutpatient[f'{iCondition}']),1)
#                                            .otherwise(0)))       
    
#for iCondition in conditions:
#    print(iCondition)
#    for iCode in codes[iCondition]:
#        conditionsOutpatient = conditionsOutpatient.drop(F.col(iCondition+iCode))

#############################

#conditionTestsInpatient = {}
#for iCondition in conditions:
#    conditionTestsInpatient[f'{iCondition}'] =  '(' + '|'.join(
#                                 f'(F.col("{iCondition+iCode}")>=1)' for iCode in codes[f'{iCondition}']) +')'

#for iCondition in conditions:
#    print(iCondition)
#    conditionsInpatient = (conditionsInpatient
#                                 .withColumn(iCondition,
#                                            F.when(eval(conditionTestsInpatient[f'{iCondition}']),1)
#                                            .otherwise(0)))       
    
#for iCondition in conditions:
#    print(iCondition)
#    for iCode in codes[iCondition]:
#        conditionsInpatient = conditionsInpatient.drop(F.col(iCondition+iCode))

#############################

#conditionTestsInpatient = {}
#for iCondition in conditions:
#    conditionTestsInpatient[f'{iCondition}'] = \
#        '(' + '|'.join('(F.col(' + f'"ICD_DGNS_CD{x}"' + f').isin(codes["{iCondition}"]))' \
#                   for x in range(1,26)) +')'

#############################

#start dataframe with all beneficiaries in outpatient claims so that I can start building codes
#conditionsInpatient = inpatientClaims.select(F.col("DSYSRTKY")).distinct() 

#for iCondition in conditions:
#    print(iCondition)
#    conditionsInpatient = (conditionsInpatient
#                               .join(
#                                   inpatientClaims
#                                   .withColumn(
#                                        iCondition,
#                                        F.when(eval(conditionTestsInpatient[f'{iCondition}']),1)
#                                        .otherwise(0))
#                                   .groupBy(
#                                         F.col("DSYSRTKY"))
#                                   .agg(
#                                         F.max(F.col(iCondition)).alias(iCondition))  #pyspark renames it hence need to alias
#                                   .select(
#                                       F.col("DSYSRTKY"),
#                                       F.col(iCondition)),
#                                   on = "DSYSRTKY",
#                                   how = "left"))
        
#conditionsInpatient = conditionsInpatient.fillna(value=0)

########################

#aggregateList = [F.max(F.col(x)).alias(x) for x in conditions]

#conditionsAgg = (conditions
#                    .groupBy(
#                        F.col("DSYSRTKY")) 
#                    .agg(
#                        *aggregateList))

#conditionsAgg.select(
#    [x for x in conditions]
#).summary().show(vertical=True)




