# Health Services Data Management

PySpark functions for CMS and other health services-related data management. 

- All functions assume that column names follow the CMS Field Short Name convention 
- Base functions with name add_x  add 1 column with column name x (so that you do not have to guess the column name)
- Revenue/Line functions with name add_x include an inClaim flag, because sometimes we are interested in determining 
  something based on the line and sometimes we are interested in something claim-wide
  - when inClaim=True, these functions add 1 column with name x and 1 column with name xInClaim
  - when inClaim=False, these functions add 1 column with name x
- Functions with complex names, eg add_pcpHomeVisit, implicitly assume an AND condition, eg pcp and home visit
- Functions with name add_x_info add more than 1 column on the dataframe (you will need to do a printSchema to see what you added)
- Functions with name get_x return x and the argument(s) of the function is/are not modified
- When there is only one dataframe argument, then this is probably done using a withColumn pyspark command or a command 
   that does not require data shuffle (smaller computational load)
- When there are more than one dataframe arguments, then this is probably done using a join pyspark command or a command 
   that requires data shuffle (larger computational load)
- Explanations and references for the methods implemented are included on the code, when available

Examples:

- add_death_date_info(mbsfDF) uses withColumn commands to add columns to the mbsfDF using information from the same dataframe
- add_death_date_info(baseDF,mbsfDF) uses a join to add columns to baseDF using information from the mbsfDF dataframe

