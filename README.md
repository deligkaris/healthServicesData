# Health Services Data Management

PySpark functions for CMS and other health services-related data management. 
These functions are designed to work with the Ohio SuperComputer (OSC) CMS data.
These functions may need to be slightly modified in order to be used with CMS data housed elsewhere in case there are
small differences in column names.

- Functions with name add_x  add 1 column with column name x (so that you do not have to guess the column name)
- Functions with name add_x_info add more than 1 column on the dataframe (you will need to do a printSchema to see what you added)
- Functions with name get_x return x and the argument(s) of the function is/are not modified
- When there is only one dataframe argument, then this is done using a withColumn pyspark command (smaller computational load)
- When there are more than one dataframe arguments, then this is probably done using a join pyspark command (larger computational load)

Examples:

- add_death_date_info(mbsfDF) uses withColumn commands to add columns to the mbsfDF using information from the same dataframe
- add_death_date_info(baseDF,mbsfDF) uses a join to add columns to baseDF using information from the mbsfDF dataframe

To do:

- organize the two functions in cms utilities with dictionaries and loops
- consider moving the Hospital10 cost report and GME data from utilities to cms utilities
- consider the ability to load Hospital10 data for more than 1 year (right now I am getting the latest year available)
- update add date info functions to work ok for more years in the future, and implement a meaningful otherwise option
- when a function A assumes that another function B has been run prior, then use an if statement to automatically run function B that needs to be run
