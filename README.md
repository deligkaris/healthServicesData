# Health Services Data Management

PySpark functions for CMS and other health services-related data management. 
These functions are designed to work with the Ohio SuperComputer (OSC) CMS data.
These functions may need to be slightly modified in order to be used with CMS data housed elsewhere in case there are
small differences in column names.

- Functions with name add_x_info add more than 1 column on the dataframe (you will need to do a printSchema to see what you added)
- Functions with name add_x add 1 column with column name x (so that you do not have to guess the column name)
- Function with name get_x return x (argument(s) of the function is/are not modified)

To do:

- organize the two functions in cms utilities with dictionaries and loops
- consider moving the Hospital10 cost report and GME data from utilities to cms utilities
- consider the ability to load Hospital10 data for more than 1 year (right now I am getting the latest year available)
