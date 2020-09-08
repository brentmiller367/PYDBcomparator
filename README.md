# PYDBcomparator Database Comparator Prototype Utility

Database Comparator Utility Theory of Operations:
The Database Comparator Utility is used to compare source to target ETL/ELT jobs and looks for data anomalies between the source and target comparisons and generates sample result reports of any anomalies from source, target or both when mismatches are found. It will check datatypes mismatches and which every column is specified within query1 and query2 which are driving comparator queries. The tool can also use columns with matching names by default when query1 and query2 are not specifically aliasing column names. This is something that is likely out of scope for the Data Warehouse, but the capability is possible and would simplify the comparisons.
The Database Comparator Utility is a Python 3.8 Pandas-based application. It should run on Python versions down to 2.7 but this hasn’t been fully tested on all versions. As with the any Python Pandas-based comparisons the Database Comparator matching will be attempted even if data types don't match. Any schema differences will be reported in the output as well as in the mismatch reports.
Note: That Python Pandas-based dataframes comparisons leverage available memory for running the application. For the scope of the Database Comparator Utility you do need to ensure that you have enough free cache memory before you run this application. 

The DB Comparator Utility service would be executed via command line but could be executed through automation.

DB Comparator.py – Python Script	The DB Comparator primary use is to find data anomalies in Source to Target database comparisons for ETL/ELT packages within the Data Warehouse team. It can also be adapted to use Source to Target files such as csv.

COMPARATOR OPERATIONS
The Database Comparator Utility operation:
•	Database comparisons of source and target tables

UTILITY OPERATION
This utility could be automated to continuously check data integrity per source to target jobs associated with ETL/ELT for the Data Warehouse or anywhere for that matter for data anomalies.
NOTE: if you only want to validate whether a dataframe matches exactly or not, you could look at ``pandas.testing.assert_frame_equal``.  
The main use case for this Database Comparator Utility Python application is when you need to interpret the difference between two dataframes.

UTILITY PROCESS
The Database Comparator Utility supports the following Actions:  
•	Source to Target comparisons via CML execution of db_comparator.py
•	Reading SQL files for the querying of source and target databases
•	Generation of a sample report
o	Summary of the data analysis
o	60 sample rows of data from source, target or both if anomalies are detected
	A future state should write all data anomaly rows to MSSQL

COMPARE ACTIONS AND DEFAULTS
The Database Comparator Utility Python application leverages a datacompy library to compare two Pandas DataFrames. This is similar to Pandas DataFrames ``Pandas.DataFrame.equals(Pandas.DataFrame)`` but with more functionality. It prints out stats, and lets you tweak how accurate matches must be, generates sample results, logs, config.ini to specify databases and read from source SQL files. Then extended to carry that functionality over to Spark DataFrames.
By default, the application will try to join two DataFrames either on a list of join columns, or on indexes.  If the two DataFrames have duplicates based on join values, the match process sorts by the remaining fields and joins based on that row number. Column-wise comparisons attempt to match values even when data types don't match. So if, for example, you have a column with ``decimal.Decimal`` values in one DataFrame and an identically-named column with ``float64`` data type in another, it will tell you that the data types are different but will still try to compare the values. 

Basic Usage within db_comparator.py
compare = datacompy.Compare(
        df1,
        df2,
        join_columns='acct_id',  #You can also specify a list of columns
        abs_tol=0, #Optional, defaults to 0
        rel_tol=0, #Optional, defaults to 0
        df1_name='Original', #Optional, defaults to 'df1'
        df2_name='New' #Optional, defaults to 'df2'
        )
    compare.matches(ignore_extra_columns=False)
    # False

    # This method prints out a human-readable report summarizing and sampling differences
    print(compare.report())

Database Comparator Utility configurations: 
•	The Comparator configurations of the application, databases and logging
•	The db_comparator.py is the primary application file where two specific areas can be changed according to the comparison requirements.
•	Within the config.ini file the databases are defined that the application uses for connections. These can be easily changed to align with the SQL query files that will be read by the application and executed to load the Dataframes.
•	The primary logging for the application is in comparator_logging.py
•	The logging.conf file defines (optional) formats for the console logging, levels, etc. for the application logging.

APPLICATION CONFIGURATIONS
Below are snippets from the primary application file (db_comparator.py) where the compare queries and joins are defined, and the main logic references live. The bulk of the comparison logic is with the datacompy folder as core.py.

# queries for the comparator
query1='SELECT [AUTH_LN_HASH], [AUTH_ID], [LOS_REQUESTING_PROVIDER], [HRD_INPUT_DT] FROM ##Temp1'
query2='SELECT [AUTH_LN_HASH], [AUTH_ID], [LOS_REQUESTING_PROVIDER], [HRD_INPUT_DT] FROM ##Temp2'

The above snippet shows which columns are being pulled from the temp tables that were load by the SQL files executed by the application and then these load the Dataframes for the comparisons. These should be adapted be use case.

    for script in os.listdir(sql_source):
        with open(sql_source+'\\' + script,'r') as inserts:
            sqlScript = inserts.read()
            for statement in sqlScript.split(';'):
                with cnxn.cursor() as cursor:
                    cursor.execute(statement)
                    
Above is the logic that open and close the cursor for each SQL statement within the files to execute with separate cursor.execute() calls using pyodbc. The last line of the SQL query files can't have a ‘;’ at the end or it will cause a split to append a blank value to the commands list and fail. 

    compare = datacompy.Compare(
                                df1,
                                df2,
                                join_columns = ['AUTH_LN_HASH', 'AUTH_ID', 'HRD_INPUT_DT'],
                                abs_tol = 0,
                                rel_tol = 0,
                                df1_name = 'Source',
                                df2_name = 'Target'
                                )

    compare.matches(ignore_extra_columns = False)
The above snippet shows which columns in the Dataframes compared. These should be adapted be use case.

WARNINGS
A common error that might be encountered when running the Database Comparator Utility can be:
pyodbc.Error: ('HY090', '[HY090] [Microsoft][ODBC Driver Manager] Invalid string or buffer length (0) (SQLExecDirectW)')
This can be associated to not having enough memory for the data, 32-bit systems, a fast_executemany parameter for a query executed by file or chunksize associated to the binding parameter, having a ‘;’ at the end of a sql query which causes a split to append a blank value to the commands list and fail on table.to_sql(), null values in join columns (this shouldn’t be an issue with the latest pyodbc), some non-supported operators like “IF” exists statements and possibly OBJECT_ syntax. Just be aware to first look to the query’s used on read first if the error is encountered.
https://github.com/mkleehammer/pyodbc/issues/548

THINGS THAT ARE HAPPENING BEHIND THE SCENES
•	By default you pass in two DataFrames (``df1``, ``df2``) to ``datacompy.Compare`` and a  column to join on (or list of columns) to ``join_columns``.  By default, the comparison needs to match values exactly, but you can pass in ``abs_tol`` and/or ``rel_tol`` to apply absolute and/or relative tolerances for numeric columns.
•	You can pass in ``on_index=True`` instead of ``join_columns`` to join on the index instead.
•	The class validates that you passed DataFrames, that they contain all the columns in `join_columns` and have unique column names other than that.  The class also lowercases all column names to disambiguate.
•	On initialization the class validates inputs and runs the comparison.
•	``Compare.matches()`` will return ``True`` if the DataFrames match, ``False`` otherwise.
•	You can pass in ``ignore_extra_columns=True`` to not return ``False`` just because there are non-overlapping column names (will still check on overlapping columns).
NOTE: if you only want to validate whether a dataframe matches exactly or not, you should look at ``pandas.testing.assert_frame_equal``.  The main use case for Database Comparator Utility Python application is when you need to interpret the difference between two dataframes.
Compare also has some shortcuts like:
•	``intersect_rows``, ``df1_unq_rows``, ``df2_unq_rows`` for getting intersection, just df1 and just df2 records (DataFrames).
•	``intersect_columns()``, ``df1_unq_columns()``, ``df2_unq_columns()`` for getting intersection, just df1 and just df2 columns (Sets).

BIG DATA COMPARISIONS
Possible future state
For larger comparisons you should leverage a Spark cluster (either on-premise or cloud) setup to take advantage of the nature of Resilient Distributed Dataset (RDD) performance advantages. With RDD you direct the compute to worker nodes while a driver (head node) acts as the execution conductor. 
Azure Databricks is a great option if no on-premise availability is an option. Also, the lift to setup an on-premise Spark cluster takes more knowledge, time and money them simply provisioning Azure Databricks for the purpose. Azure Databricks will consume Python, R, Java and SQL to then convert and optimize the source (as best as possible) to Scala which is native Spark source. With the runbooks in Databricks it’s a great interactive method to learning Scala by allowing you to write and run code line by line as you develop within Databricks.

For comparisons in the billions of records, leverage the Spark Compare class. The Spark Compare class will join two dataframes either on a list of join columns. It has the capability to map column names that may be different in each dataframe, including in the join columns. You are responsible for creating the dataframes from any source which Spark can handle and specifying a unique join key. If there are duplicates in either dataframe by join key, the match process will remove the duplicates before joining (and tell you how many duplicates were found).

As with the Pandas-based ``Compare`` class, comparisons will be attempted even if data types don't match. Any schema differences will be reported in the output as well as in any mismatch reports, so that you can assess whether a type mismatch is a problem or not.
The main reasons why you would choose to use ``Spark Compare`` over ``Compare`` are that your data is too large to fit into memory, or you're comparing data that works well in a Spark environment, like partitioned Parquet, CSV, or JSON files, or Cerebro tables.

PERFORMANCE IMPLICATIONS
Spark scales incredibly well, so you can use ``Spark Compare`` to compare billions of rows of data, provided you spin up a big enough cluster. Still, joining billions of rows of data is an inherently large task, so there are a couple of things you may want to take into consideration when getting into "big data":
``Spark Compare`` will compare all columns in common in the dataframes and report on the rest. If there are columns in the data that you don't care to compare, use a ``select`` statement/method on the dataframe(s) to filter those out. Particularly when reading from wide Parquet files, this can make a huge difference when the columns you don't care about don't have to be read into memory and included in the joined dataframe.

For large datasets, adding ``cache_intermediates=True`` to the ``SparkCompare`` call can help optimize performance by caching certain intermediate dataframes in memory, like the de-duped version of each input dataset, or the joined dataframe. Otherwise, Spark's lazy evaluation will recompute those each time it needs the data in a report or as you access instance attributes. This may be fine for smaller dataframes but will be costly for larger ones. You do need to ensure that you have enough free cache memory before you do this, so this parameter is set to False by default.

REQUIREMENTS FOR THE UTILITY OPERATION
UTILITY REQUIREMENTS
Application operation - pandas>=0.19.0,!=0.23.*numpy>=1.11.3 (generally for Spark)
Application testing - pandas>=0.19.0,!=0.23.*numpy>=1.11.3pytest>=5.1.3Sphinx>=1.6.2sphinx-rtd-theme>=0.2.4numpydoc>=0.6.0pre-commit>=1.10.4 – pytest and/or basic python unittest could be leveraged as well.

