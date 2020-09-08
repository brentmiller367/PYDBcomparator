import time
import gc
from datetime import datetime
import pyodbc
import pandas as pd
import datacompy
import configparser
import os
import sys

import comparator_logging

import logging

# queries for the comparator
query1='SELECT [AUTH_LN_HASH], [AUTH_ID], [LOS_REQUESTING_PROVIDER], [HRD_INPUT_DT] FROM ##Temp1'
query2='SELECT [AUTH_LN_HASH], [AUTH_ID], [LOS_REQUESTING_PROVIDER], [HRD_INPUT_DT] FROM ##Temp2'

# argparse could be used for command line entry of files or dbs
# turboodbc - support is only up to 3.7 as of 7/21/2020
# this could be a flexible solution to the short comings of pyodbc

# sys.maxsize 9223372036854775807 on dev box which could be used for debug of 
# buffer size. For a more robust solution pyspark and a spark cluster could be
# leveraged to push operations to worker nodes

# using pathlib makes working with files nearly painless
# when using either \\path\\ or /path/ python will use the path regardless of OS


config = configparser.ConfigParser()
config.read('config.ini')
print(config)
server = config.get ('Database1', 'server')
database = config.get ('Database1', 'database')
server2 = config.get ('Database2', 'server')
database2 = config.get ('Database2', 'database')


def main():
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-v", "--verbose", action="count", help="Increment verbosity")
    parser.add_option("-d", "--debug", action="store_true", default=False, help="Print debugging items")
   
    global setup_connection_string, setup_connection_string2, cnxn, cnxn2, df1, df2, er
    (options, args) = parser.parse_args()

    if len(args) > 1:
        parser.error('Errors')

    if not args:
        setup_connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + '; Trusted_Connection=yes'
        connection_string = setup_connection_string
        if not connection_string:
            logging.debug(f'{connection_string}: No connection string 1 defined')
            logging.warning(f'{connection_string}: No connection string 1 defined')
            logging.info(f'{connection_string}: No connection string 1 defined')
            logging.critical(f'{connection_string}: No connection string 1 defined')
            print('No connection string 1 defined', flush=True)
            parser.print_help()
            raise SystemExit()
    else:
        connection_string = args[0]

    #cnxn = pyodbc.connect(connection_string, fast_executemany=True)
    # fast_executemany=True may cause failure when executing from a file
    cnxn = pyodbc.connect(connection_string)
    logging.info(f'{cnxn}: Connecting to DB1...')
    logging.debug(f'{cnxn}: Connection 1 in elapsed time {time.process_time()}')
    logging.info(f'{cnxn}: Connection 1 in {time.process_time()}')
    print (f'{cnxn}: Connected to DB1!\n', flush=True)
    print(f'{cnxn}: Connection 1 in {time.process_time()}\n', flush=True)

    if options.verbose:
        # print and log info from verbose cnxn
        logging.debug(f'{cnxn}: options verbose')
        logging.info(f'{cnxn}: options verbose')
        logging.warning(f'{cnxn}: options verbose')
        # verbose options can be added
        print(cnxn, flush=True)

    DropQueryTemp1='''IF OBJECT_ID('tempdb..##Temp1') IS NOT NULL DROP TABLE ##Temp1'''
    cursor = cnxn.cursor()
    cursor.execute(DropQueryTemp1)

    sql_source = '.\\sql_source'

    # open and close cursor logic for each statement to execute with
    # separate cursor.execute() calls using pyodbc. The last line
    # of the query can't have a ‘;’ at the end or it will cause a
    # split to append a blank value to the commands list and fail
    for script in os.listdir(sql_source):
        with open(sql_source+'\\' + script,'r') as inserts:
            sqlScript = inserts.read()
            for statement in sqlScript.split(';'):
                with cnxn.cursor() as cursor:
                    cursor.execute(statement)
    print(f'Source query executed in {time.process_time()}\n', flush=True)

    # Create empty dfs dataframe
    dfs = []

    # Start Chunking for the compare query
    for chunk in pd.read_sql(query1, cnxn, chunksize=100000):

        # Start Appending Data Chunks from SQL Result set into List
        dfs.append(chunk)

    # Start appending data from list to dataframe
    df1 = pd.concat(dfs, ignore_index=True)

    logging.debug(f'First DataFrame row count {len(df1.index)}')
    logging.debug(f'First DataFrame executed in elapsed time {time.process_time()}')
    logging.info(f'First DataFrame row count {len(df1.index)}')
    logging.info(f'First DataFrame executed in elapsed time {time.process_time()}')
    print(f'First DataFrame executed in {time.process_time()}\n', flush=True)

    if len(args) > 1:
        parser.error('Errors')

    if not args:
        setup_connection_string2 = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server2 + ';DATABASE=' + database2 + '; Trusted_Connection=yes'
        connection_string2 = setup_connection_string2
        if not connection_string2:
            logging.debug(f'{connection_string2}: No connection string 2 defined')
            logging.warning(f'{connection_string2}: No connection string 2 defined')
            logging.info(f'{connection_string2}: No connection string 2 defined')
            logging.critical(f'{connection_string2}: No connection string 2 defined')
            print('No connection string 2 defined', flush=True)
            parser.print_help()
            raise SystemExit()
    else:
        connection_string2 = args[0]

    # cnxn2 = pyodbc.connect(connection_string2, fast_executemany=True)
    cnxn2 = pyodbc.connect(connection_string2)
    logging.info(f'{cnxn2}: Connecting to DB2...')
    logging.debug(f'{cnxn2}: Connection 2 in elapsed time {time.process_time()}')
    logging.info(f'{cnxn2}: Connection 2 {time.process_time()}')
    print (f'{cnxn2}: Connected to DB2!\n', flush=True)
    print(f'{cnxn2}: Connection 2 in {time.process_time()}\n', flush=True)

    if options.verbose:
        # print and log info from verbose cnxn
        logging.debug(f'{cnxn2}: options verbose')
        logging.info(f'{cnxn2}: options verbose')
        logging.warning(f'{cnxn2}: options verbose')
        # optional verbose can be added
        print(cnxn2, flush=True)

    DropQueryTemp2='''IF OBJECT_ID('tempdb..##Temp2') IS NOT NULL DROP TABLE ##Temp2'''
    cursor2 = cnxn2.cursor()
    cursor2.execute(DropQueryTemp2)

    sql_source2 = '.\\sql_target'

    # open and close cursor logic for each statement to execute with
    # separate cursor.execute() calls using pyodbc. The last line
    # of the query can't have a ‘;’ at the end or it will cause a
    # split to append a blank value to the commands list and fail
    for script in os.listdir(sql_source2):
        with open(sql_source2+'\\' + script,'r') as inserts:
            sqlScript = inserts.read()
            for statement2 in sqlScript.split(';'):
                with cnxn2.cursor() as cursor2:
                    cursor2.execute(statement2)
    print(f'Target query executed in {time.process_time()}\n', flush=True)

    # Create empty dfs dataframe
    dfs2 = []
    
    # Start Chunking for the compare query
    for chunk in pd.read_sql(query2, cnxn2, chunksize=100000):

        # Start Appending Data Chunks from SQL Result set into List
        dfs2.append(chunk)

    # Start appending data from list to dataframe
    df2 = pd.concat(dfs2, ignore_index=True)

    logging.debug(f'Second DataFrame row count {len(df2.index)}')
    logging.debug(f'Second DataFrame executed in elapsed time {time.process_time()}')
    logging.info(f'Second DataFrame row count {len(df2.index)}')
    logging.info(f'Second DataFrame executed in elapsed time {time.process_time()}')
    print(f'Second DataFrame executed in {time.process_time()}\n', flush=True)

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
    logging.debug(f'Comparison executed in elapsed time {time.process_time()}')
    logging.info(f'Comparison executed in elapsed time {time.process_time()}')
    print(f'Comparison executed in {time.process_time()}\n', flush=True)

    print(type(compare))

    result=[compare.report()]

    compfile = open('./results/db_comparator.txt','w')
    compfile.writelines(result) # write the compared results to file
    compfile.close() 

    del cursor
    del cursor2

    cnxn.close()
    logging.debug(f'{cnxn}: connection 1 closed')
    logging.info(f'{cnxn}: connection 1 closed')
    print('Connection 1 closed.\n', flush=True)
    cnxn2.close()
    logging.debug(f'{cnxn2}: connection 2 closed')
    logging.info(f'{cnxn2}: connection 2 closed')
    print('Connection 2 closed.\n', flush=True)
    print('Processing complete!\n', flush=True)
    logging.info(f'Processing complete')

# ##############log to MSSQL DB lines################
#    connStr = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=SQLSERVER2017;DATABASE=Local_test;Trusted_Connection=yes')
#    cursor = connStr.cursor()

#    for index,row in compare.iterrows():
#        cursor.execute("INSERT INTO dbo.Local_test([CLAIM_ID],[CLAIM_NUM]) values (?, ?)", row['CLAIM_ID'], row['CLAIM_NUM']) 
#        connStr.commit()
#        logging.info(f'DB load complete')
#    cursor.close()
#    connStr.close()   
# output to the log the DB insert counts

logging.info(f'Garbage collection count {gc.get_count()}')
logging.debug(f'Garbage collection count {gc.get_count()}')
logging.debug(f'Garbage collection')
logging.info(f'Garbage collection')
gc.get_stats()
logging.info(f'Garbage collection Info Stats: {gc.get_stats()}')
logging.debug(f'Garbage collection Debug Stats: {gc.get_stats()}')

gc.get_debug()
gc.enable()
logging.debug(f'{gc} in {time.process_time()}')
logging.debug(f'Garbage collection debug {gc.get_debug()}')
logging.info(f'Garbage collection info {gc.get_debug()}')
gc.collect()
# use the print(*objects, sep=' ', end='\n', file=sys.stdout, flush=False) instead of flush per print
print(f'Full process in elapsed time {time.process_time()}\n', flush=True)
logging.info(f'Full process in elapsed time {time.process_time()}')
    #sys.exit(0)
    

if __name__ == '__main__':

    main()