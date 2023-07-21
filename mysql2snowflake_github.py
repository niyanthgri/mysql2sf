import pandas as pd
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas
from pandas import DataFrame
from itertools import chain
from sqlalchemy import create_engine
from db_details import *
import mysql.connector
import pymysql

'''
CHECK IF YOU NEED THIS on your laptop:
======================================
START: Fix for "_mysql is not defined, Django on Mac M1 with MySQL DB"

This is needed for MacOS with M1 chip set
'''

# Fake PyMySQL’s version and install as MySQLdb
# https://adamj.eu/tech/2020/02/04/how-to-use-pymysql-with-django/
# pymysql.version_info = (1, 4, 2, 'final', 0)

pymysql.version_info = (1, 4, 6, 'final', 0) # For mysqlclient under docker
pymysql.install_as_MySQLdb()

'''
End: Fix for "_mysql is not defined, Django on Mac M1 with MySQL DB"
'''


def get_db_details(db_src, db_dest):
    '''
        Getting the User's MySQL and Snowflake details to make the connection  
    '''
    # Creare MySQL Database Connection
    print(f"{SQL_PASSWORD} {SQL_USER_NAME} {SQL_TYPE} {SQL_HOST_URL} {SQL_PORT} ")
    '''
    This is a way to connect to the mysql database. 
    But let us use an alternative method to connect using create_engine.

    mysql_conn = mysql.connector.connect(host = SQL_HOST_URL, user = SQL_USER_NAME, password = SQL_PASSWORD)
    mysql_cur = mysql_conn.cursor()
    '''

    # Let's use an alternative method to connect with MySQL
    
    mysql_engine = create_engine(f'{SQL_TYPE}://{SQL_USER_NAME}:{SQL_PASSWORD}@{SQL_HOST_URL}:{SQL_PORT}')
    mysql_conn = mysql_engine.raw_connection()
    mysql_cur = mysql_conn.cursor()

    # Create Snowflake Database Connection
    sf_conn = snow.connect(user=SF_USER,
                        password=SF_PASSWORD,
                        account=SF_ACCOUNT,
                        warehouse=SF_WAREHOUSE,
                        database= db_dest,
                        schema=SF_SCHEMA)

    sf_cur = sf_conn.cursor()

    return sf_cur, mysql_cur, sf_conn, mysql_conn

def countList(lst1, lst2):
    '''
        Creating one list from two other lists, "column name" and "column datatype". \
        Each element of the two lists are appended to the new list alternatively. 
        Example: a[0] b[0], a[1] b[1], a[2] b[2] and so on, 'a' being 'column name' and 'b' being 'column datatype'
    '''
    return [sub[item] for item in range(len(lst2))
    for sub in [lst1, lst2]]
    
def create_table(file_name, table_name, sf_cur, mysql_conn, dest_db, source_db):
    '''
        Creating a table dynamically
    '''

    df = pd.read_sql(f"desc {source_db}.{table_name}", mysql_conn)
    column_name = []
    column_dtype = []

    column_name = df["Field"] 
    column_dtype = df["Type"]

    field_list = []
    for i in column_name:
        field_list.append(i)

    dtype_list = []
    for j in column_dtype:
        dtype_list.append(j)

    total_list = []
    total_list = countList(field_list, dtype_list)
    k = ','
    # initializing N
    N = 2
    # using itertool.chain()
    # inserting k =  "," after every Nth number
    res = list(chain(*[total_list[i : i+N] + [k]
                if len(total_list[i : i+N]) == N
                else total_list[i : i+N]
                for i in range(0, len(total_list), N)]))
    res.pop(-1)
    table_column_names = ' '.join(map(str, res))

    print(f'table_column_names: {table_column_names}\ntable_name: {table_name}')
    create_table = f'create TABLE {dest_db}.{SF_SCHEMA}.{table_name} ({table_column_names})'
    exec_create_table = sf_cur.execute(create_table).fetchall()
    print(f'Result of create_table:{exec_create_table[0][0]}')

def check_table_exists(table_info, cur, db):
    '''
        Checking if the source or destination table exists. If the source table doesn't exist, return an error.
        If the destination table doesn't exist, call the create_table() function
    '''
    try: 
        sqlquery = f'USE {db}'
        res_set = cur.execute(sqlquery)
    except Exception as err:
        print(f"ERROR: {err}")
    try: # Checking if table exists. If table doesn't exist, creates a new table. If table exists, replace or append. 
        sqlquery = f'SELECT count(*) FROM {table_info}'
        print(f"{sqlquery}")
        res_set = cur.execute(sqlquery)
        list_table = cur.fetchall()
        table_found = True
    except Exception as err:
        list_table = [()]
        table_found = False
        # print(f"ERROR: Exception while checking table '{table_info}': {err}")
    return table_found, list_table

def table2csv(user_table, user_file_name, db_conn, db):
    '''
        Converting the MySQL table to a dataframe and the dataframe to a CSV file.
    '''
    dfsql = pd.read_sql(f'SELECT * FROM {db}.{user_table}', db_conn)
    dfcsv = dfsql.to_csv(user_file_name, index=False)
    # print(f'CSV file: {dfcsv}, dfsql = {dfsql}')
    return 

def prepare_dest_tbl(user_table_name, file_name, dest_db, sf_cur, mysql_conn, source_db):
    '''
        Checking if destination table exists. If table doesn't exist, creates a new table. If table exists, replace or append. 
    '''
    try: 
        dest_table_found, number_of_records = check_table_exists(f'{user_table_name}', sf_cur, dest_db)
        if dest_table_found:
            user_input = input(f"Snowflake table, '{user_table_name}' found. Enter 'a' for append, 'r' for replace: ")
            if user_input == 'a':
                print(f'\nnumber_of_records before append = {number_of_records[0][0]}')

            elif user_input == 'r':
                truncate_table_query = f'truncate table {user_table_name}'
                exec_drop_table = sf_cur.execute(truncate_table_query)
                print("\nTable is truncated.")
                # creating_table()
            else:
                print('Enter a valid option.')
                exit()
        else: # Table did not exist. Hence creating a new table.
            print(f"Snowflake table did not exist. Hence creating table: '{user_table_name}'\n")
            create_table(file_name, user_table_name, sf_cur, mysql_conn, dest_db, source_db)
    except Exception as err :
        print(f'Error: {err}')

    return number_of_records

def copy_file2snoflake(user_table_name, file_name, dest_db, sf_cur):
    '''
        Copying the converted CSV file to snowflake
    '''

    df1 = pd.read_csv(file_name)
    len_csv = len(df1)
    print(f'Number of lines in CSV file = {len_csv}')

    sql = f'PUT file://{file_name} @{SF_STAGE_NAME}' # Uploading file to stage
    file2stage = sf_cur.execute(sql).fetchall()
    print(f'\nFile2Stage:{file2stage[0]}')

    stage2table = f'COPY INTO {dest_db}.{SF_SCHEMA}.{user_table_name} \
                    FROM @{SF_STAGE_NAME}/{file_name} FILE_FORMAT = \
                    (TYPE = "csv" FIELD_DELIMITER = "," SKIP_HEADER = 0) ON_ERROR = "CONTINUE" FORCE = TRUE' # Uploading file from stage to table
    result2 = sf_cur.execute(stage2table).fetchall()
    print(f'Stage to table: {result2[0][0]}')

    return len_csv

def validate_data_copy(sf_cur, user_table_name, dest_records_present, len_csv):
    '''
        Validating if the number of records that have been appended or replaced 
        match the number of records to be appended and/or replaced.
    '''
    number_of_records_query = f'SELECT COUNT(*) FROM {user_table_name}'
    exec_no_records = sf_cur.execute(number_of_records_query).fetchall()
    print(f'\nNumber_of_records after file added = {exec_no_records[0][0]}')

    if dest_records_present != [()]:
        if exec_no_records[0][0] > dest_records_present[0][0]:
            number_of_records_added = exec_no_records[0][0] - dest_records_present[0][0]
            if number_of_records_added == len_csv or exec_no_records[0][0] == len_csv:
                print(f'The number of lines in CSV = {len_csv} is the same as number of records added = {exec_no_records[0][0]} - {dest_records_present[0][0]} = {number_of_records_added}')
            else:
                print(f'The number of lines in CSV = {len_csv} is not the same as number of records added = {exec_no_records[0][0]} - {number_of_records_added} != {number_of_records[0][0]}')
        elif exec_no_records[0][0] <= dest_records_present[0][0]:
            number_of_records_added = exec_no_records[0][0]
            if number_of_records_added == len_csv or exec_no_records[0][0] == len_csv:
                print(f'The number of lines in CSV = {len_csv} is the same as number of records added = {number_of_records_added}')
            else:
                print(f'The number of lines in CSV = {len_csv} is not the same as number of records added = {number_of_records_added}')
    else:
        # number_of_records = [(0),]
        # number_of_records_added = exec_no_records[0][0]
        print(f'The number of lines added: {exec_no_records[0][0]} = the number of lines in CSV: {len_csv}')

def main():
    '''
        Main Function    
    '''
    source_db = input(f"Enter the name of the MySQL database[{SQL_DB}]: ") or SQL_DB
    dest_db = input(f"Enter the name of the Snowflake database[{SF_DATABASE}]: ") or SF_DATABASE
    sf_cur, mysql_cur, sf_conn, mysql_conn = get_db_details(source_db, dest_db)

    user_table_name = input(f"Enter the name of the MySQL table[{DEFAULT_TABLE_NAME}]: ") or DEFAULT_TABLE_NAME
    file_name = f'{user_table_name}.csv'
    print(f"Source table name: '{user_table_name}', CSV file name to be created: '{file_name}'")

    src_tbl = f"{user_table_name}"
    dest_tbl = f"{user_table_name}"
    user_table_found, src_records = check_table_exists(src_tbl, mysql_cur, source_db)

    if not user_table_found:
        print(f'Error. Source table "{src_tbl}" not found.')
        exit(1)
    else:
        print(f'Source table "{src_tbl}" found. Number of records = {src_records[0][0]}')

    table2csv(src_tbl, file_name, mysql_conn, source_db)
    dest_records_present = prepare_dest_tbl(dest_tbl, file_name, dest_db, sf_cur, mysql_conn, source_db)
    len_csv = copy_file2snoflake(dest_tbl, file_name, dest_db, sf_cur)
    validate_data_copy(sf_cur, dest_tbl, dest_records_present, len_csv)
 
    sf_cur.close()
    sf_conn.close() 

if __name__ == "__main__":
    ''' 
        Initializing main()
    '''
    main()
    exit(0)
