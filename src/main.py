from os import listdir
import pandas as pd
import logging
import psycopg2
import time

logging.basicConfig(
    level=logging.DEBUG,
    handlers=[
        logging.FileHandler('app.log', mode='a'),
        logging.StreamHandler()
    ]
)

logging.info("start of an app")

def find_csv_filenames( path_to_dir, suffix=".csv" ):
    """
    This function returns list of names of csv files from give directory
    """
    filenames = listdir(path_to_dir)
    return [ filename for filename in filenames if filename.endswith( suffix ) ]


def gather_column_names():
    """
    this function geathers names of columns from different csv files,
    transforms those column names, so they fall under some standart
    and return list of column names which will be added to the database
    """

    filename_list = find_csv_filenames("../zno")
    df_columns_list = list()

    for name in filename_list:
        temp_df = pd.read_csv(f"../zno/{name}", sep=';', error_bad_lines=False, nrows=10)
        #make columns names lower case, so it will be easier to intersect them
        column_list = [each_string.lower() for each_string in list(temp_df.columns)]
        df_columns_list.append(column_list)

    df0_df1_intesection = [x for x in df_columns_list[1] if x in df_columns_list[0]]

    return df0_df1_intesection


def insert_chunk_into_sql_table(chunk):
    """
    this function takes chunk of dataframe and iserts it into a database
    """
    username = 'postgres'
    password = '3220'
    database = 'first_db_lab'
    host = 'localhost'
    port = '5432'

    con = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)
    cur = con.cursor()

    if table_exists(con, "zno") == False:
        str_of_table_creation = produce_sql_create_table_statement_for_df(chunk, "zno")
        cur.execute(str_of_table_creation)

    con.commit()

    if table_exists(con, "zno"):
        chunk.to_csv("temorary_csv.csv", header=chunk.columns, index=False, encoding='utf-8')
        opened_csv_file = open("temorary_csv.csv")

        SQL_STATEMENT = """
        BEGIN;

        COPY zno FROM STDIN WITH
        CSV
        HEADER
        DELIMITER AS ',';

        COMMIT;

        """

        cur.copy_expert(sql=SQL_STATEMENT, file=opened_csv_file)

    con.commit()
    con.close()


def table_exists(con, table_str):
    """
    this function checks if the table we want to access exists
    """
    exists = False
    try:
        cur = con.cursor()
        cur.execute("select exists(select relname from pg_class where relname='" + table_str + "')")
        exists = cur.fetchone()[0]
        cur.close()
    except psycopg2.Error as e:
        print(e)
    return exists


def produce_sql_create_table_statement_for_df(dataframe, table_name):
    """
    this function takes dataframe and return a string with a sql create table 
    statement for a dataframe
    """
    replacements = {
    "object": "varchar",
    "float64": "float",
    "int64": "int"}

    #creating a string, that will have "column_name type" pairs separated by comma for each column in the chunks
    col_str = ",\n".join("{} {}".format(n, d) for (n, d) in zip(dataframe.columns, dataframe.dtypes.replace(replacements)))
    create_table_str = f"create table {table_name} (\n" + col_str + "\n);"
    return create_table_str


def transorm_chunk_columns(chunk, df_columns_list, zno_year):
    """
    This function takes chunk from dataframe, drops unnecessary columns,
    makes names of columns lowercase 
    """
    chunk.columns = chunk.columns.str.strip().str.lower()
    #find those columns that are in chunk columns but not in df column list
    chunk_excess_columns = [x for x in list(chunk.columns) if x not in df_columns_list]
    for column in chunk_excess_columns:
        if column in chunk.columns:
            chunk.drop(column, inplace=True, axis=1)

    chunk["year"] = zno_year
    #chunk.loc[:, ~chunk.columns.str.contains("adaptscale")]
    chunk.drop(list(chunk.filter(regex = 'adaptscale')), axis = 1, inplace = True)

    if zno_year == 2021:
        cols_to_convert_to_int = list(chunk.filter(regex = 'ball100'))
        chunk[cols_to_convert_to_int] = chunk[cols_to_convert_to_int].apply(lambda x: x.str.replace(',','.')).apply(pd.to_numeric)

    return chunk

def get_digits_from_string(string):
    temp_str = ""
    for char in string:
        if char.isdigit():
            temp_str = temp_str + char

    return temp_str


def get_chunk_and_file_counter_from_log(filename):

    '''
    this function checks if  insert chunk error substring in lines of log file
    if that is the case it returns tuple of numbers of file and chunk we should start our prog with, 
    if not it returns two zeroes. Also this function checks if the last run of the program was not a success.
    if it was, than the function will return (0,0) so the program can be started over without any problems
    '''
    last_line = ""
    with open(filename, 'r') as f:
        lines = f.readlines()
        if len(lines) < 2:
            return (0,0)
        if "success" in lines[-2]:
            return (0, 0)
        for line in lines:
            print
            if "InsertChunkError" in line:
                last_line = line

    #if that file contains information about previous breakdown, than we can set variables as those values
    #if not we set them as zero
    if len(last_line) == 0:
        logged_file_counter = 0
        logged_chunk_counter = 0
    else:
        comprehension = [int(s) for s in last_line.split() if s.isdigit()]
        logged_file_counter = comprehension[0]
        logged_chunk_counter = comprehension[1]

    return (logged_file_counter, logged_chunk_counter) 



def run_sql_command():
    """
    this function runs needed sql command
    """
    username = 'postgres'
    password = '3220'
    database = 'first_db_lab'
    host = 'localhost'
    port = '5432'

    con = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)
    cur = con.cursor()

    if table_exists(con, "zno"):
        SQL_STATEMENT = """ 
        DROP FUNCTION IF EXISTS select_using_year;

        CREATE OR REPLACE FUNCTION select_using_year(needed_year integer)
        RETURNS TABLE (
            reg VARCHAR,
            max_score double precision
        ) 
        AS $$
        BEGIN
             RETURN QUERY
                 select regname, max(histball100) from zno
                 where histteststatus = 'Зараховано' and year = needed_year 
                 group by regname;

        END; $$
        LANGUAGE plpgsql;


        select twenty_one_result.reg, twenty_one_result.max_score as "2021 res", 
                eighteen_result.max_score as "2018 res"
                
        from select_using_year(2021) as twenty_one_result
        join select_using_year(2018) as eighteen_result
        on twenty_one_result.reg = eighteen_result.reg;
        """
        cur.execute(SQL_STATEMENT)
        con.commit()
        table = cur.fetchall()
        df = pd.DataFrame(table, columns =['region', '2021', '2018'])
        df.to_csv("sql_sequence.csv", index=False)
        print(df)
    else: 
        print("table does not exist")

    con.close()




def main():
    filename_list = find_csv_filenames("../zno")
    df_columns_list = gather_column_names()
    logged_numbers = get_chunk_and_file_counter_from_log("app.log")

    chunk_size = 50000
    file_counter = 0
    chunk_counter = 0

    #looping over files, getting chunks, from those files as dataframes and writing those dataframes into a db
    for name in filename_list:
        file_counter += 1

        zno_year = get_digits_from_string(name)

        if file_counter < logged_numbers[0]:
            print("skipped one file")
            continue

        else:
            try:
                start_time = time.time()
                for chunk in pd.read_csv(f"../zno/{name}", sep=';', error_bad_lines=False, chunksize=chunk_size, low_memory=False):
                    chunk_counter += 1
                    if chunk_counter < logged_numbers[1]:
                        print("skipped one chunk")
                        continue

                    transformed_chunk = transorm_chunk_columns(chunk, df_columns_list, int(zno_year))
                    insert_chunk_into_sql_table(chunk)
                    print("inserted one chunk")

                chunk_counter = 0
                logging.info(f'finished handling file: {name}; ' f'took time: {round(time.time() - start_time)} seconds')
                logging.info("success")


            except Exception as e:
                logging.error(f"InsertChunkError {file_counter} {chunk_counter}")
                print(e)



if __name__ == '__main__':
    #main()
    run_sql_command()

