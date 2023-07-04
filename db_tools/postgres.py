import psycopg2
import psycopg2.extras as extras
import pandas as pd
from io import StringIO
import json
from urllib.parse import quote_plus
from sqlalchemy import create_engine, types
from sqlalchemy.dialects.postgresql import JSONB


__version__ = 'dev'

def config_to_uri(param_dic):
    """
    Convert a configuration dictionary into a database URI.

    Parameters:
    param_dic (dict): Configuration dictionary.

    Returns:
    str: Database URI.
    """
    template = "postgresql://{user}:{password}@{host}:{port}/{database}"
    return template.format(**param_dic)

def try_parse_json(text):
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        return text

def insert_df_jsons(df, table_name, param_dic):
    """
    Insert a DataFrame with json or not into a database table using SQLAlchemy.

    Parameters:
    df (pandas.DataFrame): DataFrame to insert.
    table_name (str): Name of the table to insert into.
    db_uri (str): Database URI.
    """
    db_uri = config_to_uri(param_dic)
    engine = create_engine(db_uri)

    # Check all columns in the DataFrame
    for col in df.columns:
        df[col] = df[col].apply(try_parse_json)

    # Define a dictionary for dtypes
    dtypes = {col: JSONB for col in df.columns if df[col].apply(isinstance, args=(dict,)).any()}

    df.to_sql(table_name, engine, if_exists='append', index=False, dtype=dtypes)

# Usage:
# df is your DataFrame
# db_uri is your database URI, something like 'postgresql://user:password@localhost:5432/mydatabase'
insert_into_db(df, 'test_table', db_uri)
    
def array_generator4sql(items: list) -> tuple:
    """
    Convert a list of items into a tuple for SQL query.

    :param items: list of items
    :return: tuple of items
    """
    if len(items) > 1:
        items = tuple(items)
    else:
        items.append(items[0])
        items = tuple(items)
    return items

def create_connection(param_dic: dict) -> psycopg2.extensions.connection:
    """
    Create a connection to the PostgreSQL database.

    :param param_dic: dictionary containing connection parameters
    :return: connection object
    """
    conn = psycopg2.connect(
        host=param_dic["host"],
        database=param_dic["database"],
        user=param_dic["user"],
        password=param_dic["password"],
        port=param_dic["port"])
    return conn

def select_to_df(query: str, param_dic: dict) -> pd.DataFrame:
    """
    Execute a SELECT query and return the result as a DataFrame.

    :param query: SQL SELECT query
    :param param_dic: dictionary containing connection parameters
    :return: DataFrame containing query result
    """
    conn = create_connection(param_dic)
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    columns = [description[0] for description in cur.description]
    df = pd.DataFrame(rows, columns=columns)
    cur.close()
    conn.close()
    return df

def insert_values_to_table(table_name: str, df: pd.DataFrame, param_dic: dict) -> None:
    """
    Insert values from a DataFrame into a table.

    :param table_name: name of the table to insert values into
    :param df: DataFrame containing values to insert
    :param param_dic: dictionary containing connection parameters
    """
    inserted_attr_list = list(df.columns)
    records = list(df.itertuples(index=False, name=None))
    n_table_attributes = len(inserted_attr_list)
    inserted_attr_sql = f'({",".join(inserted_attr_list)})'
    inserted_values = '%s'
    if n_table_attributes > 1:
        inserted_values = inserted_values + (',%s' * (n_table_attributes - 1))
    SQL_command = f'INSERT INTO {table_name} {inserted_attr_sql} VALUES({inserted_values});'
    conn = None
    try:
        conn = create_connection(param_dic)
        cur = conn.cursor()
        cur.executemany(SQL_command, records)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def copy_from_stringio(param_dic: dict, df: pd.DataFrame, table: str) -> int:
    """
    Copy data from a DataFrame to a table using StringIO.

    :param param_dic: dictionary containing connection parameters
    :param df: DataFrame containing data to copy
    :param table: name of the table to copy data to
    :return: 1 if an error occurred, otherwise None
    """
    buffer = StringIO()
    df.to_csv(buffer, index_label='id', header=False)
    buffer.seek(0)
    
    conn = create_connection(param_dic)
    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_stringio() done")
    cursor.close()

def insert_dictionary_into_postgres(dict_obj: dict, table_name: str, jsonb_column_name: str, param_dic: dict) -> None:
    """
    Insert a dictionary as a JSON object into a PostgreSQL table.

    :param dict_obj: dictionary to insert
    :param table_name: name of the table to insert the dictionary into
    :param jsonb_column_name: name of the JSONB column to insert the dictionary into
    :param param_dic: dictionary containing connection parameters
    """
    conn = create_connection(param_dic)
    cur = conn.cursor()
    query = f"INSERT INTO {table_name} ({jsonb_column_name}) VALUES (%s)"
    json_str = json.dumps(dict_obj)
    cur.execute(query, (json_str,))
    conn.commit()
    cur.close()
    conn.close()

def connect(params_dic: dict) -> psycopg2.extensions.connection:
    """
    Connect to the PostgreSQL database server.

    :param params_dic: dictionary containing connection parameters
    :return: connection object
    """
    conn = None
    try:
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    print("Connection successful")
    return conn

def execute_values(param_dic: dict, df: pd.DataFrame, table: str) -> int:
    """
    Insert values from a DataFrame into a table using psycopg2.extras.execute_values().

    :param param_dic: dictionary containing connection parameters
    :param df: DataFrame containing values to insert
    :param table: name of the table to insert values into
    :return: 1 if an error occurred, otherwise None
    """
    conn = connect(param_dic)
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()
