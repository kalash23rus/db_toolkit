import psycopg2
import pandas as pd
from io import StringIO
import json
from copy import deepcopy
import os
import numpy as np
import psycopg2.extras as extras

__version__ = 'dev'

def dict_to_json(value: dict):
    return json.dumps(value)

def array_generator4sql(items: list):
    if len(items) > 1:
        items = tuple(items)
    else:
        items.append(items[0])
        items = tuple(items)
    return items

def create_connection(param_dic):
    conn = psycopg2.connect(
        host=param_dic["host"],
        database=param_dic["database"],
        user=param_dic["user"],
        password=param_dic["password"],
        port=param_dic["port"])
    return conn

def execute_sql(query,param_dic):
    conn = create_connection(param_dic)
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.close()
    return "Come to Kazahstan, It is nice"

def select_to_df(query,param_dic):
    conn = create_connection(param_dic)
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    columns = [description[0] for description in cur.description]
    df=pd.DataFrame(rows,columns=columns)
    cur.close()
    conn.close()
    return df

def insert_values_to_table(
                           table_name,
                           df,param_dic):
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
        cur.executemany(SQL_command,records)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def copy_from_stringio(param_dic, df, table):
    """
    Here we are going save the dataframe in memory 
    and use copy_from() to copy it to the table
    """
    # save dataframe to an in memory buffer
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


def insert_json_to_table(dict_obj,table_name,json_attr_name,param_dic):
    json_obj = dict_to_json(value=dict_obj)
    SQL_command = f'''
        INSERT INTO
            {table_name}({json_attr_name}) 
        VALUES
            ('{json_obj}')
    '''
  
    try:
            conn = create_connection(param_dic)
            cur = conn.cursor()
            cur.execute(SQL_command)
            conn.commit()
            cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    finally:
        if conn is not None:
            conn.close()
            
def dict_to_json(value: dict):
    return json.dumps(value)
            
def insert_dictionary_into_postgres(dict_obj, table_name, jsonb_column_name,param_dic):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host=param_dic["host"],
        database=param_dic["database"],
        user=param_dic["user"],
        password=param_dic["password"],
        port=param_dic["port"]
    )

    # Open a cursor to perform database operations
    cur = conn.cursor()

    # Generate a query string for inserting the JSON object
    query = f"INSERT INTO {table_name} ({jsonb_column_name}) VALUES (%s)"

    # Convert the dictionary to a JSON string
    json_str = json.dumps(dict_obj)

    # Execute the query with the JSON string as a parameter
    cur.execute(query, (json_str,))

    # Commit the transaction
    conn.commit()

    # Close the cursor and the connection
    cur.close()
    conn.close()
    
def insert_json_to_table(dict_obj,table_name,json_attr_name,param_dic):
    json_obj = dict_to_json(value=dict_obj)
    SQL_command = f'''
        INSERT INTO
            {table_name}({json_attr_name}) 
        VALUES
            ('{json_obj}')
    '''
  
    try:
            conn = create_connection(param_dic)
            cur = conn.cursor()
            cur.execute(SQL_command)
            conn.commit()
            cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    finally:
        if conn is not None:
            conn.close()

def doble_apostroph(container):
    """finds any COMMA lurking in any string anywhere in a standard sequence object,
    including in a nested sequence of sequences of arbitrary depth, and DESTROYS it"""

    copy = deepcopy(container)

    if isinstance(copy, str):
        return copy.replace("'", "''")
    else:
        if isinstance(copy, list):
            for i, v in enumerate(copy):
                copy[i] = doble_apostroph(v)
        elif isinstance(copy, tuple):
            copy = list(copy)
            for i, v in enumerate(copy):
                copy[i] = doble_apostroph(v)
            copy = tuple(copy)
        elif isinstance(copy, dict):
            for k in copy.keys():
                copy[k] = doble_apostroph(copy[k])

    return copy

def remove_N(item):
    new_annotations = []
    for ann in item['annotations']:
        new_annotations.append({k: v for k, v in ann.items() if pd.notna(v)})
    item['annotations'] = new_annotations
    
    return item

def array_generator4sql(items: list):
    if len(items) > 1:
        items = tuple(items)
    else:
        items.append(items[0])
        items = tuple(items)
    return items

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    print("Connection successful")
    return conn
# conn = connect(param_dic)

def execute_values(param_dic, df, table):
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    """
    conn = connect(param_dic)
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
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
