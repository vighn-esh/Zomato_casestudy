import pandas as pd
from sqlalchemy import create_engine
from utils.config import read_config

config = read_config('..\config.yml')

def rename_df_cols(df):
    '''Input a dataframe, outputs same dataframe with No Space in column names'''
    col_no_space = dict((i, i.replace(' ', '')) for i in list(df.columns))
    df.rename(columns=col_no_space, inplace=True)
    return df

def dtype_mapping():
    '''Returns a dictionary to refer correct data type for SQLite'''
    return config['params']['sqlite_dtype_mapping']  

def create_sql(engine, df, sql):
    '''input engine: SQLite engine, df: dataframe that you would like to create a schema for,
       outputs SQLite schema creation'''


    df = rename_df_cols(df)
    

    col_list_dtype = [(i, str(df[i].dtype)) for i in list(df.columns)]
    

    map_data = dtype_mapping()

    for col_name, dtype in col_list_dtype:
        sqlite_dtype = map_data.get(dtype, 'TEXT') 
        sql += f", {col_name} {sqlite_dtype}"

    sql += ')'

    print('\n', sql, '\n') 

    try:
        conn = engine.raw_connection()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        print("Table schema created successfully.")
    except Exception as e:
        print(f"Failed to create schema: {e}")
    
    return sql
