import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import logging
from ..utils.db_utils import execute_sql
from ..utils.config import load_config
from ..utils.pd_utils import load_df




def remove_columns_with_high_null_percentage(data,null_limit):
    
    cols_to_drop = [col for col in data.columns if (data[col].isnull().sum() / len(data)) * 100 > null_limit]
    
    data.drop(columns=cols_to_drop, inplace=True)
    logging.info(f"Dropped columns: {cols_to_drop}")
    return data

def drop_duplicates(data):
    data.drop_duplicates(inplace=True)
    return data

def convert_object_to_numeric(df):
    for column in df.select_dtypes(include='object').columns:
        try:
            # Attempt to convert to integer
            df[column] = pd.to_numeric(df[column], errors='raise', downcast='integer')
            logging.info(f"Converted {column} to integer type.")
        except (ValueError, TypeError):
            try:
                # Attempt to convert to float
                df[column] = pd.to_numeric(df[column], errors='raise')
                df[column] = df[column].astype(float)
                logging.info(f"Converted {column} to float type.")
            except (ValueError, TypeError):
                logging.info(f"Left {column} as object type.")
    return df

def create_dimension_table(engine, data, table_name, columns,config):
    '''Create dimension table (generic for restaurant or location) using SQLite syntax'''
    initial_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY"
    
    for col in columns:
        dtype = str(data[col].dtype)
        sqlite_dtype = config['params']['dtype_mapping'].get(dtype)
        if sqlite_dtype:
            initial_sql += f", {col} {sqlite_dtype}"
    
    initial_sql += ")"
    
    execute_sql(engine, initial_sql)
    logging.info(f"Created dimension table: {table_name}")

def create_fact_table(engine, data,config):
    '''Create fact table using SQLite syntax'''
    sql_table_name = 'fact_table'
    columns = config['params']['fact_table_1_columns']
    
    initial_sql = f"CREATE TABLE IF NOT EXISTS {sql_table_name} (fact_id INTEGER PRIMARY KEY"
    
    for col in columns:
        dtype = str(data[col].dtype)
        sqlite_dtype = config['params']['dtype_mapping'].get(dtype)
        if sqlite_dtype:
            initial_sql += f", {col} {sqlite_dtype}"
   
    initial_sql += ", restaurant_id INTEGER, location_id INTEGER"
    initial_sql += ", FOREIGN KEY (restaurant_id) REFERENCES restaurant_dim_table(id)"
    initial_sql += ", FOREIGN KEY (location_id) REFERENCES location_dim_table(id)"
    initial_sql += ")"
    
    execute_sql(engine, initial_sql)
    logging.info("Created fact table.")


def transform():
   
    config = load_config()
    db_path = config["file_path"]["staging_db"]
    table_name = "staging_db"
    engine = create_engine(db_path)
    df = load_df(engine, table_name)
    null_limit = config['params']['null_limit']
    df = remove_columns_with_high_null_percentage(df, null_limit)
    df = drop_duplicates(df)
    df = convert_object_to_numeric(df)
    restaurant_columns = config['params']['restaurant_dimension_table_columns']
    location_columns = config['params']['location_dimension_table_columns']
    create_dimension_table(engine, df, 'restaurant_dim_table', restaurant_columns, config)
    create_dimension_table(engine, df, 'location_dim_table', location_columns, config)
    create_fact_table(engine, df, config)
    logging.info("Transformation process completed successfully.")
    return df

    
    




