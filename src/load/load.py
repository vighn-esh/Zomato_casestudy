import pandas as pd
from sqlalchemy import create_engine
import logging
from ..utils.config import load_config
from ..utils.pd_utils import load_df

def add_auto_increment_ids(data, start_id=1):
    '''Add auto-increment IDs to DataFrame'''
    # If restaurant_id and location_id columns don't exist, create them
    if 'restaurant_id' not in data.columns:
        data['restaurant_id'] = range(start_id, start_id + len(data))
    if 'location_id' not in data.columns:
        data['location_id'] = range(start_id, start_id + len(data))
    return data

def process_data(data, start_id=1):
    '''Process data and add auto-increment IDs'''
    # If restaurant_id and location_id columns exist, append values to existing data
    if 'restaurant_id' in data.columns and 'location_id' in data.columns:
        max_restaurant_id = data['restaurant_id'].max()
        max_location_id = data['location_id'].max()
        new_data = add_auto_increment_ids(data, max(max_restaurant_id, max_location_id) + 1)
        #print("shape of data after loading in new_data is:",new_data.shape)
        return new_data
    # If restaurant_id and location_id columns don't exist, create them and add values
    else:
        return add_auto_increment_ids(data, start_id)


def load_data_into_table(data, table_name, engine, columns=None):
    '''Load data into specified table'''
    #print("columns in data before processing:", data.columns)
    processed_data = process_data(data)
    #print("Processed data shape is:", processed_data.shape)
    if columns is None:
        columns = processed_data.columns
    #print(columns)
    #print(processed_data.head(3))    
    # Load data into the table
    processed_data[columns].to_sql(table_name, con=engine, if_exists='append', index=False)


def load():
     
    config = load_config()
    db_path = config["file_path"]["staging_db"]
    engine = create_engine(db_path)
    data=load_df(engine,"staging_db")
    db_path = config["file_path"]["main_db"]
    engine = create_engine(db_path)
    data['restaurant_id'] = data.index
    data['location_id'] = data.index

    restaurant_columns = config['params'] ['restaurant_dimension_table_columns']
    #restaurant_columns_name = config['params'] ['dimension_table_1_columns']
    load_data_into_table(data, "restaurant_dimension_table", engine, columns=restaurant_columns)
  

    location_columns = config['params'] ['location_dimension_table_columns']  
    #location_columns_name = config['params'] ['dimension_table_2_columns']
    load_data_into_table(data, "location_dimension_table", engine, columns=location_columns)


    fact_table_name = config['params'] ['fact_table_1']  
    fact_columns = config['params'] ['fact_table_1_columns']

    load_data_into_table(data, "fact_table", engine, columns=fact_columns)
 
    logging.info("Loading data completed.")

   