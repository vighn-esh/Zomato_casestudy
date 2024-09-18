import logging
import pandas as pd
from utils.config import read_config




def load_data_into_table(data, table_name, engine):
    """
    Load data into specified table in the database.
    """
    try:
        data.to_sql(table_name, con=engine, if_exists='append', index=False)
        logging.info(f"Data successfully loaded into {table_name}.")
    except Exception as e:
        logging.error(f"Failed to load data into {table_name}: {e}")
        raise

def load_dimension_tables(engine, data,config):
    """
    Load data into dimension tables.
    """
    # Load data into restaurant dimension table
    restaurant_columns = config['params']['restaurant_columns']
    restaurant_data = data[restaurant_columns]
    load_data_into_table(restaurant_data, 'restaurant_dimension_table', engine)
    
    # Load data into location dimension table
    location_columns = config['params']['location_columns']
    location_data = data[location_columns]
    load_data_into_table(location_data, 'location_dimension_table', engine)

def load_fact_table(engine, data,config):
    """
    Load data into fact table.
    """
    fact_columns = config['params']['fact_table_1_columns']
    fact_data = data[fact_columns]
    load_data_into_table(fact_data, 'fact_table', engine)

def load_data(engine, transformed_data,config):
    """
    Main function to load all tables.
    """
    logging.info("Starting the data load process.")
    load_dimension_tables(engine, transformed_data,config)
    load_fact_table(engine, transformed_data,config)
    logging.info("Data loading completed successfully.")
