from utils.extract import extract
from utils.staging import rename_df_cols, dtype_mapping, create_sql
from utils.transform import transform
from utils.load import load_data  # Import the load function from the load.py
from utils.config import read_config
import sqlite3
import sqlalchemy
import logging
import os

# Define SQLite database path and table name
sql_staging_table_name = 'staging_data'
sqlite_db_path = "../Main_DB/zomato_DB.sqlite"
input_path="../../data/input"
output_file="../../data/restaurant_data1.csv"

# Set up SQLite engine using SQLAlchemy
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def initialize_db(engine):
    # Check connection and create the database if necessary
    with engine.connect() as conn:
        logging.info("Database created and connected successfully.")

config = read_config('config.yml')

def etl_pipeline():
    logging.info("Starting ETL Pipeline...")

    # Step 1: Load the Data
    enriched_data = extract(input_path, output_file)
    logging.info(f"Data extracted. Dimensions: {enriched_data.shape}")

    # Step 2: Prepare and write data to the staging database
    os.makedirs(os.path.dirname(sqlite_db_path), exist_ok=True)
    engine = sqlalchemy.create_engine(f'sqlite:///{sqlite_db_path}')
    initialize_db(engine)
    logging.info("Preparing staging data...")
    
    create_sql(engine, enriched_data, config)
    enriched_data.to_sql(sql_staging_table_name, con=engine, if_exists='append', index=False)
    logging.info(f"Data successfully staged in the SQLite database! Dimensions: {enriched_data.shape}")

    # Step 3: Transform the data (Transform Step)
    logging.info("Transforming data...")
    transformed_data = transform(engine, sql_staging_table_name, config)
    logging.info(f"Data transformation completed. Dimensions: {transformed_data.shape}")

    # Step 4: Load the transformed data into final tables (Load Step)
    logging.info("Loading data into final tables...")
    load_data(engine, transformed_data, config)
    logging.info("Data successfully loaded into the dimension and fact tables!")

    logging.info("ETL Pipeline completed successfully!")

if __name__ == "__main__":
    etl_pipeline()
