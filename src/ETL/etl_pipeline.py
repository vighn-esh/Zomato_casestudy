from utils.extract import load_data, clean_data, enrich_data
from utils.staging import rename_df_cols, dtype_mapping, create_sql
import sqlite3
import sqlalchemy

# Define SQLite database path and table name
sql_staging_table_name = 'staging_data'
sqlite_staging_db_path = "../stagingDB/zomato_db_staging.sqlite"

# Set up SQLite engine using SQLAlchemy
staging_engine = sqlalchemy.create_engine(f'sqlite:///{sqlite_staging_db_path}')
initial_sql = f"CREATE TABLE IF NOT EXISTS {sql_staging_table_name} (key_pk INTEGER PRIMARY KEY"

def etl_pipeline():
    print("Starting ETL Pipeline...")

    # Step 1: Load the Data
    print("Loading data...")
    data_file = r"restaurant_data_with_lat_long_parallel.csv"
    df = load_data(data_file)

    if df is None:
        print("Failed to load data, exiting.")
        return

    # Step 2: Clean the Data
    print("Cleaning data...")
    cleaned_data = clean_data(df)

    # Step 3: Enrich the Data
    print("Enriching data with latitude and longitude...")
    enriched_data = enrich_data(cleaned_data)

    # Step 4: Prepare and write data to the staging database
    print("Preparing staging data...")

    # Create the table schema using the enriched data
    create_sql(staging_engine, enriched_data)

    # Write the enriched data to the SQLite database table
    enriched_data.to_sql(sql_staging_table_name, con=staging_engine, if_exists='append', index=False)

    print("Data successfully staged in the SQLite database!")

    print("ETL Pipeline completed successfully!")

if __name__ == "__main__":
    etl_pipeline()
