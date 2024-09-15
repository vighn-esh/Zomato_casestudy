# etl_pipeline.py
from utils.extract import load_data, clean_data, enrich_data
from utils.staging import rename_df_cols,dtype_mapping,create_sql

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

    # step 4 preparing the staging data:
     

    print("ETL Pipeline completed successfully!")


if __name__ == "__main__":
    etl_pipeline()
