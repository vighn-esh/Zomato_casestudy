# main.py

import pandas as pd
import sqlalchemy
import logging
from load import load_data, preprocess_data
from sklearn.model_selection import train_test_split  # Add this line at the top of load.py
from model import train_linear_regression, train_decision_tree, train_random_forest, evaluate_model
import yaml
import os
sql_staging_table_name = 'staging_data'
sqlite_db_path = "../Main_DB/zomato_DB.sqlite"

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



def read_config(file_path):
    try:
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        logging.error(f"An error occurred while reading the config file: {e}")

config = read_config('ML_config.yml')

def initialize_db(engine):
    """Check the database connection and ensure it's active."""
    with engine.connect() as conn:
        logging.info("Database created and connected successfully.")

def etl_predictive_pipeline():
    """Main ETL and Predictive Analysis Pipeline."""
    logging.info("Starting ETL and Predictive Analysis Pipeline...")
    
    # Load configuration
   

    # Set up database connection
    
    os.makedirs(os.path.dirname(sqlite_db_path), exist_ok=True)
    engine = sqlalchemy.create_engine(f'sqlite:///{sqlite_db_path}')
    initialize_db(engine)

    # Step 1: Load Data
    staging_table_names = [config['params']['fact_table_1'], config['params']['dimension_table_1'], config['params']['dimension_table_2']]
    df = load_data(engine, staging_table_names)

    # Step 2: Preprocess Data
    X_train, X_test, y_train, y_test = preprocess_data(df, config)

    # Step 3: Train Models and Evaluate
    # Train Linear Regression
    lr_model = train_linear_regression(X_train, y_train)
    mse_lr, mae_lr = evaluate_model(lr_model, X_test, y_test)
    logging.info(f"Linear Regression - MSE: {mse_lr}, MAE: {mae_lr}")

    # Train Decision Tree
    dt_model = train_decision_tree(X_train, y_train, config)
    mse_dt, mae_dt = evaluate_model(dt_model, X_test, y_test)
    logging.info(f"Decision Tree - MSE: {mse_dt}, MAE: {mae_dt}")

    # Train Random Forest
    rf_model = train_random_forest(X_train, y_train, config)
    mse_rf, mae_rf = evaluate_model(rf_model, X_test, y_test)
    logging.info(f"Random Forest - MSE: {mse_rf}, MAE: {mae_rf}")

if __name__ == "__main__":
    config=read_config("./ML_config.yml")
    etl_predictive_pipeline()
