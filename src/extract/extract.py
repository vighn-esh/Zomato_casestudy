import os
import logging
import pandas as pd
from .combine import iterate
from ..utils.config import load_config
from .enrich import enrich
def extract():

    config=load_config()
    raw_data =config["file_path"]["raw_data"]
    initial_data=config["file_path"]["initial_data"]
    output_file=config["file_path"]["enriched_data"]
    print(output_file,os.getcwd())

    try:
        
        if os.path.exists(output_file):
            logging.info(f"{output_file} exists. Loading DataFrame from CSV.")
            print("filefound")
            combined_dataframe = pd.read_csv(output_file)
           
        else:
            logging.info(f"{output_file} does not exist. Creating DataFrame from CSV files in directory.")
            combined_dataframe = iterate(raw_data)
            enriched_dataframe= enrich(combined_dataframe,output_file)
            enriched_dataframe.to_csv(output_file, index=False)
            
        
        logging.info("Merging the CSV files complete")
        return combined_dataframe

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")