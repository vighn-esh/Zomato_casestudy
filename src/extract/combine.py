import os
import logging
import pandas as pd

def iterate(path):
    combined_df = pd.DataFrame()

    for root, _, files in os.walk(path):
        for f in files:
            if f.endswith(".csv"):
                file_path = os.path.join(root, f)
                try:
                    df = pd.read_csv(file_path, sep="|", header=0)
                    combined_df = pd.concat([combined_df, df], ignore_index=True)
                except Exception as e:
                    logging.error(f"Error reading file {file_path}: {e}")

    logging.info("Converted to DataFrame successfully")
    combined_df.drop_duplicates(inplace=True)
    return combined_df
