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
    # Remove duplicate rows
    combined_df.drop_duplicates(inplace=True)
    return combined_df

def main():
    # Path to the CSV files
    path = "../data/input"
    combined_csv_file = '../data/combined_data.csv'

    try:
        # Check if the combined CSV file exists
        if os.path.exists(combined_csv_file):
            logging.info(f"{combined_csv_file} exists. Loading DataFrame from CSV.")
            combined_dataframe = pd.read_csv(combined_csv_file)
        else:
            logging.info(f"{combined_csv_file} does not exist. Creating DataFrame from CSV files in directory.")
            combined_dataframe = iterate(path)
            # Save the DataFrame to a CSV file
            combined_dataframe.to_csv(combined_csv_file, index=False)
        
        logging.info("Merging the CSV files complete")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
