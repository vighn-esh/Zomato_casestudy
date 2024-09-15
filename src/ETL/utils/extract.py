# Transform.py
import logging
import pandas as pd
from bs4 import BeautifulSoup
from joblib import Parallel, delayed
import multiprocessing
from tqdm import tqdm
import requests
from requests.exceptions import SSLError, ConnectionError
import os

# Function to extract latitude and longitude from a URL
def extract_lat_long(url):
    try:
        header = {
            'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36'
        }
        response = requests.get(url, headers=header, timeout=10)  # Adding timeout to avoid hanging
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            script_content = soup.find('script', string=lambda text: text and 'window.__PRELOADED_STATE__' in text)
            if script_content:
                latitude_start_index = script_content.string.find(r'"latitude\":\"') + len(r'"latitude\":\"')
                latitude_end_index = script_content.string.find('",', latitude_start_index)
                latitude = script_content.string[latitude_start_index:latitude_end_index-1]

                longitude_start_index = script_content.string.find(r'"longitude\":\"') + len(r'"longitude\":\"')
                longitude_end_index = script_content.string.find('",', longitude_start_index)
                longitude = script_content.string[longitude_start_index:longitude_end_index-1]

                return latitude, longitude
    except (SSLError, ConnectionError) as e:
        print(f"Error fetching {url}: {e}")
    except Exception as e:
        print(f"Unexpected error with {url}: {e}")
    return None, None

# Function to load data
def load_data(input_path, output_file):
    combined_df = pd.DataFrame()

    try:
        # Check if the combined CSV file already exists
        if os.path.exists(output_file):
            logging.info(f"{output_file} exists. Loading DataFrame from CSV.")
            combined_df = pd.read_csv(output_file)
        else:
            logging.info(f"{output_file} does not exist. Creating DataFrame from CSV files in directory.")
            # Iterate over all CSV files in the specified directory
            for root, _, files in os.walk(input_path):
                for f in files:
                    if f.endswith(".csv"):
                        file_path = os.path.join(root, f)
                        try:
                            # Read the CSV file using '|' as the separator
                            df = pd.read_csv(file_path, sep="|", header=0)
                            combined_df = pd.concat([combined_df, df], ignore_index=True)
                        except Exception as e:
                            logging.error(f"Error reading file {file_path}: {e}")
            # Remove duplicates and save the combined DataFrame
            combined_df.drop_duplicates(inplace=True)
            combined_df.to_csv(output_file, index=False)
            logging.info("CSV files merged and saved successfully.")
        
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise e  # Optionally re-raise the exception for further handling

    return combined_df


# Function to enrich data with latitude and longitude
def enrich_data(df, output_file="enriched_data.csv"):
    # Use multiprocessing to speed up processing
    num_cores = multiprocessing.cpu_count()

    # Batch processing to save progress frequently
    batch_size = 100
    df_to_process = df[df['Latitude'].isnull() & df['Longitude'].isnull()]  # Only process rows that don't have lat-long

    for start in tqdm(range(0, len(df_to_process), batch_size), desc="Processing Batches"):
        end = min(start + batch_size, len(df_to_process))
        batch_df = df_to_process.iloc[start:end]

        # Apply parallel processing on each batch
        results = Parallel(n_jobs=num_cores)(
            delayed(extract_lat_long)(row['URL']) for index, row in batch_df.iterrows()
        )

        # Assign the latitude and longitude to the original DataFrame
        df.loc[batch_df.index, ['Latitude', 'Longitude']] = results

        # Save the DataFrame to the output CSV file
        df.to_csv(output_file, index=False)

    print("Enrichment complete.")
    return df
