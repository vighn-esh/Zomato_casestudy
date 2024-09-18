import pandas as pd
from bs4 import BeautifulSoup
from joblib import Parallel, delayed
import multiprocessing
from tqdm import tqdm
import os
import requests
from requests.exceptions import SSLError, ConnectionError

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

# Load the dataset or resume from saved progress
output_file = r"restaurant_data_with_lat_long_parallel.csv"
if os.path.exists(output_file):
    # Load the partially processed data
    df = pd.read_csv(output_file)
else:
    # If no previous progress, load the original dataset
    df = pd.read_csv(r"..\data\combined_data.csv")
    # Initialize latitude and longitude columns
    df['Latitude'] = None
    df['Longitude'] = None

# Filter rows that have not been processed (i.e., no latitude and longitude)
df_to_process = df[df['Latitude'].isnull() & df['Longitude'].isnull()]
data_rows = len(df_to_process)

# Convert 'RATING' column to numeric type
df['RATING'] = pd.to_numeric(df['RATING'], errors='coerce')

# Use multiprocessing to speed up processing
num_cores = multiprocessing.cpu_count()

# Function to extract latitude and longitude from a URL using tqdm
def extract_lat_long_tqdm(row):
    return extract_lat_long(row['URL'])

# Batch processing: process in chunks to save progress frequently
batch_size = 100  # Save progress every 100 rows
for start in tqdm(range(0, data_rows, batch_size), desc="Processing Batches"):
    end = min(start + batch_size, data_rows)
    batch_df = df_to_process.iloc[start:end]
    
    # Apply parallel processing on each batch
    results = Parallel(n_jobs=num_cores)(
        delayed(extract_lat_long_tqdm)(row) for index, row in batch_df.iterrows()
    )
    
    # Assign the latitude and longitude to the original DataFrame
    df.loc[batch_df.index, ['Latitude', 'Longitude']] = results
    
    # Save the DataFrame to the output CSV file
    df.to_csv(output_file, index=False)

print("Processing complete.")
