#This file is used to webscrap from the combined dataset initially using parallelism approach. Could generate a data
#10000 data points


import pandas as pd
from bs4 import BeautifulSoup
from joblib import Parallel, delayed
import multiprocessing
from tqdm import tqdm

# Function to extract latitude and longitude from a URL
def extract_lat_long(url):
    # Move the imports inside the function
    import requests

    header = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36'
    }
    response = requests.get(url, headers=header)
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
    return None, None

# Read your dataset into a DataFrame
df = pd.read_csv("..\data\combined_data.csv")

data_rows = len(df) 

# Convert 'RATING' column to numeric type
df['RATING'] = pd.to_numeric(df['RATING'], errors='coerce')  # 'coerce' will convert non-numeric values to NaN

# Use multiprocessing to speed up processing
num_cores = multiprocessing.cpu_count()  # Number of CPU cores

# Function to extract latitude and longitude from a URL using tqdm
def extract_lat_long_tqdm(row):
    return extract_lat_long(row['URL'])

# Apply tqdm to Parallel processing
results = Parallel(n_jobs=num_cores)(
    delayed(extract_lat_long_tqdm)(row) for index, row in tqdm(df.iterrows(), total=data_rows, desc="Processing")
)

# Assign results to DataFrame
df[['Latitude', 'Longitude']] = results

# Save the updated DataFrame to a new CSV file
df.to_csv(r"restaurant_data_with_lat_long_parallel.csv", index=False)