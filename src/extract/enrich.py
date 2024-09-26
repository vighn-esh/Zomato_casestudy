from joblib import Parallel, delayed
import multiprocessing
from tqdm import tqdm
import requests
from requests.exceptions import SSLError, ConnectionError
import pandas as pd
from bs4 import BeautifulSoup
import os

def enrich(df,output_file):
    
    num_cores = multiprocessing.cpu_count()

 
    batch_size = 100
    df_to_process = df[df['Latitude'].isnull() & df['Longitude'].isnull()]

    for start in tqdm(range(0, len(df_to_process), batch_size), desc="Processing Batches"):
        end = min(start + batch_size, len(df_to_process))
        batch_df = df_to_process.iloc[start:end]
        results = Parallel(n_jobs=num_cores)(
            delayed(extract_lat_long)(row['URL']) for index, row in batch_df.iterrows()
        )
        df.loc[batch_df.index, ['Latitude', 'Longitude']] = results
        df.to_csv(output_file, index=False)
    print("Enrichment complete.")
    return df

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