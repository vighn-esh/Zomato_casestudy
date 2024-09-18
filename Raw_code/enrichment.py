import requests
import pandas as pd
import numpy as np
import re
from bs4 import BeautifulSoup
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry  # Correct import

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_session_with_retries():
    session = requests.Session()
    
    # Define retry strategy
    retry = Retry(
        total=5,  # Total number of retries
        backoff_factor=1,  # Backoff factor to apply between attempts
        status_forcelist=[500, 502, 503, 504],  # Retry on these status codes
        allowed_methods=["GET"]  # Retry only on GET requests
    )
    
    # Attach the retry strategy to the session
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    return session

def webScrape(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    session = create_session_with_retries()
    
    try:
        r = session.get(url, headers=headers)
    
        if r.status_code == 202:
            
            
        
            r.raise_for_status()  # Raise an error for HTTP errors other than 404
            
            soup = BeautifulSoup(r.content, 'html.parser')
            
            img_tag = extractlocations(soup)
            return img_tag
        
    except requests.RequestException as e:
        logging.error(f"Request failed: {e} - URL: {url}")
        return np.nan

def extractlocations(soup):
    img_tag = str(soup)
    if img_tag:
        match = re.search(r'center=([0-9\.\-]+,[0-9\.\-]+)', img_tag)
        if match:
            return match.group(1)
    logging.warning("Image tag or location not found.")
    return np.nan

def enhanceDF(df):
    df['latitude'] = np.nan
    df['longitude'] = np.nan
    
    for index, row in df.iterrows():
        location_str = webScrape(row["URL"])
        
        if location_str and isinstance(location_str, str):
            try:
                lat, lon = map(float, location_str.split(','))
                df.at[index, 'latitude'] = lat
                df.at[index, 'longitude'] = lon
            except ValueError:
                logging.error(f"Failed to convert location '{location_str}' to float. - URL: {row['URL']}")
        else:
            logging.info(f"No valid location found for URL: {row['URL']}")
    
    df = df.drop(columns=['URL'])
    
    return df

# Example usage
if __name__ == "__main__":
    # Test URL
    test_url = "https://www.zomato.com/agra/the-salt-cafe-kitchen-bar-tajganj/info"
    print(webScrape(test_url))
