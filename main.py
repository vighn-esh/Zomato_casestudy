from src.extract.extract import extract
from src.transform.staging import stage_data
from src.transform.transform import transform
from src.load.load import load

import logging

logging.basicConfig(
    filename="logs/etl_pipeline.log", 
    level=logging.INFO,  
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S" 
)

def main():
    logging.info("Starting extraction process...")
    extracted_data = extract()
    logging.info("Extraction complete.")
    logging.info(f"Extracted data dimension: {extracted_data.shape}")
    
    logging.info("Starting staging process...")
    stage_data(extracted_data)
    logging.info("Staging complete.")
    
    logging.info("Starting transformation process...")
    transformed_data = transform()
    logging.info(f"Transformed data dimension: {transformed_data.shape}")
    logging.info("Transformation complete.")
    
    logging.info("Starting loading process...")
    load()
    logging.info("Loading complete.")

if __name__ == "__main__":
    main()
