import yaml
import logging
import sqlalchemy

def read_config(file_path):
    try:
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        logging.error(f"An error occurred while reading the config file: {e}")