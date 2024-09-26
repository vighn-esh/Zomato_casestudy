import json
import os

def load_config():
    
    try:
        script_dir = os.path.dirname(__file__)
        file_path = os.path.join(script_dir, '..', '..', "config.json")
      
        with open(file_path, 'r') as file:
            data = json.load(file)
        processed_data = data  

        return processed_data

    except FileNotFoundError:
        print(f"Error: The file at {file_path} was not found.")
    except json.JSONDecodeError:
        print("Error: Failed to decode JSON.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")