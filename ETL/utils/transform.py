



import pandas as pd
import logging

def fetch_staging_data(engine, staging_table_name):
    '''Fetch data from the staging table.'''
    with engine.connect() as conn:
        query = f"SELECT * FROM {staging_table_name}"
        staging_df = pd.read_sql(query, conn)
    return staging_df

def drop_columns(data, cols_to_drop):
    '''Drop specified columns from DataFrame.'''
    data.drop(columns=cols_to_drop, inplace=True)

def remove_columns_with_high_null_percentage(data, null_limit):
    '''Remove columns with missing values greater than a given null percentage limit.'''
    cols_to_drop = [col for col in data.columns if (data[col].isnull().sum() / len(data)) * 100 > null_limit]
    data.drop(columns=cols_to_drop, inplace=True)
    logging.info(f"Dropped columns: {cols_to_drop}")
    return data

def drop_duplicates(data):
    '''Drop duplicate rows from the DataFrame.'''
    data.drop_duplicates(inplace=True)
    return data

def convert_object_to_numeric(df):
    '''Convert object-type columns to numeric where possible.'''
    for column in df.select_dtypes(include='object').columns:
        try:
            df[column] = pd.to_numeric(df[column], errors='raise', downcast='integer')
            logging.info(f"Converted {column} to integer type.")
        except (ValueError, TypeError):
            try:
                df[column] = pd.to_numeric(df[column], errors='raise')
                df[column] = df[column].astype(float)
                logging.info(f"Converted {column} to float type.")
            except (ValueError, TypeError):
                logging.info(f"Left {column} as object type.")
    return df

def get_meta_info(df):
    '''Retrieve meta information about the DataFrame (data types and unique records).'''
    temp_pd = pd.DataFrame(df.dtypes, columns=['data_type']).reset_index()
    unique_records = pd.DataFrame(df.nunique(), columns=['unique_records']).reset_index()
    meta_df = pd.merge(temp_pd, unique_records, on='index')
    logging.info(f"Meta information: {meta_df}")
    return meta_df

def create_dimension_table(engine, data, table_name, columns, config):
    '''Create dimension table for the given data.'''
    initial_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY"
    for col in columns:
        dtype = str(data[col].dtype)
        sqlite_dtype = config['params']['dtype_mapping'].get(dtype)
        if sqlite_dtype:
            initial_sql += f", {col} {sqlite_dtype}"
    initial_sql += ")"
    execute_sql(engine, initial_sql)
    logging.info(f"Created dimension table: {table_name}")

def create_fact_table(engine, data, config):
    '''Create the fact table with foreign keys.'''
    sql_table_name = 'fact_table'
    columns = config['params']['fact_table_1_columns']
    initial_sql = f"CREATE TABLE IF NOT EXISTS {sql_table_name} (fact_id INTEGER PRIMARY KEY"
    for col in columns:
        dtype = str(data[col].dtype)
        sqlite_dtype = config['params']['dtype_mapping'].get(dtype)
        if sqlite_dtype and col not in ['restaurant_id', 'location_id']:
            initial_sql += f", {col} {sqlite_dtype}"
    
    # Add foreign keys
    initial_sql += ", restaurant_id INTEGER, location_id INTEGER"
    initial_sql += ", FOREIGN KEY (restaurant_id) REFERENCES restaurant_dim_table(id)"
    initial_sql += ", FOREIGN KEY (location_id) REFERENCES location_dim_table(id)"
    initial_sql += ")"
    
    execute_sql(engine, initial_sql)
    logging.info("Created fact table.")

def execute_sql(engine, sql):
    '''Execute SQL commands.'''
    try:
        conn = engine.raw_connection()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        logging.info("SQL executed successfully.")
    except Exception as e:
        logging.error(f"SQL execution failed: {e}")
        raise

def add_auto_increment_ids(data, start_id=1):
    '''Add auto-increment IDs to DataFrame.'''
    if 'restaurant_id' not in data.columns:
        data['restaurant_id'] = range(start_id, start_id + len(data))
    if 'location_id' not in data.columns:
        data['location_id'] = range(start_id, start_id + len(data))
    return data

def transform(engine, staging_table_name, config):
    '''Perform all transformation activities.'''
    # Fetch data from staging
    data = fetch_staging_data(engine, staging_table_name)
    
    # Drop unwanted columns
    drop_columns(data, config['params']['columns_to_drop'])
    
    # Remove columns with high null percentage
    data = remove_columns_with_high_null_percentage(data, config['params']['null_percentage_limit'])
    
    # Drop duplicate rows
    data = drop_duplicates(data)
    
    # Convert object columns to numeric
    data = convert_object_to_numeric(data)
    
    # Get meta information
    meta_info = get_meta_info(data)
    
    # Add auto-increment IDs
    data = add_auto_increment_ids(data)
    
    # Create dimension and fact tables
    create_dimension_table(engine, data, 'restaurant_dimension_table', config['params']['restaurant_columns'], config)
    create_dimension_table(engine, data, 'location_dimension_table', config['params']['location_columns'], config)
    create_fact_table(engine, data, config)

    logging.info("Transformation completed.")
    return data
