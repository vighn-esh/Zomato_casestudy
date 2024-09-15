import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import logging
from config import read_config


# Read configuration
config = read_config('..\config.yml')

def fetch_staging_data(engine, staging_table_name):
    '''Fetch data from staging table'''
    with engine.connect() as conn:
        query = f"SELECT * FROM {staging_table_name}"
        staging_df = pd.read_sql(query, conn)
    return staging_df

def drop_columns(data, cols_to_drop):
    '''Drop specified columns from DataFrame'''
    data.drop(columns=cols_to_drop, inplace=True)

def remove_columns_with_high_null_percentage(data):
    '''Remove columns with missing values greater than threshold in config file'''
    null_limit = config['params']['null_percentage_limit']
    cols_to_drop = [col for col in data.columns if (data[col].isnull().sum() / len(data)) * 100 > null_limit]
    
    data.drop(columns=cols_to_drop, inplace=True)
    logging.info(f"Dropped columns: {cols_to_drop}")
    return data


def drop_duplicates(data):
    '''Drop duplicate rows from DataFrame'''
    data.drop_duplicates(inplace=True)
    return data

def convert_object_to_numeric(df):
    for column in df.select_dtypes(include='object').columns:
        try:
            # Attempt to convert to integer
            df[column] = pd.to_numeric(df[column], errors='raise', downcast='integer')
            logging.info(f"Converted {column} to integer type.")
        except (ValueError, TypeError):
            try:
                # Attempt to convert to float
                df[column] = pd.to_numeric(df[column], errors='raise')
                df[column] = df[column].astype(float)
                logging.info(f"Converted {column} to float type.")
            except (ValueError, TypeError):
                logging.info(f"Left {column} as object type.")
    return df

# Function to inspect and clean a specific column
def inspect_and_clean_column(df, column_name):
    unique_values = df[column_name].unique()
    logging.info(f"Unique values in {column_name} before cleaning: {unique_values}")

    
    df[column_name] = df[column_name].str.strip()
    df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
    
    unique_values_after = df[column_name].unique()
    logging.info(f"Unique values in {column_name} after cleaning: {unique_values_after}")
    
    return df


# Function to get meta information about the data (data types, unique records)
def get_meta_info(df):
    temp_pd = pd.DataFrame(df.dtypes, columns=['data_type']).reset_index()
    unique_records = pd.DataFrame(df.nunique(), columns=['unique_records']).reset_index()
    meta_df = pd.merge(temp_pd, unique_records, on='index')
    logging.info(f"Meta information about the data: {meta_df}")
    return meta_df


def create_dimension_table(engine, data, table_name, columns):
    '''Create dimension table (generic for restaurant or location) using SQLite syntax'''
    initial_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY"
    
    for col in columns:
        dtype = str(data[col].dtype)
        sqlite_dtype = config['params']['dtype_mapping'].get(dtype)
        if sqlite_dtype:
            initial_sql += f", {col} {sqlite_dtype}"
    
    initial_sql += ")"
    
    execute_sql(engine, initial_sql)
    logging.info(f"Created dimension table: {table_name}")

def create_fact_table(engine, data):
    '''Create fact table using SQLite syntax'''
    sql_table_name = 'fact_table'
    columns = config['params']['fact_table_1_columns']
    
    initial_sql = f"CREATE TABLE IF NOT EXISTS {sql_table_name} (fact_id INTEGER PRIMARY KEY"
    
    for col in columns:
        dtype = str(data[col].dtype)
        sqlite_dtype = config['params']['dtype_mapping'].get(dtype)
        if sqlite_dtype:
            initial_sql += f", {col} {sqlite_dtype}"
    
    # Add foreign keys for SQLite
    initial_sql += ", restaurant_id INTEGER, location_id INTEGER"
    initial_sql += ", FOREIGN KEY (restaurant_id) REFERENCES restaurant_dim_table(id)"
    initial_sql += ", FOREIGN KEY (location_id) REFERENCES location_dim_table(id)"
    initial_sql += ")"
    
    execute_sql(engine, initial_sql)
    logging.info("Created fact table.")

def execute_sql(engine, sql):
    '''Execute a SQL query'''
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

# Visualization Functions

def plot_pie_chart(data):
    '''Plot pie chart for top 3 cities'''
    city_names = data['CITY'].value_counts().index
    city_values = data['CITY'].value_counts().values
    
    plt.pie(city_values[:3], labels=city_names[:3], autopct='%1.2f%%')
    plt.show()

def plot_ratings_distribution(data):
    '''Plot distribution of ratings'''
    plt.hist(data['RATING'], bins=5)
    plt.xticks(rotation=90)
    plt.title("Ratings Distribution")
    plt.show()

def plot_top_restaurants(data):
    '''Plot top 10 restaurants by city'''
    restaurant_counts = data['CITY'].value_counts()
    top_10_restaurants = restaurant_counts.head(10)

    plt.figure(figsize=(16, 10))
    sns.barplot(x=top_10_restaurants.index, y=top_10_restaurants.values)
    plt.xticks(rotation=90)
    plt.xlabel('City')
    plt.ylabel('Number of Restaurants')
    plt.title('Top 10 Restaurants by City')
    plt.show()

def plot_boxplot(data):
    '''Plot boxplot of ratings by cuisine type'''
    plt.figure(figsize=(15, 8))
    sns.boxplot(x='CUSINETYPE', y='RATING', data=data)
    plt.xticks(rotation=90)
    plt.show()

def plot_scatter(data):
    '''Plot scatter plot of dining cost vs hotel rating'''
    plt.figure(figsize=(8, 6))
    plt.scatter(data['PRICE'], data['RATING'], alpha=0.5)
    plt.title('Scatter Plot: Cost of Dining vs. Hotel Rating')
    plt.xlabel('Cost of Dining')
    plt.ylabel('Hotel Rating')
    plt.grid(True)
    plt.show()
