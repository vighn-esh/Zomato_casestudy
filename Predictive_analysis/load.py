

import pandas as pd
import sqlalchemy
from sklearn.model_selection import train_test_split  # Add this line at the top of load.py


def load_data(engine, staging_table_names):
    """Load data from multiple staging tables and concatenate them into a single DataFrame."""
    print("Connecting to database with engine:", engine)
    dfs = []
    
    # Connect to the database and retrieve the staging data for each table
    for table_name in staging_table_names:
        with engine.connect() as conn:
            query = f"SELECT * FROM {table_name}"
            print(query)
            df = pd.read_sql(query, conn)
            dfs.append(df)

    # Concatenate the DataFrames from all staging tables
    combined_df = pd.concat(dfs, axis=1)
    return combined_df

from sklearn.impute import SimpleImputer

def preprocess_data(df, ML_config):
    """Preprocess the data by cleaning, encoding, imputing, and splitting it into train and test sets."""
    # Remove duplicate columns
    df = df.loc[:, ~df.columns.duplicated()].copy()

    # Drop unnecessary columns
    uselessColumns = ['fact_id', 'restaurant_id', 'location_id', 'NAME']
    df = df.drop(uselessColumns, axis=1)

    # Add 'cusine_count' column based on 'CUSINE_CATEGORY'
    df['cusine_count'] = df['CUSINE_CATEGORY'].str.count(',') + 1

    # Drop rows with missing essential data
    df = df.dropna(subset=['Latitude', 'Longitude', 'RATING', 'cusine_count', 'RATING_TYPE'])

    # Select columns for prediction
    selected_columns = ML_config['params']['selected_columns_for_prediction']
    filtered_df = df[selected_columns]

    # Encode ordinal categorical column (RATING_TYPE)
    category_mapping = ML_config['params']['rating_type_category_mapping']
    filtered_df.loc[:, 'RATING_TYPE'] = df['RATING_TYPE'].map(category_mapping)

    # Apply one-hot encoding to nominal categorical columns
    filtered_df = pd.get_dummies(filtered_df, columns=['CUSINETYPE', 'CITY'])

    # Impute missing values (replace NaN values with the mean or another strategy)
    imputer = SimpleImputer(strategy='mean')  # You can also use 'median' or other strategies
    X = pd.DataFrame(imputer.fit_transform(filtered_df), columns=filtered_df.columns)

    # Split data into features and target variable
    y = df['RATING']

    # Split data into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=ML_config['params']['test_size'], random_state=ML_config['params']['random_state'])

    return X_train, X_test, y_train, y_test

