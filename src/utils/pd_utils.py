import pandas as pd

def drop_column(df, column_name):
    
    if column_name in df.columns:
        return df.drop(columns=[column_name])
    else:
        print(f"Error: Column '{column_name}' does not exist in the DataFrame.")
        return df  


def load_df(engine, table_name):
    df = pd.read_sql(f"SELECT * FROM {table_name}", con=engine)
    return df