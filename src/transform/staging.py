import pandas as pd
from sqlalchemy import create_engine
from ..utils.config import load_config
from ..utils.db_utils import execute_sql




def rename_df_cols(df):
    
    col_no_space = dict((i, i.replace(' ', '')) for i in list(df.columns))
    df.rename(columns=col_no_space, inplace=True)
    return df


def stage_data(df):
    
    
    config=load_config()
    map_data = config["params"]["dtype_mapping"]
    table_name="staging_db"
    staging_db_path=config["file_path"]["staging_db"]
    df = rename_df_cols(df)
    sql=f"CREATE TABLE IF NOT EXISTS {table_name} (key_pk INTEGER PRIMARY KEY AUTOINCREMENT,"

    engine=create_engine(staging_db_path)

    col_list_dtype = [(i, str(df[i].dtype)) for i in list(df.columns)]
    for col_name, dtype in col_list_dtype:
        sqlite_dtype = map_data.get(dtype, 'TEXT') 
        sql += f" {col_name} {sqlite_dtype}"

    sql += ')'
    print(sql)
    execute_sql(engine,sql)
    df.to_sql(table_name, engine, index=False, if_exists="replace")