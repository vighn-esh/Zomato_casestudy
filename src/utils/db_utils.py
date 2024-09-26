import logging
from sqlalchemy import create_engine

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

def create_sqlite_engine(db_path):
   
    engine = create_engine(f'sqlite:///{db_path}')
    return engine
