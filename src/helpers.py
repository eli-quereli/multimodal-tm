from src.collect_data.config import HOST, PORT, PW, USER
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import sys
sys.path.append('../../')


def create_postgres_engine(database, host=HOST, port=PORT, user=USER, password=PW):
    db_url = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(db_url, client_encoding='utf8')
    return engine


def close_connection(conn):
    conn.close()
    print('Connection to DB closed')


def execute_sql_select(command, database, return_result_as_df=False):
    """Use for SELECT statements. Per default returns the result as a pandas DataFrame"""
    conn = None
    try:
        conn = psycopg2.connect(host=HOST, port=PORT, database=database, user=USER, password=PW, sslmode='require')
        cur = conn.cursor()
        cur.execute(command)
        colnames = [desc[0] for desc in cur.description]
        print("Column names: ", colnames)
        data = cur.fetchall()
        cur.close()
        conn.commit()
        if return_result_as_df is True:
            return pd.DataFrame.from_records(data, columns=colnames)
        else:
            return colnames, data
    finally:
        if conn is not None:
            close_connection(conn)
