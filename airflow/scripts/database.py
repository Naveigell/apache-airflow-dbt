import os
import sqlite3

import duckdb
import pandas as pd


def save_dataframe_into_sqlite(df, db_name, table_name):
    """
    Save data into a SQLite database.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing the data to save.
    table_name : str
        Name of the table to save the data into.

    Returns
    -------
    None
    """
    connection = sqlite3.connect(db_name)

    df.to_sql(table_name, connection, if_exists='append', index=False)

    connection.close()

    print(f"Data saved to {table_name} in {db_name}")

def save_parquet_into_duckdb(files, db_name, table_name):
    """
    Save parquet files into a DuckDB database.

    Parameters
    ----------
    files : list of str
        List of paths to parquet files to save.
    db_name : str
        Name of the DuckDB database to save the data into.
    table_name : str
        Name of the table to save the data into.

    Returns
    -------
    None

    """
    mode = 'CREATE'

    # Pastikan direktori database ada
    os.makedirs(os.path.dirname(db_name), exist_ok=True)

    print(f"Connection to: {db_name}")

    for file in files:

        conn = duckdb.connect(database=db_name)

        try:
            print(f"Fetch {file} to table {table_name}...")

            if mode == 'CREATE':
                load_query = f"""
                        CREATE TABLE {table_name} AS 
                        SELECT * FROM read_parquet('{file}');
                    """

                mode = 'APPEND'
            else:
                load_query = f"""
                        INSERT INTO {table_name} 
                        SELECT * FROM read_parquet('{file}');
                    """

            conn.execute(load_query)

            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

            print(f"Loaded data to duckdb, {row_count} loaded to table : '{table_name}'.")

        except Exception as e:
            print(f"Duckdb failed to loaded : {e}")

            raise

        finally:
            conn.close()

            print("Connection closed")



def save_parquet_into_staging(files, db_name, table_name):
    """
    Save parquet files into a SQLite database.

    Parameters
    ----------
    files : list of str
        List of paths to parquet files to save.
    db_name : str
        Name of the SQLite database to save the data into.
    table_name : str
        Name of the table to save the data into.

    Returns
    -------
    None

    """
    for file in files:
        df = pd.read_parquet(file, engine='pyarrow')

        save_dataframe_into_sqlite(df, db_name, table_name)