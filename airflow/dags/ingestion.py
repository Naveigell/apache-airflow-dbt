import os
import sys
from datetime import timedelta, datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from dotenv import load_dotenv

sys.path.append('../')
sys.path.insert(1, os.path.dirname(os.path.abspath(__file__)) + '/../')

from scripts.database   import save_parquet_into_duckdb
from scripts.notify     import dag_notify_failure, dag_notify_success
from scripts.validate   import validate_file

load_dotenv()

def validation():
    """
    Validate the correctness of the provided files.

    Returns:
        bool: True if the file is valid, False otherwise.

    Args:
        files (list of str): List of file paths to validate.
    """
    return validate_file([
        os.environ['RAW_FOLDER'] + '/yellow_tripdata_2023-01.parquet',
        os.environ['RAW_FOLDER'] + '/yellow_tripdata_2023-02.parquet',
        os.environ['RAW_FOLDER'] + '/yellow_tripdata_2023-03.parquet',
    ])

def load():
    """
    Load parquet files into a DuckDB database.

    Parameters
    ----------
    files : list of str
        List of paths to parquet files to load.
    db_name : str
        Name of the DuckDB database to load the data into.
    table_name : str
        Name of the table to load the data into.

    Returns
    -------
    None

    """
    save_parquet_into_duckdb([
        os.environ['RAW_FOLDER'] + '/yellow_tripdata_2023-01.parquet',
        os.environ['RAW_FOLDER'] + '/yellow_tripdata_2023-02.parquet',
        os.environ['RAW_FOLDER'] + '/yellow_tripdata_2023-03.parquet',
    ], os.environ['DATABASE_NAME'], os.environ['DATABASE_TABLE'])

with DAG(
    dag_id='nyc_taxi_ingestion',
    schedule=os.environ['DAG_INGESTION_SCHEDULE'],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags={'example'},
    default_args={
        'retries': 1,
        'retry_delay': timedelta(seconds=30),
        'on_failure_callback': dag_notify_failure,
    },
) as dag:
    validation_file = PythonOperator(
        task_id='validate_file',
        python_callable=validation,
        on_failure_callback=dag_notify_failure,
        on_success_callback=dag_notify_success,
    )

    load_to_database = PythonOperator(
        task_id='load_to_database',
        python_callable=load,
        on_failure_callback=dag_notify_failure,
        on_success_callback=dag_notify_success,
    )

    validation_file >> load_to_database