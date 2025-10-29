import os
import sys

# append path
sys.path.append('../')
sys.path.insert(1, os.path.dirname(os.path.abspath(__file__)) + '/../')

from datetime import timedelta, datetime

from airflow import DAG

from scripts.database import save_parquet_into_duckdb

from scripts.validate import validate_file


def notify_failure():
    pass

with DAG(
    dag_id='etl_to_datamart_552518',
    schedule=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags={'example'},
    default_args={
        'retries': 1,
        'retry_delay': timedelta(seconds=30),
        'on_failure_callback': notify_failure,
    },
) as dag:
    pass

validate_file([
    './airflow/data/raw/yellow_tripdata_2023-01.parquet',
    './airflow/data/raw/yellow_tripdata_2023-02.parquet',
    './airflow/data/raw/yellow_tripdata_2023-03.parquet',
])

save_parquet_into_duckdb([
    './airflow/data/raw/yellow_tripdata_2023-01.parquet',
    './airflow/data/raw/yellow_tripdata_2023-02.parquet',
    './airflow/data/raw/yellow_tripdata_2023-03.parquet',
], './dbt/nyc_taxi.duckdb', 'yellow_tripdata')
