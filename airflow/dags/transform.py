import os
import sys
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator

from airflow import DAG

sys.path.append('../')
sys.path.insert(1, os.path.dirname(os.path.abspath(__file__)) + '/../')

from scripts.notify import dag_notify_failure, dag_notify_success

with DAG(
    dag_id='nyc_taxi_transform',
    schedule=os.environ['DAG_TRANSFORM_SCHEDULE'],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags={'example'},
    default_args={
        'retries': 1,
        'retry_delay': timedelta(seconds=30),
        'on_failure_callback': dag_notify_failure,
    },
) as dag:
    dbt_root_folder = os.environ['DBT_ROOT_FOLDER']

    trigger_db_seed = BashOperator(
        task_id='trigger_db_seed',
        bash_command=f'cd {dbt_root_folder} && dbt seed',
        on_failure_callback=dag_notify_failure,
        on_success_callback=dag_notify_success,
    )

    run_db_models = BashOperator(
        task_id='trigger_db_models',
        bash_command=f'cd {dbt_root_folder} && dbt run --exclude tag:anomaly',
        on_failure_callback=dag_notify_failure,
        on_success_callback=dag_notify_success,
    )

    run_db_tests = BashOperator(
        task_id='trigger_db_tests',
        bash_command=f'cd {dbt_root_folder} && dbt test --exclude tag:anomaly',
        on_failure_callback=dag_notify_failure,
        on_success_callback=dag_notify_success,
    )

    generate_dbt_docs = BashOperator(
        task_id='trigger_db_docs',
        bash_command=f'cd {dbt_root_folder} && dbt docs generate --exclude tag:anomaly',
        on_failure_callback=dag_notify_failure,
        on_success_callback=dag_notify_success,
    )

    trigger_db_seed >> run_db_models >> run_db_tests >> generate_dbt_docs