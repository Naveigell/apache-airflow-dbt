from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator

from airflow import DAG
from scripts.notify import notify_failure

with DAG(
    dag_id='nyc_taxi_transform',
    schedule='0 6 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags={'example'},
    default_args={
        'retries': 1,
        'retry_delay': timedelta(seconds=30),
        'on_failure_callback': notify_failure,
    },
) as dag:
    trigger_db_seed = BashOperator(
        task_id='trigger_db_seed',
        bash_command='cd ../dbt && dbt seed',
    )

    run_db_models = BashOperator(
        task_id='trigger_db_models',
        bash_command='cd ../dbt && dbt run',
    )

    run_db_tests = BashOperator(
        task_id='trigger_db_tests',
        bash_command='cd ../dbt && dbt test',
    )

    generate_dbt_docs = BashOperator(
        task_id='trigger_db_docs',
        bash_command='cd ../dbt && dbt docs generate',
    )

    trigger_db_seed >> run_db_models >> run_db_tests >> generate_dbt_docs