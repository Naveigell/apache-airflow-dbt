import os
import sys
from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator

from airflow import DAG

sys.path.append('../')
sys.path.insert(1, os.path.dirname(os.path.abspath(__file__)) + '/../')

from scripts.notify import notify_failure, notify_success

import scripts.monitoring as monitoring

with DAG(
    dag_id='nyc_taxi_monitor',
    schedule=os.environ['DAG_MONITOR_SCHEDULE'],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags={'example'},
    default_args={
        'retries': 1,
        'retry_delay': timedelta(seconds=30),
        'on_failure_callback': notify_failure,
    },
) as dag:
    check_data_freshness = PythonOperator(
        task_id='check_data_freshness',
        python_callable=monitoring.check_data_freshness,
        on_failure_callback=notify_failure,
        on_success_callback=notify_success,
    )

    validate_row_counts = PythonOperator(
        task_id='validate_row_counts',
        python_callable=monitoring.validate_row_counts,
        on_failure_callback=notify_failure,
        on_success_callback=notify_success,
    )

    compare_with_historical = PythonOperator(
        task_id='compare_with_historical',
        python_callable=monitoring.compare_with_historical,
        on_failure_callback=notify_failure,
        on_success_callback=notify_success,
    )

    generate_daily_summary = PythonOperator(
        task_id='generate_daily_summary',
        python_callable=monitoring.generate_daily_summary,
        on_failure_callback=notify_failure,
        on_success_callback=notify_success,
    )

    check_data_freshness >> validate_row_counts >> compare_with_historical >> generate_daily_summary