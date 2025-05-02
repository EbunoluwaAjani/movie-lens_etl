import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/../'))
from etl.etl import download_data, load


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    dag_id ='my_dag',
    description ='dec_beginner_project',
    default_args = default_args,
    schedule_interval = '@daily'
)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=download_data,
    dag = dag
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)

extract_task >> load_task
