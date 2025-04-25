import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/../'))
from etl.etl import load

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'my_dag',
    default_args=default_args,
    description='dec_beginner_project',
)

load = load
task1 = PythonOperator(
    task_id='movie_lens',
    python_callable=load,
    dag=dag,
)

task1
