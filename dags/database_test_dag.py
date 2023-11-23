import airflow
import datetime
import urllib.request as request
import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

first_dag = DAG(
    dag_id='db_test',
    default_args=default_args_dict,
    catchup=False,
)

create_pet_table = PostgresOperator(
    dag=first_dag,
    task_id="create_pet_table",
    postgres_conn_id="postgres_default",
    sql="sql/pet_schema.sql",
)

populate_pet_table = PostgresOperator(
    dag=first_dag,
    task_id="populate_pet_table",
    postgres_conn_id="postgres_default",
    sql="sql/pet_insert.sql",
)

get_all_pets = PostgresOperator(
    dag=first_dag,
    task_id="get_all_pets",
    postgres_conn_id="postgres_default",
    sql="SELECT * FROM pet;",
)

create_pet_table>>populate_pet_table>>get_all_pets