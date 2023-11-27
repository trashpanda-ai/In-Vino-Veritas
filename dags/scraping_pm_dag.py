from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
default_args = {
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'concurrency': 1
}

dag = DAG(
    'scraping_pm_dag',
    default_args=default_args,
    description='DAG for scraping data using Papermill',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1
)

notebook_path = '/opt/airflow/dags/scraping.ipynb'
output_path = '/opt/airflow/dags/output.ipynb'

papermill_operator = PapermillOperator(
    task_id='run_papermill',
    dag=dag,
    input_nb=notebook_path,
    output_nb=output_path,
    parameters={'param1': 'test1', 'param2': 'test2'},
    retries=2,
    retry_delay=timedelta(minutes=1)
)


end = DummyOperator(
    task_id='finale',
    dag=dag,
    trigger_rule='none_failed'
)

papermill_operator>>end
