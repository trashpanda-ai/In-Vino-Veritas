from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow import settings
from airflow.models import Connection
from sqlalchemy.exc import SQLAlchemyError
import logging
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy.sql import text
import psycopg2
import logging
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
    retries=0,
    retry_delay=timedelta(minutes=1)
)
def _create_or_update_conn(conn_id, conn_type, host, login, pwd, port, desc):
    conn = Connection(conn_id=conn_id,
                      conn_type=conn_type,
                      host=host,
                      login=login,
                      password=pwd,
                      port=port,
                      description=desc)

    try:
        session = settings.Session()
        existing_conn = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

        if existing_conn:
            existing_conn.conn_type = conn_type
            existing_conn.host = host
            existing_conn.login = login
            existing_conn.password = pwd
            existing_conn.port = port
            existing_conn.description = desc
            logging.info(f"Connection {conn.conn_id} updated")
        else:
            session.add(conn)
            logging.info(f"Connection {conn.conn_id} created")

        session.commit()
    except SQLAlchemyError as e:
        logging.error(f"Error occurred while handling connection {conn.conn_id}: {e}")
    finally:
        session.close()

postgres_connect = PythonOperator(
    task_id='postgres_connect',
    dag=dag,
    python_callable=_create_or_update_conn,
    op_kwargs={
        'conn_id': 'postgres_default',
        'conn_type': 'postgres',
        'host': 'postgres',
        'login': 'airflow',
        'pwd': 'airflow',
        'port': '5432',
        'desc': 'Postgres connection default'
    },
    trigger_rule='none_failed'
)

def _check_and_update_postgres():
    # Read the Parquet file into a pandas dataframe
    parquet_file = '/opt/airflow/wine_data.parquet'
    df = pd.read_parquet(parquet_file)

    # Connect to the PostgreSQL database
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/postgres')

    # Create an inspector
    inspector = inspect(engine)
    # Convert DataFrame column names to lowercase
    df.columns = map(str.lower, df.columns)
    # Check if the table exists
    if 'wines' not in inspector.get_table_names():
        # Create the table
        df.head(0).to_sql('wines', engine, if_exists='replace', index=False)
        with engine.connect() as con:
            con.execute('ALTER TABLE wines ADD CONSTRAINT wines_id_key UNIQUE (id);')

    # Iterate over each row in the dataframe
    for row in df.itertuples(index=False):
        query = text("""
        INSERT INTO wines (id, winery, name, vintage, country, region, wine_style, wine_type, wine_category, grape_type,
            grape_id, rating, review_count, price, acidity, fizziness, intensity, sweetness, tannin, scrape_date)
        VALUES (:id, :winery, :name, :vintage, :country, :region, :wine_style, :wine_type, :wine_category, :grape_type,
            :grape_id, :rating, :review_count, :price, :acidity, :fizziness, :intensity, :sweetness, :tannin, :scrape_date)
        ON CONFLICT (id) DO NOTHING
        """)
        try:
            # Execute the query
            engine.execute(query, row._asdict())
        except Exception as e:
            # Log the insertion error
            logging.error(f"Error occurred while inserting row with ID {row.id}: {e}")

    # Close the database connection
    engine.dispose()
check_postgres = PythonOperator(
    task_id='check_postgres',
    dag=dag,
    python_callable=_check_and_update_postgres,
    trigger_rule='none_failed'
)


write_postgres = DummyOperator(
    task_id='write_postgres',
    dag=dag,
    trigger_rule='none_failed'
)

get_grape_and_year = DummyOperator(
    task_id='get_grape_and_year',
    dag=dag,
    trigger_rule='none_failed'
)
get_country_and_year = DummyOperator(
    task_id='get_country_and_year',
    dag=dag,
    trigger_rule='none_failed'
)
get_region_and_year = DummyOperator(
    task_id='get_region_and_year',
    dag=dag,
    trigger_rule='none_failed'
)
enrich_trends = DummyOperator(
    task_id='enrich_trends',
    dag=dag,
    trigger_rule='none_failed'
)
enrich_harvest = DummyOperator(
    task_id='enrich_harvest',
    dag=dag,
    trigger_rule='none_failed'
)
enrich_weather = DummyOperator(
    task_id='enrich_weather',
    dag=dag,
    trigger_rule='none_failed'
)

end = DummyOperator(
    task_id='finale',
    dag=dag,
    trigger_rule='none_failed'
)

papermill_operator>>postgres_connect>>check_postgres>>write_postgres>>end
check_postgres>>[get_grape_and_year,get_country_and_year,get_region_and_year]
get_grape_and_year>>enrich_trends>>end
get_country_and_year>>enrich_harvest>>end
get_region_and_year>>enrich_weather>>end