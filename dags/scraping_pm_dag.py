import logging
import statistics
import time

import numpy as np
import pandas as pd
import faostat
import psycopg2
from airflow import DAG, settings
from airflow.models import Connection
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.papermill.operators.papermill import PapermillOperator
from datetime import datetime, timedelta
from pytrends.request import TrendReq
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text
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
    max_active_tasks=3
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

    # Initialize count
    count = 0
    conflict_count = 0

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
            result = engine.execute(query, row._asdict())
            if result.rowcount > 0:
                count += 1  # Increment count for successful insertions
            else:
                conflict_count += 1  # Increment count for conflicts
        except Exception as e:
            # Log the insertion error
            logging.error(f"Error occurred while inserting row with ID {row.id}: {e}")
    logging.info(f"inserted {count} rows")
    logging.info(f"conflicts at {conflict_count} rows (duplicate IDs)")

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

def _get_grape_and_year():
    # Connect to the PostgreSQL database
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/postgres')
    # Create an inspector
    inspector = inspect(engine)
    # Check if the table exists
    if 'wines' not in inspector.get_table_names():
        logging.error('Table wines does not exist')
        return
    # Query the database
    query = text("""
    SELECT DISTINCT grape_type, vintage
    FROM wines
    WHERE grape_type IS NOT NULL AND vintage IS NOT NULL AND vintage > 2004
    """)
    try:
        # Execute the query
        df = pd.read_sql(query, engine)
        # Write the dataframe to a Parquet file
        df.to_parquet('/opt/airflow/grape_and_year.parquet')
    except Exception as e:
        # Log the insertion error
        logging.error(f"Error occurred while querying the database: {e}")
    # Close the database connection
    engine.dispose()

get_grape_and_year = PythonOperator(
    task_id='get_grape_and_year',
    dag=dag,
    python_callable=_get_grape_and_year,
    trigger_rule='none_failed'
)
def _get_country_and_year():
    # Connect to the PostgreSQL database
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/postgres')
    # Create an inspector
    inspector = inspect(engine)
    # Check if the table exists
    if 'wines' not in inspector.get_table_names():
        logging.error('Table wines does not exist')
        return
    # Query the database
    query = text("""
    SELECT DISTINCT country, vintage
    FROM wines
    WHERE country IS NOT NULL AND vintage IS NOT NULL
    """)
    try:
        # Execute the query
        df = pd.read_sql(query, engine)
        # Write the dataframe to a Parquet file
        df.to_parquet('/opt/airflow/country_and_year.parquet')
    except Exception as e:
        # Log the insertion error
        logging.error(f"Error occurred while querying the database: {e}")
    # Close the database connection
    engine.dispose()

get_country_and_year = PythonOperator(
    task_id='get_country_and_year',
    dag=dag,
    python_callable=_get_country_and_year,
    trigger_rule='none_failed'
)
def _get_region_and_year():
    # Connect to the PostgreSQL database
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/postgres')
    # Create an inspector
    inspector = inspect(engine)
    # Check if the table exists
    if 'wines' not in inspector.get_table_names():
        logging.error('Table wines does not exist')
        return
    # Query the database
    query = text("""
    SELECT DISTINCT region, vintage
    FROM wines
    WHERE region IS NOT NULL AND vintage IS NOT NULL
    """)
    try:
        # Execute the query
        df = pd.read_sql(query, engine)
        # Write the dataframe to a Parquet file
        df.to_parquet('/opt/airflow/region_and_year.parquet')
    except Exception as e:
        # Log the insertion error
        logging.error(f"Error occurred while querying the database: {e}")
    # Close the database connection
    engine.dispose()
get_region_and_year = PythonOperator(
    task_id='get_region_and_year',
    dag=dag,
    python_callable=_get_region_and_year,
    trigger_rule='none_failed'
)

def _enrich_trends():
    headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Sec-Fetch-Site': 'same-origin',
    'Cookie': 'SIDCC=ACA-OxOnJOC7DS5KiRxIT_fR2QkEWCcC0qnRq2CsEQsF4Sys_RO_vNC35Crs2MB5YdXFgAsV; __Secure-1PSIDCC=ACA-OxP0ptWglmVe-8Y9UjZEA2PjryZYF9U8Da7mYl3v3-UARlu_SF6igP2ExrnNjJIq3n_0; __Secure-3PSIDCC=ACA-OxNgjKevY3Zow3HLaWD6avR6m0Vw9brDKI_2YZ1HqqVhrksBkErpBGhNVcwjaR90uCTacQ; __Secure-1PSIDTS=sidts-CjIBNiGH7jHN0DIAoxvWfLffElvkiz4xuHvf4HlXznYgJbCKXTpI1Vd7cNt473ct6lIW0RAA; __Secure-3PSIDTS=sidts-CjIBNiGH7jHN0DIAoxvWfLffElvkiz4xuHvf4HlXznYgJbCKXTpI1Vd7cNt473ct6lIW0RAA; OTZ=7313007_52_52_123900_48_436380; APISID=rGw9b4LlhUzdu9JK/A5FCfs42fPt9ML6c_; HSID=AyERsaCQdeoOCSTaz; NID=511=X8aF5DKahzhIfJnmosQ1ObPEG8zuH2izcRIKsuqYeEylpV2ZHP3pSelOtraRPMq4pgy2wjm7kxo6c7lmnqSxHev6GySI8oINAQB_awzDtax9YDqHEShrtK_WLysYWiVDRMvy_LrNkEmF8G2N13timw27rWOVsasND3EDY4hkYo3oliLUp5CZL3xjGC_M2hz2JvuR5I-ICNWpO32rj4F_ocrG43zfkegl5cJHGT2MCiSUriZCbxP1kaMBPQTG-_Y4JTRakj4E; SAPISID=xtm7yO5yqBMIfMO_/AraNHIFdvZpGK-nKQ; SID=dQitCu_1-9kn51deqjTS85Cx8dHgq3zcTMHZonuWCvwC6Ty3PQihDzktg1v-0nRdHwAujA.; SSID=Agfst-sFEI8JQacAA; __Secure-1PAPISID=xtm7yO5yqBMIfMO_/AraNHIFdvZpGK-nKQ; __Secure-1PSID=dQitCu_1-9kn51deqjTS85Cx8dHgq3zcTMHZonuWCvwC6Ty3nqCQMxgz8872_h16xzPhcA.; __Secure-3PAPISID=xtm7yO5yqBMIfMO_/AraNHIFdvZpGK-nKQ; __Secure-3PSID=dQitCu_1-9kn51deqjTS85Cx8dHgq3zcTMHZonuWCvwC6Ty3BncuJgujlOIb3nvCSH52zQ.',
    'Sec-Fetch-Dest': 'document',
    'Accept-Language': 'en-gb',
    'Sec-Fetch-Mode': 'navigate',
    'Host': 'trends.google.com',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15',
    'Referer': 'https://trends.google.com/',
    # 'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
}

    # Read the Parquet file into a pandas dataframe
    parquet_file = '/opt/airflow/grape_and_year.parquet'
    df = pd.read_parquet(parquet_file)
    logging.info(f"read parquet file grape and year with {len(df)} rows")

    # Connect to the PostgreSQL database
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/postgres')
    # Create an inspector
    inspector = inspect(engine)
    # Check if the table exists
    if 'trends' not in inspector.get_table_names():
        # Create the table
        df.head(0).to_sql('trends', engine, if_exists='replace', index=False)
        with engine.connect() as con:
            con.execute('ALTER TABLE trends ADD CONSTRAINT trends_id_key PRIMARY KEY (grape_type, vintage);')
            con.execute('ALTER TABLE trends ADD COLUMN median FLOAT;')
            con.execute('ALTER TABLE trends ADD COLUMN mean FLOAT;')

    update_number=0
    # Iterate over each row in the dataframe
    for row in df.itertuples(index=False): 
        keyword = row.grape_type
        year = row.vintage   
        query = text("""
        INSERT INTO trends (grape_type, vintage)
        VALUES (:grape_type, :vintage)
        ON CONFLICT (grape_type, vintage) DO NOTHING
        """)
        try:
            # Execute the query
            engine.execute(query, row._asdict())
        except Exception as e:
            # Log the insertion error
            logging.error(f"Error occurred while inserting row with grape_type {row.grape_type} and vintage {row.vintage}: {e}")

            
        # Check if the row for median or mean column is empty
        with engine.connect() as con:
            result = con.execute(f"SELECT median, mean FROM trends WHERE grape_type = '{keyword}' AND vintage = {year};")
            row = result.fetchone()
            if row is None or row['median'] is None or row['mean'] is None:
                timeframe = f'{year}-01-01 {year}-12-31' # time frame for data
                time.sleep(1)
                try:
                    pytrends = TrendReq(hl='en-US', tz=360, retries=3, backoff_factor=0.1, requests_args={"headers": headers})
                    pytrends.build_payload([keyword], cat=0, timeframe=timeframe) 
                    data = pytrends.interest_over_time()
                    median = statistics.median(data.loc[:, keyword].tolist())
                    mean = statistics.mean(data.loc[:, keyword].tolist())
                    # Update the trends table with median and mean values
                    con.execute(f"UPDATE trends SET median = {median}, mean = {mean} WHERE grape_type = '{keyword}' AND vintage = {year};")
                    logging.info(f"updated trends for {keyword} in {year} with values {median}, {mean}")
                    update_number+=1
                except Exception as e:
                    logging.error(f"Error occurred while querying Google Trends for {keyword} in {year}: {e}")

    logging.info(f"updated {update_number} rows")
    # Close the database connection
    engine.dispose()

enrich_trends = PythonOperator(
    task_id='enrich_trends',
    dag=dag,
    python_callable=_enrich_trends,
    trigger_rule='none_failed'
)
def _enrich_harvest():
    # Read the Parquet file into a pandas dataframe
    parquet_file = '/opt/airflow/country_and_year.parquet'
    df = pd.read_parquet(parquet_file)
    logging.info(f"read parquet file country and year with {len(df)} rows")

    # Connect to the PostgreSQL database
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/postgres')
    # Create an inspector
    inspector = inspect(engine)
    # Check if the table exists
    if 'harvest' not in inspector.get_table_names():
        # Create the table
        df.head(0).to_sql('harvest', engine, if_exists='replace', index=False)
        with engine.connect() as con:
            con.execute('ALTER TABLE harvest ADD CONSTRAINT harvest_id_key PRIMARY KEY (country, vintage);')
            con.execute('ALTER TABLE harvest ADD COLUMN harvest_area FLOAT;')
            con.execute('ALTER TABLE harvest ADD COLUMN harvest_grape FLOAT;')
            con.execute('ALTER TABLE harvest ADD COLUMN harvest_wine FLOAT;')
    
    # Use faostat to dynamically get the harvest amount for each country and year pair
    update_number = 0
    # Iterate over each row in the dataframe
    for rows in df.itertuples(index=False):
        country = rows.country
        year = rows.vintage
        try:
            name_dict = faostat.get_par('QCL', 'area')
            if country in name_dict:
                df1 = faostat.get_data_df('QCL', pars={'item':'560', 'area':name_dict[country], 'year':year}, show_flags=True, null_values=True)
                df2 = faostat.get_data_df('QCL', pars={'item':'564', 'area':name_dict[country], 'year':year}, show_flags=True, null_values=True)
                if not df1.empty and not df2.empty:
                    area_harvested = df1.loc[df1['Element'] == "Area harvested"]['Value']
                    grape_harvested = df1.loc[df1['Element'] == "Production"]['Value']
                    wine_made = df2.loc[df2['Element'] == "Production"]['Value']
                    if area_harvested is not None and grape_harvested is not None and wine_made is not None:
                        area_harvested = float(area_harvested.iloc[0])
                        grape_harvested = float(grape_harvested.iloc[0])
                        wine_made = float(wine_made.iloc[0])           
                        try:
                            with engine.connect() as con:
                                con.execute(f"UPDATE harvest SET harvest_area = {area_harvested}, harvest_grape = {grape_harvested}, harvest_wine = {wine_made} WHERE country = '{country}' AND vintage = {year};")
                            update_number += 1  # Increase the update_number only if there were no errors
                            logging.info(f"updated harvest for {country} in {year} with values {area_harvested}, {grape_harvested}, {wine_made}")
                        except Exception as e:
                            logging.error(f"Error occurred while updating the database for {country} in {year}: {e}")
                            continue
                        
                    else:
                        logging.error(f"None value found in DataFrame for {country} in {year}")
                else:
                    logging.error(f"Empty DataFrame returned for {country} in {year}")
            else:
                logging.error(f"'{country}' not found in name_dict")            

        except Exception as e:
            logging.error(f"Error occurred while querying faostat for {country} in {year}: {e}")
            continue
    logging.info(f"updated {update_number} rows")
    



enrich_harvest = PythonOperator(
    task_id='enrich_harvest',
    dag=dag,
    python_callable=_enrich_harvest,
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

# papermill_operator>>postgres_connect>>check_postgres>>write_postgres>>end
# check_postgres>>[get_grape_and_year,get_country_and_year,get_region_and_year]
get_grape_and_year>>enrich_trends>>end
get_country_and_year>>enrich_harvest>>end
get_region_and_year>>enrich_weather>>end