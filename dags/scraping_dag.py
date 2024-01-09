import logging

import requests
import airflow
import datetime
import urllib.request as request
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow import settings
from airflow.models import Connection
from sqlalchemy.exc import SQLAlchemyError



default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

scraping_dag = DAG(
    dag_id='scraping_vivino',
    default_args=default_args_dict,
    catchup=False,
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

create_connection=PythonOperator(
    dag=scraping_dag,
    task_id="create_a_postgres_connection",
    python_callable=_create_or_update_conn,
    op_kwargs={
        "conn_id":"postgres_default",
        "conn_type":"postgres",
        "host":"postgres",
        "login":"airflow",
        "pwd":"airflow",
        "port":"5432",
        "desc":"postgres def connection"
    },
    trigger_rule='all_done',
    depends_on_past=False,

)
def generate_numbers(n, max_val):
    min = np.floor(max_val*0.1).astype(int)
    small_numbers = np.random.exponential(scale=1.0, size=np.floor(n*0.5).astype(int))
    small_numbers = np.floor(small_numbers * 0.5*min).astype(int)
    large_numbers = np.random.exponential(scale=1.0, size=np.floor(n*0.5).astype(int))
    large_numbers = np.floor(large_numbers * 0.5*max_val).astype(int)
    numbers = np.concatenate((small_numbers, large_numbers))
    return list(numbers)



def vivino_scraper():
    wine_cols = ["ID", "Winery","Name", "Vintage","Country", "Region", "Wine_Style", "Wine_Type", "Wine_Category", "Grape_Type",
            "Grape_ID", 'Rating', 'Review_Count','Price', 'Acidity', "Fizziness", "Intensity", "Sweetness", "Tannin", "Scrape_Date"]
    
    random_grapes = generate_numbers(20, 1500)

    temp_df = []

    number_of_pages = 15

    for y in random_grapes: #y range is the number of grape types (up to 200)  
        for z in [1,2,3,4,7,24]: # z is the wine type (1: red, 2: white, 3: sparkling, 4: rosé 7: dessert wine 24: fortified wine) 
            for x in range(1, number_of_pages): # x range is the number of pages (up to ?? - depends on grape)  
            # instead of parsing we found a somewhat unofficial API that we can use to get the data
            # But normally one would only get 2000 results (https://stackoverflow.com/questions/71264253/web-scraping-vivino-using-python)
            # thats why we analyzed all the data one can use as payload to design restarts for the random walk of API scraping
                r = requests.get(
            "https://www.vivino.com/api/explore/explore",
            params = {
                #"country_code": "en",
                'grape_ids[]':y,
                #"country_codes[]":["pt", "es", "fr", "de"],
                "currency_code":"EUR",
                #"grape_filter":"varietal",
                "min_rating":"1",
                #"order_by":"price", #  "ratings_average"
                #"order":"asc",
                "page": x,
                "price_range_max":"1500",
                "price_range_min":"0",
                "wine_type_ids[]":z,
                "language":"en",
                "per_page":50
            },
                headers= {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0",  
                'Accept': 'application/json',
                'Accept-Language': 'en-US,en;q=0.5',
            }
            )
                try:
                    results = [
                    (
                    f'{t["vintage"]["wine"]["name"]} {t["vintage"]["year"]}',#ID
                    t["vintage"]["wine"]["winery"]["name"], #winery
                    t["vintage"]["wine"]["name"], #Name
                    t["vintage"]["year"], #Vintage
                    t["vintage"]["wine"]["region"]["country"]["name"], #Country
                    t["vintage"]["wine"]["region"]["name"], #region
                    t["vintage"]["wine"]["style"]["seo_name"], # wine style
                    t["vintage"]["wine"]["style"]["varietal_name"], # wine type
                    t["vintage"]["wine"]["type_id"], #wine type by id
                    r.json()["selected_filters"][0]["items"][0]["name"], # grape type
                    r.json()["selected_filters"][0]["items"][0]["id"], # grape id
                    t["vintage"]["statistics"]["ratings_average"], #rating
                    t["vintage"]["statistics"]["ratings_count"],# number of ratings
                    t["price"]["amount"],#price
                    t["vintage"]["wine"]["taste"]["structure"]["acidity"], # wine dimensions 1
                    t["vintage"]["wine"]["taste"]["structure"]["fizziness"],# wine dimensions 2
                    t["vintage"]["wine"]["taste"]["structure"]["intensity"], # wine dimensions 3
                    t["vintage"]["wine"]["taste"]["structure"]["sweetness"],# wine dimensions 4
                    t["vintage"]["wine"]["taste"]["structure"]["tannin"],    # wine dimensions 5
                    # add scrape date as date
                    pd.to_datetime('today').strftime("%d-%m-%Y")
                    )
                    for t in r.json()["explore_vintage"]["matches"]
                    ]
                    temp_df.append(results)
                except:
                        pass

    if all(isinstance(i, list) for i in temp_df):
        temp_df = [item for sublist in temp_df for item in sublist]  # Flatten the list of lists
        wine_df = pd.DataFrame(temp_df, columns=wine_cols)
    return wine_df

def _scrape_vivino():
    

    wine_df =vivino_scraper() 
    wine_df.to_csv("vivino_scrape.csv", index=False)
    



scrape_vivino=PythonOperator(
    dag=scraping_dag,
    task_id="scrape_vivino",
    python_callable=_scrape_vivino,
    trigger_rule='all_success',
    depends_on_past=False,
)

def _clean_data():
    wine_df = pd.read_csv("vivino_scrape.csv")
    # map wine type id to wine type
    wine_df["Wine_Category"] = wine_df["Wine_Category"].replace({1: "Red", 2: "White", 3: "Sparkling", 4: "Rosé", 7: "Dessert Wine", 24: "Fortified Wine"})

    # Remove duplicates
    wine_df = wine_df.drop_duplicates(subset=['ID'])

    #cleaning region data, TO BE EXPANDED
    wine_df['Region'] = wine_df['Region'].str.replace('Grand Cru', '')

    #clean NaN values
    wine_df['Rating'] = wine_df['Rating'].replace(0, np.nan)
    wine_df['Vintage'] = wine_df['Vintage'].replace("N.V.", np.nan)
    wine_df.to_csv("vivino_scrape_clean_1.csv", index=False)
    

clean_data=PythonOperator(
    dag=scraping_dag,
    task_id="clean_data",
    python_callable=_clean_data,
    trigger_rule='all_success',
    depends_on_past=False,
)

def _drop_rows():
    wine_df = pd.read_csv("vivino_scrape_clean_1.csv")
    # drop rows of wine_df['Vintage'] with value '' (empty string)
    wine_df = wine_df[wine_df['Vintage'] != '']
    wine_df['Vintage'] = wine_df['Vintage'].fillna(0)
    wine_df['Vintage'].unique()
    wine_df.to_csv("vivino_scrape_clean_2.csv", index=False)
   
drop_rows=PythonOperator(
    dag=scraping_dag,
    task_id="drop_rows",
    python_callable=_drop_rows,
    trigger_rule='all_success',
    depends_on_past=False,
)

def _change_data_type():
    wine_df = pd.read_csv("vivino_scrape_clean_2.csv")
    wine_df['Vintage'] = wine_df['Vintage'].astype(int)
    wine_df['Grape_ID'] = wine_df['Grape_ID'].astype(int)
    wine_df['Rating'] = wine_df['Rating'].astype(float)
    wine_df['Review_Count'] = wine_df['Review_Count'].astype(int)
    wine_df['Price'] = wine_df['Price'].astype(float)
    wine_df['Acidity'] = wine_df['Acidity'].astype(float)
    wine_df['Fizziness'] = wine_df['Fizziness'].astype(float)
    wine_df['Intensity'] = wine_df['Intensity'].astype(float)
    wine_df['Sweetness'] = wine_df['Sweetness'].astype(float)
    wine_df['Tannin'] = wine_df['Tannin'].astype(float)
    wine_df.to_csv("vivino_scrape_clean_3.csv", index=False)

change_data_type=PythonOperator(
    dag=scraping_dag,
    task_id="change_data_type",
    python_callable=_change_data_type,
    trigger_rule='all_success',
    depends_on_past=False,
)

def _to_parquet():
    wine_df = pd.read_csv("vivino_scrape_clean_3.csv")
    wine_df.to_parquet('data.parquet', engine='fastparquet')

to_parquet=PythonOperator(
    dag=scraping_dag,
    task_id="to_parquet",
    python_callable=_to_parquet,
    trigger_rule='all_success',
    depends_on_past=False,
)
    

create_connection >> scrape_vivino>> clean_data >> drop_rows >> change_data_type >> to_parquet