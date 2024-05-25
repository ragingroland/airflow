from datetime import datetime
import pandas as pd
import requests
from sqlalchemy import create_engine
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

pd_data = pd.DataFrame() #датафрейм для данных

def task_university_e(): # Забираем список словарей
    responce = requests.get('http://universities.hipolabs.com/search')
    responce = responce.json()
    pd_data = pd.DataFrame(responce)
    # Удаляем ненужные столбцы
    for key, value in pd_data.items():
        if key == 'domains' or key == 'web_pages':
            pd_data = pd_data.drop([key], axis=1)
    # создаем новый столбец и циклом 
    pd_data['uni_type'] = pd_data['name']
    for index, row in pd_data.iterrows():
        if 'College' in row['name']:
            pd_data.at[index, 'uni_type'] = 'College'
        elif 'Univer' in row['name']:
            pd_data.at[index, 'uni_type'] = 'University'
        elif 'Institute' in row['name']:
            pd_data.at[index, 'uni_type'] = 'Institute'
        else:
            pd_data.at[index, 'uni_type'] = ''
            
    pd_data = pd_data.rename(columns = {'state-province':'state_province'})
    
    pgsql_con = create_engine('postgresql+psycopg2://lydemere:admin@host.docker.internal:5432/postgres')
    pd_data.to_sql(
        name="university_domain",
        schema='public',
        con=pgsql_con,
        if_exists='replace',
        index=False)

with DAG(
    dag_id = 'university_test_task',
    description = 'Universities domain ETL',
    schedule_interval = '0 0 * * *',
    start_date = datetime.combine(datetime.today(), datetime.min.time()),
    catchup = False,
) as dag:
    task_create_pgsql_table = PostgresOperator (
        task_id="create_pgsql_table",
        sql="""
            CREATE TABLE IF NOT EXISTS university_domain (
            uni_id SERIAL PRIMARY KEY,
            alpha_two_code VARCHAR(2) NULL,
            country VARCHAR(100) NULL,
            name VARCHAR(200) NULL,
            state_province VARCHAR(100) NULL,
            uni_type VARCHAR(50) NULL);
          """,
        postgres_conn_id = 'postgres'
    )

    transforming_uni_data = PythonOperator (
        task_id = 'uni_data_transform',
        python_callable = task_university_e,
        provide_context = True
    )
    
    task_create_pgsql_table >> transforming_uni_data