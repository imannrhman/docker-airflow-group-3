from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.edgemodifier import Label
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime, timedelta
import requests
import json 

OPENAQ_API = "OPENAQ_API"
RESPONSE_KEY = "response_api"

default_args = {
    "owner" : "Kelompok 3",
    "depends_on_past": False,
    "retries": 2,
    'email_on_failure': False,
    'email_on_retry': False,
    "retry_delay": timedelta(minutes=5),
}

assignment_dag = DAG(
    dag_id="assignment_dag",
    start_date=datetime(2021, 10, 26),
    default_args=default_args,
    catchup=False,
    schedule_interval="0 4 * * *"
)


def fetch_data_air_quality(**context):
    url = Variable.get(OPENAQ_API)
    response = requests.get(url)
    return context['ti'].xcom_push(key="response_api", value=response.json())

def check_table_is_exist():
    hook = PostgresHook(postgres_conn_id="postgres_db")
    sql_query = "SELECT * FROM information_schema.tables WHERE table_name = 'air_quality';"
    query = hook.get_first(sql_query)
    print(query)
    if query:
        return 'insert_table_air_quality'
    else:     
        return 'create_table_air_quality'

def create_table_air_quality():
    hook = PostgresHook(postgres_conn_id="postgres_db")
    sql_query = '''CREATE TABLE IF NOT EXISTS air_quality (
    id INT PRIMARY KEY,
    city VARCHAR(100),
    name TEXT,
    entity VARCHAR(100),
    country TEXT,
    sources TEXT,
    isMobile BOOLEAN,
    isAnalysis BOOLEAN,
    parameters JSONB,
    sensorType TEXT,
    coordinates JSONB,
    lastUpdated DATE,
    firstUpdated DATE,
    measurements INT, 
    bounds JSONB,
    manufacturers JSONB
    );
    '''
    query = hook.get_first(sql_query)
    print(query)
    if query:
        print("Berhasil Membuat table")
    else:
        print("Tabel sudah dibuat")

def format_value(key: str, result, is_string=False, is_json=False):
    if result[key] != None:
        if is_string:
            return "'{}'".format(result[key]) 
        elif is_json:
            return "'{}'".format(json.dumps({ key : result[key]} ))
        else:
            return result[key]
    else:
        return "NULL"
        
def insert_table_air_quality(**context):
    hook = PostgresHook(postgres_conn_id="postgres_db")
    response_json = context['ti'].xcom_pull(key='response_api')
    for result in response_json['results'] :
        id = result['id'] 
        city = format_value('city', result, is_string=True)
        name = format_value('name', result, is_string=True)
        entity = format_value('entity', result, is_string=True)
        country = format_value('country', result, is_string=True)
        sources = format_value('sources', result, is_string=True)
        isMobile = format_value('isMobile', result) 
        isAnalysis = format_value('isAnalysis', result)
        parameters = format_value('parameters', result, is_json=True) # Seharusnya dibuat tabel khusus parameter karena berbentuk list json
        sensorType = format_value('sensorType', result, is_string=True)
        coordinates = format_value('coordinates', result, is_json=True) # Seharusnya dibuat tabel khusus coordinates karena berbentuk list json
        lastUpdated = format_value('lastUpdated', result, is_string=True)
        firstUpdated = format_value('firstUpdated', result, is_string=True)
        measurements = format_value('measurements', result) 
        bounds =  format_value('bounds', result, is_json=True)
        manufacturers = format_value('manufacturers', result, is_json=True) # Seharusnya dibuat tabel khusus manufacturers karena berbentuk list json

        sql_query = f''' INSERT INTO air_quality (id, city, name, 
            entity, country, sources, 
            isMobile, isAnalysis, parameters, 
            sensorType, coordinates, lastUpdated, firstUpdated, 
            measurements, bounds, manufacturers) VALUES
            ({id}, {city}, {name},
            {entity}, {country}, {sources},
            {isMobile}, {isAnalysis}, {parameters},
            {sensorType}, {coordinates}, {lastUpdated}, {firstUpdated},
            {measurements}, {bounds}, {manufacturers}
            )
            ON CONFLICT (id)
            DO
            UPDATE SET
                city = {city},
                name = {name},
                entity = {entity},
                country = {country},
                sources = {sources},
                isMobile = {isMobile},
                isAnalysis = {isAnalysis},
                parameters = {parameters},
                sensorType = {sensorType},
                coordinates = {coordinates},
                lastUpdated = {lastUpdated},
                firstUpdated = {firstUpdated},
                measurements = {measurements},
                bounds = {bounds},
                manufacturers = {manufacturers} 
            ;
        '''
        hook.run(sql_query, autocommit=True)


fetch_data_task = PythonOperator(
    task_id='fetch_data_air_quality',
    python_callable=fetch_data_air_quality,
    dag=assignment_dag,
)

check_table_is_exist_task = BranchPythonOperator(
    task_id='check_table_is_exist',
    python_callable= check_table_is_exist,
    dag=assignment_dag
)


create_table_air_quality_task = PythonOperator(
    task_id='create_table_air_quality',
    python_callable=create_table_air_quality,
    dag=assignment_dag
)

insert_table_air_quality_task = PythonOperator(
    task_id='insert_table_air_quality',
    python_callable=insert_table_air_quality,
    trigger_rule='one_success',
    provide_context=True,
    dag=assignment_dag
)



[fetch_data_task, check_table_is_exist_task]
check_table_is_exist_task >> [create_table_air_quality_task, insert_table_air_quality_task]
create_table_air_quality_task >> insert_table_air_quality_task