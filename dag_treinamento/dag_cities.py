from airflow import DAG
from airflow import configuration
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from datetime import datetime
import os


QUERIES_PATH = os.path.join(
    configuration.get('core', 'dags_folder'), 'queries')

START_DATE = datetime(2024, 1, 1, 00, 00, 0)

default_args = {
    'owner': 'Time de Dados',
    'depends_on_past': False,
    'start_date': START_DATE,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(dag_id= "dag_cities", 
         schedule_interval='0 18 * * *',
         default_args=default_args, 
         catchup=False, max_active_runs=2,
         on_failure_callback='',
         concurrency=2) as dag:
      

#CREATE
    create_refined_cities = BigQueryOperator(
        task_id="create_refined_cities", 
        write_disposition= "WRITE_TRUNCATE",
        sql="query/create_refined_cities.sql",
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
    )

    
#INSERT
    insert_refined_cities = BigQueryOperator(
        task_id="insert_refined_cities", 
        write_disposition= "WRITE_TRUNCATE",
        sql="query/insert_refined_cities.sql",
        use_legacy_sql=False,
        destination_dataset_table="bedu-tech-dev.cities.cities_refined",
        gcp_conn_id="google_cloud_default",
    )

create_refined_cities >>  insert_refined_cities
