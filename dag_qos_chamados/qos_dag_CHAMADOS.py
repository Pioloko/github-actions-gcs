from airflow import DAG
from airflow import configuration
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
import os
from datetime import datetime


QUERIES_PATH = os.path.join(
    configuration.get('core', 'dags_folder'))

START_DATE = datetime(1900, 1, 1, 00, 00, 0)

default_args = {
    'owner': 'Qos',
    'depends_on_past': False,
    'start_date': START_DATE,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    'dataflow_default_options': {
        'temp_location':    "gs://dataflow_temp_datalake/temp",
        "project":          "analytics-qos",
        "machineType":      "n2-standard-2"
        
    }
}




with DAG(dag_id= "dag-qos-chamados", 
         schedule_interval='0 9 * * *',
         default_args=default_args, 
         catchup=False, max_active_runs=2,
         on_failure_callback='',
         concurrency=3) as dag:
         

    #insert 
         
    insert_table_chamados  = DataflowTemplateOperator(
        task_id = 'insert-raw-qos-CHAMADOS_VW',
        template= 'gs://dataflow-templates-us-central1/latest/Jdbc_to_BigQuery',
        location='us-central1',
        parameters = {
            "driverClassName": "com.mysql.cj.jdbc.Driver",
            "driverJars" : "gs://dataflow-driver/mysql-connector-java-8.0.23.jar",      
            "connectionURL" : "jdbc:mysql://45.225.26.79:3306/otrs",
            "query" : "SELECT c.*, i.INEP , DATE(NOW()) AS dt_carga FROM otrs_CHAMADOS AS c INNER JOIN otrs_CHAMADOS_DADOS AS i ON c.OS = i.OS;",
            "outputTable" : "analytics-qos:qos_noc.otrs_CHAMADOS_DF",
            "username" : "lookerstd",
            "password" : "290&*HJUYANH",
            "bigQueryLoadingTemporaryDirectory": "gs://dataflow_temp_datalake/temp_bq/",
        },
        gcp_conn_id='google_cloud_default'  
    )

    create_qos_chamados_refined = BigQueryOperator(
        task_id="create_qos_chamados_refined", 
        write_disposition= "WRITE_TRUNCATE",
        sql=f"sql/refined/dag_qos_chamados_create.sql",
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
    )