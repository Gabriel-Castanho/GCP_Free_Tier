from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import pymssql
import warnings
from google.cloud import storage
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 8),
    'email': ['your-email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}



dag = DAG(
    'BI_ShipMethod', default_args=default_args, schedule=timedelta(days=1))

def extrator_csv():
    
    conn = pymssql.connect(server='000.000.0.0', 
                           user='sa', 
                           password='#sa12345678', 
                           database='AdventureWorks2019')

    query = 'SELECT [ShipMethodID],[Name],[ShipBase],[ShipRate],[rowguid],[ModifiedDate] FROM [AdventureWorks2019].[Purchasing].[ShipMethod]'

    with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = pd.read_sql(query, conn)


    df.to_csv(r'/tmp/ShipMethod.csv', index=False,sep='|')
    conn.close()

Extrator_lojas = PythonOperator(
    task_id='Extrator',
    python_callable=extrator_csv,
    dag=dag)


def Upload_GCS():
    storage_client = storage.Client.from_service_account_json(r'/opt/airflow/GCP_KEYS/portiolio-1d08c6159b15.json')
    #buckets = list(storage_client.list_buckets())
    bucket = storage_client.get_bucket('airflow-dev-00')
    blob = bucket.blob('ShipMethod.csv')
    #print(buckets)

    #upload do arquivo CSV para o GCS
    blob.upload_from_filename(r'/tmp/ShipMethod.csv')

# Adicione esta tarefa ao seu DAG
Upload_bucket_gcs = PythonOperator(
    task_id='Upload_Bucket_gcs',
    python_callable=Upload_GCS,
    dag=dag)

load_BigQuery = GCSToBigQueryOperator(
    task_id='load_BigQuery',
    bucket='airflow-dev-00',
    source_objects=['ShipMethod.csv'],
    destination_project_dataset_table='portiolio.DW.d_ship_method',
    write_disposition='WRITE_TRUNCATE',
    field_delimiter='|',
    skip_leading_rows=1,
    dag=dag)



# Defina a ordem das tarefas
Extrator_lojas >> Upload_bucket_gcs >> load_BigQuery
