from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import pymssql
import warnings
from google.cloud import storage


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
    'BI_Stores', default_args=default_args, schedule=timedelta(days=1))

def extrator_csv():
    
    conn = pymssql.connect(server='000.000.0.0', 
                           user='sa', 
                           password='#sa12345678', 
                           database='AdventureWorks2019')

    query = 'SELECT [BusinessEntityID],[Name],[SalesPersonID],[ModifiedDate] FROM [AdventureWorks2019].[Sales].[Store]'

    with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = pd.read_sql(query, conn)


    df.to_csv(r'/tmp/Store.csv', index=False,sep='|')
    conn.close()

Extrator_lojas = PythonOperator(
    task_id='Extrator',
    python_callable=extrator_csv,
    dag=dag)


def Upload_GCS():
    storage_client = storage.Client.from_service_account_json(r'/opt/airflow/GCP_KEYS/portiolio-1d08c6159b15.json')
    #buckets = list(storage_client.list_buckets())
    bucket = storage_client.get_bucket('airflow-dev-00')
    blob = bucket.blob('Store.csv')
    #print(buckets)

    #upload do arquivo CSV para o GCS
    blob.upload_from_filename(r'/tmp/Store.csv')

# Adicione esta tarefa ao seu DAG
Upload_bucket_gcs = PythonOperator(
    task_id='Upload_Bucket_gcs',
    python_callable=Upload_GCS,
    dag=dag)

# Defina a ordem das tarefas
Extrator_lojas >> Upload_bucket_gcs
