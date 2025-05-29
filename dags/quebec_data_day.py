from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
import os
#import pyarrow
import sys
import pandera as pa
from data_getters.raw_getters import fetch_and_store_quebec_dataset, MTL_MAP #
from common.db_utils import get_db_connection, df_to_sql, create_schema_if_not_exists

OUTPUT_DIR_CONTAINER = os.getenv("OUTPUT_DIR_CONTAINER", "/opt/airflow/master_database/init-scripts/") #
POSTGRES_SCHEMA = 'quebec_data'

def create_pg_schema():
    with get_db_connection() as conn:
        with conn.begin(): # Start transaction
            create_schema_if_not_exists(POSTGRES_SCHEMA, conn)

def process_dataset(dataset_key: str, dataset_info: dict):
    resource_id = dataset_info['resource_id']
    dataset_name = dataset_key
    table_name = dataset_key.lower()
    with get_db_connection() as conn:
        with conn.begin():
            parquet_path, csv_path, total_rows = fetch_and_store_quebec_dataset(
                resource_id=resource_id,
                dataset_name=dataset_name,
                output_dir=OUTPUT_DIR_CONTAINER,
                ssl_verify=False,
                connection=conn,
                table_name=table_name,
                schema_name=POSTGRES_SCHEMA
            )
    print(f"Loaded {dataset_name} to {POSTGRES_SCHEMA}.{table_name} with {total_rows} rows.")

default_args = {'owner': 'airflow', 'start_date': datetime(2024,1,1), 'retries': 1, 'retry_delay': timedelta(minutes=2)}

with DAG(
    dag_id='quebec_open_data_pipeline',
    default_args=default_args,
    description='Fetch, process, and store Quebec open datasets.',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['quebec', 'open-data'],
) as dag:
    start = EmptyOperator(task_id='start_pipeline')
    ensure_schema = PythonOperator(task_id='create_quebec_schema', python_callable=create_pg_schema)
    end = EmptyOperator(task_id='end_pipeline')

    for key, info in MTL_MAP.items(): #
        process_task = PythonOperator(
            task_id=f'process_{key}',
            python_callable=process_dataset,
            op_kwargs={'dataset_key': key, 'dataset_info': info},
        )
        start >> ensure_schema >> process_task >> end