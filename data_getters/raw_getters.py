from datetime import datetime
from pandera import Check 
import pandas as pd
import os
from typing import Optional,Tuple, Callable
import logging
from logging import INFO, WARNING, ERROR
import sys
#import pyarrow
import requests
import pandera as pa
sys.path.append('../')
from data_getters.quebec_getters import fetch_data
from data_getters.ontario_getters import fetch_toronto_data

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/opt/airflow/logs/generic_data_fetcher.log')
    ]
)
logger = logging.getLogger(__name__)

# Mapping from table_name to column order (from 02_import_tables.sql)
TABLE_COLUMNS = {
#'to_fines_buidlings': [ ... ],
    'to_Business': ["_id","AREA_ID","DATE_EFFECTIVE","AREA_ATTR_ID","PARENT_AREA_ID","AREA_SHORT_CODE","AREA_LONG_CODE","AREA_NAME","AREA_DESC","OBJECTID","geometry"],
   }

def fetch_and_store_quebec_dataset(
    resource_id: str,
    output_dir: str = "../master_database/init-scripts/",
    dataset_name: Optional[str] = None,
    limit: Optional[int] = None,
    ssl_verify: bool = False,
    schema: Optional[pa.DataFrameSchema] = None,
    preprocess_func: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    connection=None,
    table_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    batch_size: int = 1000,
    **kwargs
) -> Tuple[str, str, int]:
    """
    Fetch, process, and store Québec Open Data datasets in chunks to avoid OOM.
    Writes each batch directly to Postgres if connection and table_name are provided.
    All columns are loaded as text (string) and in the correct order for the raw table.
    All type conversion/cleaning should be done in SQL after loading.
    Returns (parquet_path, csv_path, total_rows_written)
    """
    os.makedirs(output_dir, exist_ok=True)
    execution_date = kwargs.get('execution_date', datetime.now())
    name_safe = dataset_name or f"dataset_{resource_id[:8]}"
    base_filename = f"{name_safe}"
    parquet_path = os.path.join(output_dir, f"{base_filename}.parquet")
    csv_path = os.path.join(output_dir, f"{base_filename}.csv")
    total_rows = 0
    try:
        logger.info(f"Fetching dataset {resource_id} ({name_safe}) in batches")
        # Get total count
        all_records = []
        for batch in fetch_data(
            resource_id=resource_id,
            limit=limit,
            batch_size=batch_size,
            ssl_verify=ssl_verify
        ):
            if not batch:
                continue
            # Log the structure of batch for debugging
            logger.critical(f"BATCH STRUCTURE: {type(batch)} - {str(batch)[:500]}")
            if not batch:
                df = pd.DataFrame()  # Empty dataset
            elif isinstance(batch, dict):
                logger.info(f"Handling single-record dictionary for {resource_id}")
                df = pd.DataFrame([batch])
            elif isinstance(batch, list) and batch and not isinstance(batch[0], dict):
                logger.warning(f"Scalar list detected for {resource_id}. Creating 'value' column")
                df = pd.DataFrame(batch, columns=["value"])
            else:
                df = pd.DataFrame(batch)
            if preprocess_func:
                logger.info("Applying preprocessing function")
                df = preprocess_func(df)
            if schema:
                logger.info("Validating with schema")
                schema.validate(df)
            # --- Ensure all columns are text and in correct order ---
            if table_name in TABLE_COLUMNS:
                col_order = TABLE_COLUMNS[table_name]
                col_order = [col for col in col_order if col in df.columns]
                df = df[col_order]
            df = df.astype(str)
            if connection and table_name:
                logger.info(f"Writing batch to Postgres: {table_name}")
                df.to_sql(table_name, connection, if_exists='append', index=False, schema=schema_name, method='multi', chunksize=1000)
            df.to_csv(csv_path, mode='a', header=not os.path.exists(csv_path), encoding='utf-8-sig', index=False)
            total_rows += len(df)
        logger.info(f"Total rows written: {total_rows}")
        return parquet_path, csv_path, total_rows
    except Exception as e:
        logger.error(f"Failed to process {resource_id}: {str(e)}")
        raise


MTL_MAP = {
    'mtl_fines_food': {
        'resource_id': '7f939a08-be8a-45e1-b208-d8744dca8fc6',
        'has_coords': False,
        'category': 'alimentaires'}
    }

def fetch_and_store_ontario_dataset(
    package_id: str,
    output_dir: str = "../master_database/init-scripts/",
    dataset_name: Optional[str] = None,
    limit: Optional[int] = None,
    ssl_verify: bool = False,
    schema: Optional[pa.DataFrameSchema] = None,
    preprocess_func: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    base_url: str = "https://ckan0.cf.opendata.inter.prod-toronto.ca",
    connection=None,
    table_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    chunk_size: int = 10000,
    **kwargs
) -> Tuple[str, str, int]:
    """
    Fetch, process, and store Toronto Open Data datasets in chunks to avoid OOM.
    Writes each chunk directly to Postgres if connection and table_name are provided.
    All columns are loaded as text (string) and in the correct order for the raw table.
    All type conversion/cleaning should be done in SQL after loading.
    Returns (csv_path, total_rows_written)
    """
    import numpy as np
    os.makedirs(output_dir, exist_ok=True)
    execution_date = kwargs.get('execution_date', datetime.now())
    name_safe = dataset_name or f"dataset_{package_id[:8]}"
    base_filename = f"{name_safe}"
    parquet_path = os.path.join(output_dir, f"{base_filename}.parquet")
    csv_path = os.path.join(output_dir, f"{base_filename}.csv")
    total_rows = 0

    try:
        logger.info(f"Fetching dataset {package_id} ({name_safe})")
        from data_getters.ontario_getters import fetch_toronto_data
        df = fetch_toronto_data(
            base_url=base_url,
            package_id=package_id,
            num_observations=limit
        )
        if df.empty:
            logger.info(f"Empty DataFrame {dataset_name} returned from API{package_id}")
            return csv_path, 0
        if preprocess_func:
            logger.info("Applying preprocessing function")
            df = preprocess_func(df)
        if schema:
            logger.info("Validating with schema")
            schema.validate(df)
        # --- Ensure all columns are text and in correct order ---
        if table_name in TABLE_COLUMNS:
            col_order = TABLE_COLUMNS[table_name]
            # Only keep columns that exist in the DataFrame
            col_order = [col for col in col_order if col in df.columns]
            df = df[col_order]
        # Cast all columns to string (text)
        df = df.astype(str).replace({"nan": np.nan, "None": np.nan})
        df.to_csv(csv_path, encoding='utf-8-sig', index=False)
        if connection and table_name:
            logger.info(f"Writing to Postgres in chunks: {table_name}")
            for chunk in pd.read_csv(csv_path, chunksize=chunk_size, dtype=str):
                chunk = chunk.astype(str).replace({"nan": np.nan, "None": np.nan})
                chunk.to_sql(table_name, connection, if_exists='append', index=False, schema=schema_name, method='multi', chunksize=1000)
                total_rows += len(chunk)
        else:
            total_rows = len(df)
        logger.info(f"Total rows written: {total_rows}")
        return csv_path, total_rows
    except Exception as e:
        logger.error(f"Failed to process {package_id}: {str(e)}", exc_info=True)
        raise


TO_MAP = {
   
     'to_Business': {
        'resource_id': 'business-improvement-areas',
        'has_coords': True
    }
}