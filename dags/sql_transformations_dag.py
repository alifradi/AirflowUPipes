# dags/sql_transformations_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator # Correct import
from airflow.utils.session import provide_session # Correct import
from airflow.models import DagRun, TaskInstance # Correct import
from airflow.utils.state import State # Correct import
from datetime import datetime, timedelta
import os
import re
import sqlalchemy
from jinja2 import Template
import pandas as pd
import logging # Add logging

# Ensure correct import path if db_utils is in a subfolder like 'common'
from common.db_utils import get_db_connection, create_schema_if_not_exists, execute_sql_statement
# Import MTL_MAP and geocoding utilities
from data_getters.raw_getters import MTL_MAP, TO_MAP
from common.geocoding_utils import preprocess_addresses, geocode_batch_parallel, apply_coordinates_to_dataframe

# Function to check if a table has coordinates
def has_coordinates(table_name):
    """Check if a table has latitude/longitude columns"""
    with get_db_connection() as conn:
        query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            AND (column_name ILIKE '%lat%' OR column_name ILIKE '%long%');
        """
        df = pd.read_sql(query, conn)
        return not df.empty

# Function to check if a table has address column
def has_address_column(table_name):
    """Check if a table has an address column"""
    with get_db_connection() as conn:
        query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            AND column_name ILIKE '%address%';
        """
        df = pd.read_sql(query, conn)
        return not df.empty

# Function to determine tables needing geocoding
def get_tables_needing_geocoding():
    """Get list of tables that need geocoding"""
    tables_needing_geocoding = []
    
    # Check TO_MAP tables
    for table_name, info in TO_MAP.items():
        if not info.get('has_coords', False) and has_address_column(table_name):
            tables_needing_geocoding.append(table_name)
    
    # Check MTL_MAP tables
    for table_name, info in MTL_MAP.items():
        if not info.get('has_coords', False) and has_address_column(table_name):
            tables_needing_geocoding.append(table_name)
    
    return tables_needing_geocoding

TRANSFORMED_SCHEMA = 'public_transformed_data' #
QUEBEC_SQL_FILE_PATH = os.path.join(os.path.dirname(__file__), 'quebec_transformations.sql')
ONTARIO_SQL_FILE_PATH = os.path.join(os.path.dirname(__file__), 'ontario_transformations.sql')

# Setup logger for the DAG file
dag_logger = logging.getLogger("airflow.task")


def run_sql_transformations_func(**context):
    """
    Runs Quebec and Ontario transformation SQL scripts as part of the DAG.
    Reads each SQL file as a Jinja2 template, renders it with the execution date,
    splits it into individual SELECT queries, wraps them in CREATE TABLE AS statements, and executes them.
    """
    execution_date = context.get('execution_date')
    if execution_date is None:
        from airflow.utils.dates import days_ago
        execution_date = days_ago(0)
    data_pull_date = execution_date.strftime('%Y-%m-%d')

    sql_files = [QUEBEC_SQL_FILE_PATH, ONTARIO_SQL_FILE_PATH]
    with get_db_connection() as conn:
        # Create the schema outside the transaction for transformations
        dag_logger.info(f"Attempting to create schema: {TRANSFORMED_SCHEMA}")
        try:
            create_schema_if_not_exists(TRANSFORMED_SCHEMA, conn)
            dag_logger.info(f"Schema '{TRANSFORMED_SCHEMA}' created or already exists.")
        except Exception as e:
            dag_logger.error(f"Failed to create schema '{TRANSFORMED_SCHEMA}': {e}")
            raise  # Fail the task so you see the error

        with conn.begin():
            for SQL_FILE_PATH in sql_files:
                if not os.path.exists(SQL_FILE_PATH):
                    dag_logger.warning(f"SQL file not found: {SQL_FILE_PATH}, skipping.")
                    continue
                dag_logger.info(f"Processing SQL file: {SQL_FILE_PATH}")
                with open(SQL_FILE_PATH, 'r') as f:
                    sql_template = Template(f.read())
                    sql_content = sql_template.render(data_pull_date=data_pull_date)

                # Split SQL script by semicolon (standard SQL statement delimiter)
                individual_select_queries = [q.strip() for q in sql_content.split(';') if q.strip()]

                if not individual_select_queries:
                    dag_logger.info(f"No SQL queries found in the file {SQL_FILE_PATH}.")
                    continue

                dag_logger.info(f"Executing entire SQL script for {os.path.basename(SQL_FILE_PATH)}")
                try:
                    queries = [q.strip() for q in sql_content.split(';') if q.strip()]
                    for i, query in enumerate(queries, 1):
                        if not query or all(line.strip().startswith('--') for line in query.splitlines() if line.strip()):
                            dag_logger.info(f"Skipping comment-only or empty query block at position {i} in {os.path.basename(SQL_FILE_PATH)}.")
                            continue
                        try:
                            dag_logger.info(f"Executing query {i}/{len(queries)} from {os.path.basename(SQL_FILE_PATH)}:\n{query[:200]}...")
                            execute_sql_statement(query, conn)
                            dag_logger.info(f"Successfully executed query {i}")
                        except Exception as e:
                            dag_logger.error(f"Error executing query {i}: {str(e)}")
                            raise
                except Exception as e:
                    dag_logger.error(f"Error executing SQL transformations: {str(e)}")
                    raise

            dag_logger.info("All SQL file transformations completed.")


def check_required_tables_exist(): #
    """
    Checks that all required source tables exist in the database before running transformations.
    """
    required_tables = [ #
        ('quebec_data', 'mtl_fines_food'), #
         ('ontario_data', 'to_business') # Added based on raw_getters.py and typical usage
    ]
    with get_db_connection() as conn: #
        inspector = sqlalchemy.inspect(conn) #
        missing = [] #
        for schema, table in required_tables: #
            if not inspector.has_table(table, schema=schema): #
                missing.append(f"{schema}.{table}") #
        if missing: #
            raise RuntimeError(f"Missing required tables: {', '.join(missing)}") #
        dag_logger.info("All required tables exist.") #

class LatestDagSuccessSensor(BaseSensorOperator): #
    def __init__(self, external_dag_id, external_task_id, *args, **kwargs): #
        super().__init__(*args, **kwargs) #
        self.external_dag_id = external_dag_id #
        self.external_task_id = external_task_id #

    @provide_session #
    def poke(self, context, session=None): #
        latest_run = ( #
            session.query(DagRun) #
            .filter(DagRun.dag_id == self.external_dag_id, DagRun.state == State.SUCCESS) #
            .order_by(DagRun.execution_date.desc()) #
            .first() #
        )
        if not latest_run: #
            self.log.info(f"No successful DagRun found for {self.external_dag_id} yet.") #
            return False #
        ti = ( #
            session.query(TaskInstance) #
            .filter( #
                TaskInstance.dag_id == self.external_dag_id, #
                TaskInstance.task_id == self.external_task_id, #
                TaskInstance.execution_date == latest_run.execution_date, #
                TaskInstance.state == State.SUCCESS, #
            )
            .first() #
        )
        if ti: #
            self.log.info(f"Found successful {self.external_task_id} in {self.external_dag_id} at {latest_run.execution_date}.") #
            return True #
        self.log.info(f"Latest DagRun for {self.external_dag_id} does not have successful {self.external_task_id} yet.") #
        return False #

def geocode_transformed_data_func(**context):
    """
    Geocodes transformed tables that have an address column but no coordinates.
    Accepts max_workers and batch_size from Airflow context for parallel geocoding performance tuning.
    """
    dag_logger.info("Starting geocoding of transformed tables...")

    # Get geocoding parallelism parameters from context or use defaults
    max_workers = context.get('max_workers', 12)
    batch_size = context.get('batch_size', 100)

    with get_db_connection() as conn:
        # Get all transformed tables
        query = sqlalchemy.text(
            """SELECT table_name FROM information_schema.tables 
            WHERE table_schema = :schema AND table_name LIKE 'tf_%'"""
        )
        result = conn.execute(query, {'schema': TRANSFORMED_SCHEMA})
        transformed_tables = [row[0] for row in result]

        for table_name in transformed_tables:
            try:
                dag_logger.info(f"\nProcessing table: {table_name}")
                
                # Check if table has coordinates
                has_coords_query = sqlalchemy.text(
                    """SELECT column_name FROM information_schema.columns 
                        WHERE table_schema = :schema 
                        AND table_name = :table 
                        AND (column_name = 'latitude' OR column_name = 'longitude')
                    """
                )
                coords_result = conn.execute(has_coords_query, {'schema': TRANSFORMED_SCHEMA, 'table': table_name}).fetchall()
                has_lat = any(row[0].lower() == 'latitude' for row in coords_result)
                has_long = any(row[0].lower() == 'longitude' for row in coords_result)

                # Check if table has an address column
                has_address_query = sqlalchemy.text(
                    """SELECT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_schema = :schema 
                        AND table_name = :table 
                        AND column_name = 'address'
                    )"""
                )
                has_address = conn.execute(has_address_query, {'schema': TRANSFORMED_SCHEMA, 'table': table_name}).scalar_one_or_none()

                if not has_address:
                    dag_logger.info(f"Table {table_name} has no address column. Skipping geocoding.")
                    continue

                # If address exists but latitude/longitude do not, add them
                if has_address and (not has_lat or not has_long):
                    alter_sql = f"""
                        ALTER TABLE {TRANSFORMED_SCHEMA}.{table_name}
                        {'' if has_lat else 'ADD COLUMN latitude REAL,'}
                        {'' if has_long else 'ADD COLUMN longitude REAL,'}
                        """.replace(',\n"""', '\n"""').replace('ALTER TABLE', 'ALTER TABLE').strip()
                    # Remove trailing comma and newline if present
                    alter_sql = alter_sql.rstrip(',\n')
                    dag_logger.info(f"Altering table {table_name} to add latitude/longitude columns if missing.")
                    try:
                        conn.execute(sqlalchemy.text(alter_sql))
                        dag_logger.info(f"Successfully altered table {table_name}.")
                    except Exception as e:
                        dag_logger.warning(f"Could not alter table {table_name}: {e}")
                        continue

                # Refresh check for coordinates after ALTER
                coords_result = conn.execute(has_coords_query, {'schema': TRANSFORMED_SCHEMA, 'table': table_name}).fetchall()
                has_lat = any(row[0].lower() == 'latitude' for row in coords_result)
                has_long = any(row[0].lower() == 'longitude' for row in coords_result)
                if not (has_lat and has_long):
                    dag_logger.warning(f"Table {table_name} still missing latitude or longitude columns after attempt to add. Skipping geocoding.")
                    continue

                # Check if the table has latitude and longitude columns but they are NULL
                has_null_coords_query = sqlalchemy.text(
                    f"SELECT EXISTS ("
                    f"    SELECT 1 FROM {TRANSFORMED_SCHEMA}.{table_name}"
                    f"    WHERE latitude IS NULL OR longitude IS NULL"
                    f"    LIMIT 1"
                    f")"
                )
                has_null_coords = conn.execute(has_null_coords_query).scalar_one_or_none()

                if not has_null_coords:
                    dag_logger.info(f"Table {table_name} has coordinates for all rows. Skipping geocoding.")
                    continue

                # Get the address column
                address_query = sqlalchemy.text(
                    """SELECT column_name FROM information_schema.columns 
                    WHERE table_schema = :schema 
                    AND table_name = :table 
                    AND column_name = 'address'"""
                )
                address_result = conn.execute(address_query, {'schema': TRANSFORMED_SCHEMA, 'table': table_name})
                address_column = address_result.scalar_one_or_none()

                if not address_column:
                    dag_logger.warning(f"Could not find address column in {table_name}. Skipping geocoding.")
                    continue

                # Get addresses that need geocoding
                addresses_query = sqlalchemy.text(
                    f"SELECT DISTINCT {address_column} FROM {TRANSFORMED_SCHEMA}.{table_name} "
                    f"WHERE latitude IS NULL OR longitude IS NULL"
                )
                addresses_result = conn.execute(addresses_query)
                addresses = [row[0] for row in addresses_result]

                if not addresses:
                    dag_logger.info(f"No addresses found in {table_name} that need geocoding. Skipping.")
                    continue

                dag_logger.info(f"Found {len(addresses)} unique addresses to geocode in {table_name}")

                # Geocode addresses in batches
                batch_size = 100
                num_batches = (len(addresses) + batch_size - 1) // batch_size

                for batch_idx in range(num_batches):
                    start_idx = batch_idx * batch_size
                    end_idx = min(start_idx + batch_size, len(addresses))
                    batch_addresses = addresses[start_idx:end_idx]

                    dag_logger.info(f"Geocoding batch {batch_idx + 1}/{num_batches} ({len(batch_addresses)} addresses)")

                    try:
                        # Geocode the addresses
                        coordinates = geocode_batch_parallel(batch_addresses)
                        
                        # Update the table with coordinates
                        update_query = sqlalchemy.text(
                            f"UPDATE {TRANSFORMED_SCHEMA}.{table_name} "
                            f"SET latitude = :lat, longitude = :lon "
                            f"WHERE {address_column} = :address "
                            f"AND (latitude IS NULL OR longitude IS NULL)"
                        )

                        for address, (lat, lon) in coordinates.items():
                            if lat is not None and lon is not None:
                                conn.execute(update_query, {
                                    'lat': lat,
                                    'lon': lon,
                                    'address': address
                                })
                        
                        # conn.commit()  # Removed: not available on SQLAlchemy connection object
                        dag_logger.info(f"Successfully updated coordinates for batch {batch_idx + 1}")

                    except Exception as e:
                        dag_logger.error(f"Error geocoding batch {batch_idx + 1}: {e}")
                        # conn.rollback()  # Removed: not available on SQLAlchemy connection object

            except Exception as e:
                dag_logger.error(f"Error processing table {table_name}: {e}")
                continue

    dag_logger.info("Geocoding completed.")


default_args_transforms = { #
    'owner': 'airflow', #
    'start_date': datetime(2024, 1, 1), #
    'retries': 0, #
    'retry_delay': timedelta(minutes=5), #
    'depends_on_past': False, #
}

with DAG(
    dag_id='sql_transformations_pipeline', #
    default_args=default_args_transforms, #
    description='Runs SQL transformations and geocoding after data loading.', # Updated description
    schedule_interval=None, #
    catchup=False, #
    tags=['transformations', 'sql', 'yakin', 'geocoding'], # Updated tags
) as dag:
    wait_for_quebec_data = LatestDagSuccessSensor( #
        task_id='wait_for_quebec_data_pipeline_completion', #
        external_dag_id='quebec_open_data_pipeline', #
        external_task_id='end_pipeline', #
        poke_interval=120, #
        timeout=7200, #
        mode='poke', #
    )

    wait_for_ontario_data = LatestDagSuccessSensor( #
        task_id='wait_for_ontario_data_pipeline_completion', #
        external_dag_id='ontario_open_data_pipeline', #
        external_task_id='end_pipeline', #
        poke_interval=120, #
        timeout=7200, #
        mode='poke', #
    )

    check_tables = PythonOperator( #
        task_id='check_required_tables_exist', #
        python_callable=check_required_tables_exist, #
    )

    run_transformations = PythonOperator( #
        task_id='run_sql_file_transformations', #
        python_callable=run_sql_transformations_func, #
    )

    # New Geocoding Task
    geocode_data = PythonOperator(
    task_id='geocode_transformed_data',
    python_callable=geocode_transformed_data_func,
    op_kwargs={'max_workers': 12, 'batch_size': 100},
)

    # Define dependencies
    wait_for_quebec_data  >> check_tables >> run_transformations >> geocode_data
    wait_for_ontario_data >> check_tables >> run_transformations >> geocode_data 
