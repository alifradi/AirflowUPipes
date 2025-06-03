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
from data_getters.raw_getters import MTL_MAP #
from common.geocoding_utils import preprocess_addresses, geocode_batch_parallel, apply_coordinates_to_dataframe

TRANSFORMED_SCHEMA = 'public_transformed_data' #
SQL_FILE_PATH = os.path.join(os.path.dirname(__file__), 'yakin sep tables.sql') #

# Setup logger for the DAG file
dag_logger = logging.getLogger("airflow.task")


def run_sql_transformations_func(**context): #
    """
    Reads the SQL file as a Jinja2 template, renders it with the execution date,
    splits it into individual SELECT queries, wraps them in CREATE TABLE AS statements, and executes them.
    """
    execution_date = context.get('execution_date') #
    if execution_date is None: #
        from airflow.utils.dates import days_ago #
        execution_date = days_ago(0) #
    data_pull_date = execution_date.strftime('%Y-%m-%d') #

    with get_db_connection() as conn: #
        with conn.begin(): # Use a transaction for schema creation and table creations #
            create_schema_if_not_exists(TRANSFORMED_SCHEMA, conn) #

            if not os.path.exists(SQL_FILE_PATH): #
                raise FileNotFoundError(f"SQL file not found: {SQL_FILE_PATH}") #

            with open(SQL_FILE_PATH, 'r') as f: #
                sql_template = Template(f.read()) #
                sql_content = sql_template.render(data_pull_date=data_pull_date) #

            individual_select_queries = [] #
            current_query_lines = [] #
            for line in sql_content.splitlines(): #
                stripped_line = line.strip() #
                if (stripped_line.upper().startswith("SELECT") or stripped_line.upper().startswith("WITH")) and current_query_lines: #
                    if "".join(current_query_lines).strip(): #
                        individual_select_queries.append("\n".join(current_query_lines)) #
                    current_query_lines = [line] #
                elif stripped_line or current_query_lines: #
                    current_query_lines.append(line) #
            if current_query_lines and "".join(current_query_lines).strip(): #
                individual_select_queries.append("\n".join(current_query_lines)) #

            if not individual_select_queries: #
                dag_logger.info("No SQL queries found in the file.") #
                return #

            for i, select_query in enumerate(individual_select_queries): #
                select_query = select_query.strip() #
                if not select_query: #
                    continue #

                dag_logger.info(f"\nProcessing query {i+1}/{len(individual_select_queries)}:\n{select_query[:200]}...") #

                source_table_name_in_sql = None #
                from_match = re.search(r"FROM\s+(?:[a-zA-Z0-9_]+\.)?([a-zA-Z0-9_]+)", select_query, re.IGNORECASE) #
                if from_match: #
                    source_table_name_in_sql = from_match.group(1) #
                else: #
                    with_from_match = re.search(r"WITH\s+\w+\s+AS\s*\(\s*SELECT.*?\sFROM\s+(?:[a-zA-Z0-9_]+\.)?([a-zA-Z0-9_]+)", select_query, re.DOTALL | re.IGNORECASE) #
                    if with_from_match: #
                        source_table_name_in_sql = with_from_match.group(1) #
                if not source_table_name_in_sql: #
                    dag_logger.warning(f"WARNING: Could not determine a base table name for query: {select_query[:100]}... Using generic name 'transformed_query_{i+1}'.") #
                    output_table_name = f"transformed_query_{i+1}" #
                else: #
                    output_table_name = f"tf_{source_table_name_in_sql}" #

                def schema_replacer(match): #
                    keyword = match.group(1) #
                    table_name = match.group(2) #
                    if table_name.startswith('mtl_'): #
                        return f"{keyword}quebec_data.{table_name}" #
                    elif table_name.startswith('to_'): #
                        return f"{keyword}ontario_data.{table_name}" #
                    return match.group(0) #

                transformed_select_query = re.sub(r'(FROM\s+|JOIN\s+)([a-zA-Z0-9_]+)', schema_replacer, select_query, flags=re.IGNORECASE) #
                drop_table_sql = f"DROP TABLE IF EXISTS {TRANSFORMED_SCHEMA}.{output_table_name} CASCADE;" #
                dag_logger.info(f"Executing: {drop_table_sql}") #
                execute_sql_statement(drop_table_sql, conn) #

                create_table_sql = f"CREATE TABLE {TRANSFORMED_SCHEMA}.{output_table_name} AS ({transformed_select_query});" #
                dag_logger.info(f"Executing: CREATE TABLE {TRANSFORMED_SCHEMA}.{output_table_name} ...") #
                try: #
                    execute_sql_statement(create_table_sql, conn) #
                    dag_logger.info(f"Successfully created table {TRANSFORMED_SCHEMA}.{output_table_name}") #
                except Exception as e: #
                    dag_logger.error(f"ERROR creating table {TRANSFORMED_SCHEMA}.{output_table_name}: {e}") #
                    dag_logger.error(f"Failed SQL (approximate): {create_table_sql[:500]}...") #


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
    Geocodes datasets specified in MTL_MAP if they lack coordinates.
    Reads from the transformed table, adds lat/lon, and writes back.
    """
    dag_logger.info("Starting geocoding process for transformed data.")

    for dataset_key, dataset_info in MTL_MAP.items(): #
        # Process if 'has_coords' is False. If key is missing, assume True (i.e., no geocoding needed).
        if not dataset_info.get('has_coords', True):
            dag_logger.info(f"Dataset {dataset_key} is marked for geocoding (has_coords: False).")
            transformed_table_name = f"tf_{dataset_key}" # Based on convention in run_sql_transformations_func
            
            # Determine address column dynamically or use a mapping
            # For 'mtl_fines_food', the transformed SQL output uses 'address'
            address_column = dataset_info.get('address_column_transformed', 'address' if dataset_key == 'mtl_fines_food' else None)

            if not address_column:
                dag_logger.warning(f"No address column defined or inferable for {dataset_key} in its transformed version. Skipping geocoding.")
                continue

            try:
                with get_db_connection() as conn:
                    # Check if table exists in the transformed schema
                    # Using text() for sqlalchemy literal_binds compatibility if conn.execute is used directly
                    check_table_query = sqlalchemy.text(
                        f"SELECT EXISTS (SELECT FROM information_schema.tables "
                        f"WHERE table_schema = :schema AND table_name = :table);"
                    )
                    table_exists_result = conn.execute(check_table_query, {'schema': TRANSFORMED_SCHEMA, 'table': transformed_table_name})
                    table_exists = table_exists_result.scalar_one_or_none()


                    if not table_exists:
                        dag_logger.warning(f"Table {TRANSFORMED_SCHEMA}.{transformed_table_name} does not exist. Skipping geocoding.")
                        continue

                    dag_logger.info(f"Reading data from {TRANSFORMED_SCHEMA}.{transformed_table_name}")
                    # Use pandas read_sql_query for robust reading
                    df = pd.read_sql_query(f'SELECT * FROM {TRANSFORMED_SCHEMA}."{transformed_table_name}"', conn)

                    if df.empty:
                        dag_logger.info(f"Table {TRANSFORMED_SCHEMA}.{transformed_table_name} is empty. No geocoding needed.")
                        continue
                    
                    if address_column not in df.columns:
                        dag_logger.warning(f"Address column '{address_column}' not found in {TRANSFORMED_SCHEMA}.{transformed_table_name}. Skipping.")
                        continue
                    
                    dag_logger.info(f"Found {len(df)} records in {transformed_table_name}.")

                    unique_addresses = preprocess_addresses(df, address_column)
                    
                    if not unique_addresses.size: # Check if unique_addresses is empty (it's a numpy array from .unique())
                         dag_logger.info(f"No unique addresses to geocode in {transformed_table_name}.")
                         continue

                    # Geocode (max_workers and batch_size can be configured via Airflow variables if needed)
                    coordinate_results = geocode_batch_parallel(
                        list(unique_addresses), # Ensure it's a list
                        max_workers=context.get('params', {}).get('geocoding_max_workers', 4), # Example of using params
                        batch_size=context.get('params', {}).get('geocoding_batch_size', 50)
                    )
                    
                    df = apply_coordinates_to_dataframe(df, address_column, coordinate_results)
                    
                    successful_geocodes = df['latitude'].notna().sum()
                    total_unique_addresses = len(unique_addresses)
                    success_rate = (successful_geocodes / total_unique_addresses * 100) if total_unique_addresses > 0 else 0.0
                    dag_logger.info(f"Successfully geocoded {successful_geocodes}/{total_unique_addresses} unique addresses for {transformed_table_name}.")
                    dag_logger.info(f"Success rate: {success_rate:.1f}%")

                    # Write back to DB, replacing the existing table
                    dag_logger.info(f"Writing geocoded data back to {TRANSFORMED_SCHEMA}.{transformed_table_name}")
                    with conn.begin(): # Ensure write is in a transaction
                        # Drop existing table first to avoid conflicts with data types or constraints
                        conn.execute(sqlalchemy.text(f'DROP TABLE IF EXISTS {TRANSFORMED_SCHEMA}."{transformed_table_name}" CASCADE;'))
                        df.to_sql(
                            name=transformed_table_name,
                            con=conn, # Use the connection object
                            schema=TRANSFORMED_SCHEMA,
                            if_exists='append', # Should be 'append' after explicit drop, or 'replace' if not dropping
                            index=False,
                            method='multi' # Consistent with db_utils
                        )
                    dag_logger.info(f"Finished geocoding for {transformed_table_name}.")

            except Exception as e:
                dag_logger.error(f"Error during geocoding for {dataset_key} ({transformed_table_name}): {e}", exc_info=True)
                # Optionally re-raise to fail the task: raise

    dag_logger.info("Geocoding process for transformed data finished.")


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
        # You can pass params for geocoding workers/batch_size if needed
        # params={'geocoding_max_workers': 4, 'geocoding_batch_size': 50},
    )

    # Define dependencies
    [wait_for_quebec_data, wait_for_ontario_data] >> check_tables >> run_transformations >> geocode_data #