# dags/sql_transformations_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import os
import re

# Ensure correct import path if db_utils is in a subfolder like 'common'
# For Airflow, if 'dags' is in sys.path, and 'common' is a subfolder of 'dags':
from common.db_utils import get_db_connection, create_schema_if_not_exists, execute_sql_statement

TRANSFORMED_SCHEMA = 'public_transformed_data'
# Ensure this path is correct relative to where Airflow executes DAGs
SQL_FILE_PATH = os.path.join(os.path.dirname(__file__), 'yakin sep tables.sql')


def run_sql_transformations_func():
    """
    Reads the SQL file, splits it into individual SELECT queries,
    wraps them in CREATE TABLE AS statements, and executes them.
    """
    with get_db_connection() as conn:
        with conn.begin(): # Use a transaction for schema creation and table creations
            create_schema_if_not_exists(TRANSFORMED_SCHEMA, conn)

            if not os.path.exists(SQL_FILE_PATH):
                raise FileNotFoundError(f"SQL file not found: {SQL_FILE_PATH}")

            with open(SQL_FILE_PATH, 'r') as f:
                sql_content = f.read()

            # Split queries: Assumes queries are separated by one or more blank lines
            # and each "effective" query block starts with SELECT or WITH.
            individual_select_queries = []
            current_query_lines = []
            for line in sql_content.splitlines():
                stripped_line = line.strip()
                # Start of a new query block
                if (stripped_line.upper().startswith("SELECT") or stripped_line.upper().startswith("WITH")) and current_query_lines:
                    # Only add if the accumulated query is not just whitespace
                    if "".join(current_query_lines).strip():
                        individual_select_queries.append("\n".join(current_query_lines))
                    current_query_lines = [line]
                # Continue current query block or start the very first one
                elif stripped_line or current_query_lines: # Add non-empty lines or if already in a query
                    current_query_lines.append(line)
            
            # Add the last query if any
            if current_query_lines and "".join(current_query_lines).strip():
                individual_select_queries.append("\n".join(current_query_lines))

            if not individual_select_queries:
                print("No SQL queries found in the file.")
                return

            for i, select_query in enumerate(individual_select_queries):
                select_query = select_query.strip()
                if not select_query:
                    continue

                print(f"\nProcessing query {i+1}/{len(individual_select_queries)}:\n{select_query[:200]}...")

                # Attempt to infer a base table name for the output table
                source_table_name_in_sql = None
                # Regex for 'FROM table_name' or 'FROM schema.table_name'
                from_match = re.search(r"FROM\s+(?:[a-zA-Z0-9_]+\.)?([a-zA-Z0-9_]+)", select_query, re.IGNORECASE)
                if from_match:
                    source_table_name_in_sql = from_match.group(1)
                else:
                    # Handle queries starting with WITH, e.g., WITH cte AS (SELECT ... FROM base_table) SELECT ... FROM cte
                    # This regex looks for the first FROM inside a WITH block's subquery
                    with_from_match = re.search(r"WITH\s+\w+\s+AS\s*\(\s*SELECT.*?\sFROM\s+(?:[a-zA-Z0-9_]+\.)?([a-zA-Z0-9_]+)", select_query, re.DOTALL | re.IGNORECASE)
                    if with_from_match:
                        source_table_name_in_sql = with_from_match.group(1)
                
                if not source_table_name_in_sql:
                    print(f"WARNING: Could not determine a base table name for query: {select_query[:100]}... Using generic name 'transformed_query_{i+1}'.")
                    output_table_name = f"transformed_query_{i+1}"
                else:
                    output_table_name = f"tf_{source_table_name_in_sql}"

                # Automatically prefix schema names to tables in FROM and JOIN clauses
                # This is a critical step as the SQL file doesn't have schemas.
                def schema_replacer(match):
                    keyword = match.group(1) # FROM or JOIN
                    table_name = match.group(2) # The table name
                    if table_name.startswith('mtl_'):
                        return f"{keyword}quebec_data.{table_name}"
                    elif table_name.startswith('to_'):
                        return f"{keyword}ontario_data.{table_name}"
                    return match.group(0) # No change if no prefix match

                # Regex to find 'FROM table' or 'JOIN table'
                transformed_select_query = re.sub(r'(FROM\s+|JOIN\s+)([a-zA-Z0-9_]+)', schema_replacer, select_query, flags=re.IGNORECASE)
                
                # Drop table if exists, for idempotency
                drop_table_sql = f"DROP TABLE IF EXISTS {TRANSFORMED_SCHEMA}.{output_table_name} CASCADE;"
                print(f"Executing: {drop_table_sql}")
                execute_sql_statement(drop_table_sql, conn)

                create_table_sql = f"CREATE TABLE {TRANSFORMED_SCHEMA}.{output_table_name} AS ({transformed_select_query});"
                
                print(f"Executing: CREATE TABLE {TRANSFORMED_SCHEMA}.{output_table_name} ...")
                try:
                    execute_sql_statement(create_table_sql, conn)
                    print(f"Successfully created table {TRANSFORMED_SCHEMA}.{output_table_name}")
                except Exception as e:
                    print(f"ERROR creating table {TRANSFORMED_SCHEMA}.{output_table_name}: {e}")
                    print(f"Failed SQL (approximate): {create_table_sql[:500]}...")
                    # Optionally, re-raise the exception if you want the DAG task to fail hard
                    # raise

default_args_transforms = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1), # Adjust as needed
    'retries': 0, # Set to 0 to avoid rerunning complex transformations automatically on failure
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}

with DAG(
    dag_id='sql_transformations_pipeline',
    default_args=default_args_transforms,
    description='Runs SQL transformations from yakin sep tables.sql after data loading.',
    schedule_interval=None, # This DAG will be triggered by sensors or run manually.
    catchup=False,
    tags=['transformations', 'sql', 'yakin'],
) as dag:
    # Sensor for the Quebec data pipeline
    wait_for_quebec_data = ExternalTaskSensor(
        task_id='wait_for_quebec_data_pipeline_completion',
        external_dag_id='quebec_open_data_pipeline',
        external_task_id='end_pipeline', # Assumes 'end_pipeline' is the last task in Quebec DAG
        timeout=7200, # 2 hours
        allowed_states=['success'],
        mode='poke',
        poke_interval=120, # Check every 2 minutes
    )

    # Sensor for the Ontario data pipeline
    wait_for_ontario_data = ExternalTaskSensor(
        task_id='wait_for_ontario_data_pipeline_completion',
        external_dag_id='ontario_open_data_pipeline',
        external_task_id='end_pipeline', # Assumes 'end_pipeline' is the last task in Ontario DAG
        timeout=7200, # 2 hours
        allowed_states=['success'],
        mode='poke',
        poke_interval=120, # Check every 2 minutes
    )

    run_transformations = PythonOperator(
        task_id='run_sql_file_transformations',
        python_callable=run_sql_transformations_func,
    )

    [wait_for_quebec_data, wait_for_ontario_data] >> run_transformations
