# dags/common/db_utils.py
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import os
from airflow.hooks.postgres_hook import PostgresHook

# Remove old environment variable and engine code
# DB_USER = os.getenv("POSTGRES_USER", "airflow")
# DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
# DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
# DB_PORT = os.getenv("POSTGRES_PORT", "5432")
# DB_NAME = os.getenv("POSTGRES_DB", "airflow")
# DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
# engine = create_engine(DATABASE_URL)

def get_db_connection(conn_id="postgres_default"):
    """
    Returns a SQLAlchemy connection using Airflow's PostgresHook.
    """
    hook = PostgresHook(postgres_conn_id=conn_id)
    return hook.get_sqlalchemy_engine().connect()

def create_schema_if_not_exists(schema_name: str, connection):
    """Creates a PostgreSQL schema if it doesn't exist using the given connection."""
    sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    connection.execute(text(sql))
    # Commit is needed if connection is not in a transaction block already for schema creation
    # However, typically called within a conn.begin() block elsewhere.
    # For safety, if used standalone: if not connection.in_transaction(): connection.commit()


def df_to_sql(df: pd.DataFrame, table_name: str, connection, if_exists: str = 'replace', index: bool = False, schema: str = None):
    """Writes a DataFrame to a SQL table using the given connection."""
    df.to_sql(table_name, connection, if_exists=if_exists, index=index, schema=schema, method='multi')

def execute_sql_statement(sql_query: str, connection):
    """Executes a single SQL statement using the given connection."""
    connection.execute(text(sql_query))
