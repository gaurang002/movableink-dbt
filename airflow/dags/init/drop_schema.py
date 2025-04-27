from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

def print_airflow_home():
    airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')  # default if not set
    print(f"AIRFLOW_HOME directory is: {airflow_home}")

default_args = {
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='drop_schema',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    template_searchpath=['/opt/airflow/dags'],
) as dag:

    drop_schema = PostgresOperator(
        task_id='drop_schema',
        postgres_conn_id='my_postgres_conn',
        sql="sql/drop_schema.sql",
    )

    drop_bronze_tables = PostgresOperator(
        task_id='drop_bronze_tables',
        postgres_conn_id='my_postgres_conn',
        sql="sql/drop_bronze_tables.sql",
    )

#print_home
drop_bronze_tables >> drop_schema