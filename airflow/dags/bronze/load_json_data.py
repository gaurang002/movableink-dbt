from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from datetime import timedelta
from psycopg2.extras import execute_values, execute_batch
import json
import os

def safe_stream_ndjson(path):
    with open(path, 'r') as f:
        for idx, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON at line {idx}: {e}")

def get_all_files_info(directory):
    files_metadata = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            stat_info = os.stat(file_path)
            files_metadata.append({
                'file_name': file,
                'full_path': file_path,
                'size_bytes': stat_info.st_size,
                'created_at': datetime.fromtimestamp(stat_info.st_ctime),
                'modified_at': datetime.fromtimestamp(stat_info.st_mtime),
            })
    return files_metadata

def load_json_bulk_to_postgres(file_dir):
    files_info = get_all_files_info(file_dir) # Other information will be used for designing Incremental Logic.
    files = [i.get('full_path') for i in files_info]
    #path = '/opt/airflow/dags/data/sample_data.json'
    for file in files:
        print(file)
        records = []
        with open(file, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    record = json.loads(line)
                    records.append((record['request_uuid'],
                                    record['object_type'],
                                    record['msecs'],
                                    json.dumps(record),))  # 1-element tuple

        hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Now batch insert
        execute_batch(
            cursor,
            "INSERT INTO bronze.campaign (request_uuid, object_type, msecs, payload) "
            "VALUES (%s, %s, %s, %s)"
            "ON CONFLICT (request_uuid, object_type, msecs) DO NOTHING;",
            records,
            page_size=1000  # How many rows to send at once (tune based on your system)
        )

        conn.commit()
        cursor.close()
        conn.close()


default_args = {
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='load_json_data',
    default_args=default_args,
    schedule_interval="0 3 * * *",
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=2),
    catchup=False,
    template_searchpath=['/opt/airflow/dags'],
) as dag:
    '''
    truncate_staging_tables = PostgresOperator(
        task_id='truncate_staging_tables',
        postgres_conn_id='my_postgres_conn',
        sql="truncate table bronze.campaign_stg;",
    )
    '''
    '''
    load_staging_tables = PostgresOperator(
        task_id='load_staging_tables',
        postgres_conn_id='my_postgres_conn',
        sql="sql/bronze/load_json_data.sql",
        params={"file_path": "/opt/airflow/data/sample_data.json"},
    )
    '''
    load_bronze_tables = PythonOperator(
        task_id='load_bronze_tables',
        python_callable=load_json_bulk_to_postgres,
        op_args=["/opt/airflow/dags/data/"],
    )

    '''
    TODO: Dbt integration
    '''


load_bronze_tables