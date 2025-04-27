#!/usr/bin/env bash
## Airflow set up ##
cd airflow
mkdir -p config logs plugins
docker compose build
docker compose up airflow-init
docker compose up -d
docker compose exec airflow-webserver airflow connections add 'my_postgres_conn' \
    --conn-uri 'postgresql://postgres:postgres@dwh_postgres/postgres'
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow dags unpause create_schema
docker compose exec airflow-webserver airflow dags trigger create_schema
sleep 10
docker compose exec airflow-webserver airflow dags unpause load_json_data