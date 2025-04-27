#!/usr/bin/env bash
cd airflow
docker compose down --volumes --rmi all --remove-orphans