#!/usr/bin/env bash
## Dbt set up ##
cd postgres
mkdir -p analyses logs seeds snapshots tests
dbt run --select bronze
dbt run --select silver