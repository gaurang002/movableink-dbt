# movableink-dbt
## Table of Contents

- [Problem Statement](#problem_statement)
- [Architecture](#architecture)
  - [Notes](#notes)
- [Setup Local Env](#setup_local_env)
- [Technologies](#technologies)
- [Trade-offs](#trade_offs)
- [Insights](#insights)
- [Scalability](#scalability)
- [References](#references)

## Problem Statement
As an interviewee, one need to present end to end data solution using given sample data to gain business insights.

## Architecture
<p align="center">
  <img src="images/architecture.png" width="400" alt="Architecture Diagram">
</p>

## Notes
- Just for the scope of this solution to run locally, tools and technologies used are represented by red color texxt.
- Other colored components are open for discussion during the review meeting.

## Setup Local Env
- Assumption is that env is clean i.e. no prior postgres or airflow docker images/container exists.
- Clone this repository.
- Follow below steps to copy your sample_data.json at required location
  - `mkdir -p airflow/dags/data`
  - `cp <path/to/your/sample_data.json> airflow/dags/data`
- Set up python3 virtual environment
  - `./setup_python_venv.sh`
- Set up Airflow
  - `./setup_airflow.sh`
  - Webserver UI - http://localhost:8080/
  - Username/Password - airflow/airflow
  - PGAdmin
    - UI - http://localhost:5050/
    - Username/Password - admin@example.com/admin123
    - DWH Conn - postgresql://postgres:postgres@localhost:5434/postgres
- Set up Dbt (Note: postgres folder is dbt project. Name can be better and left for future improvement)
  - `./setup_dbt.sh`
- [ONLY IF NEEDED] To destroy airflow - `./cleanup_airflow.sh`
- After successful set up. Ond can verify by following:
- ```
  % docker ps
  CONTAINER ID   IMAGE                       COMMAND                  CREATED          STATUS          PORTS                              NAMES
  722a5280bb72   dpage/pgadmin4              "/entrypoint.sh"         20 minutes ago   Up 20 minutes   443/tcp, 0.0.0.0:5050->80/tcp      pgadmin
  6a88cbf51500   airflow-airflow-flower      "/usr/bin/dumb-init …"   20 minutes ago   Up 20 minutes   0.0.0.0:5555->5555/tcp, 8080/tcp   airflow-airflow-flower-1
  dbfbb1fbc863   airflow-airflow-worker      "/usr/bin/dumb-init …"   20 minutes ago   Up 20 minutes   8080/tcp                           airflow-airflow-worker-1
  ae213f58e1ec   airflow-airflow-scheduler   "/usr/bin/dumb-init …"   20 minutes ago   Up 20 minutes   8080/tcp                           airflow-airflow-scheduler-1
  27c1dd043a83   airflow-airflow-webserver   "/usr/bin/dumb-init …"   20 minutes ago   Up 20 minutes   0.0.0.0:8080->8080/tcp             airflow-airflow-webserver-1
  97722eb9eec4   postgres:15                 "docker-entrypoint.s…"   20 minutes ago   Up 20 minutes   0.0.0.0:5434->5432/tcp             dwh_postgres_db
  27b629d6a81f   postgres:13                 "docker-entrypoint.s…"   20 minutes ago   Up 20 minutes   5432/tcp                           airflow-postgres-1
  7945a397094c   redis:latest                "docker-entrypoint.s…"   20 minutes ago   Up 20 minutes   6379/tcp                           airflow-redis-1
  %
  ```
- Airflow UI with two dags create_schema and load_json_data are active and successfully completed
- <p align="center">
  <img src="images/airflow.png" width="400" alt="Architecture Diagram">
</p>


## Technologies

| Component      | Technologies                                | Comment                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|----------------|---------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Extract + Load | - Docker<br/>- Airlfow 2.9.0<br/>- Postgres | - Reasonable tools to build and show working end to end data solution<br/>- Docker is easy to set up and allows to mimic the prod like env and generally goto choice for dev/poc type of work load.<br/>- Airflow is robust orchestration tool which can be used for running different types of workflows (sequential + parallel). It also efficiently handles workflow dependencies.<br/>- Postgres features are closer to big data storage technologies like Redshift, SNowflake etc. Postgres has wide range of feature which comes handy for advance analytics. |
| Transform      | - Dbt                                       | - Mordern and widey used tool for SQL and Python based data transformations.<br/>- Fits well with CICD architecture.<br/>- Dbt core can be installed and use locally without any cost.<br/>- Can be one transformation tool for both data engineers, data analysts and data scientist.                                                                                                                                                                                                                                                                              |
| Others         | - Github                                    | - For efficient tracking and sharing solution.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |


## Trade-offs
- Extract & Load
  - No file splitting as sample file size provided was in ideal range.
  - Loading one file at time i.e. sequential loading vs. parallel loading.
  - Wanted to use COPY command to load but somehow set up was not allowing to do so. Did not debug more on this.
  - Had to include dwh_postgres and pgadmin services in same airflow docker-compose file for smoother communication among docker containers. 
- Transformation
  - No date and time dimension creation.
  - No Dimension model created due to low volume of sample data and lack of information about metadata for campaign, company, user etc. Also, wanted to keep the solution simple for interview purpose.
  - Potential Dimensional Model
  - <p align="center">
  <img src="images/star_schema.png" width="400" alt="Architecture Diagram">
</p>
- Misc
  - Single node airflow and postgres deployment.
  - No distributed reads/writes from/to database.
  - No Dbt integration with Airflow.

## Insights
- Created one view in silver schema for each insight asked.
- Some of the column names shows on the sample output of exercise not making sense. But keeping the output as it is shown in exercise.
- Usage Reporting
  - query -> `select * from silver.usage_reporting;`
- Most Valuable Time of Day
  - query -> `select * from silver.most_valuable_time_of_day;`
- Request duration by Device Type
  - query -> `select * from silver.request_duration_by_device_type;`
- Time to Conversion
  - query -> `select * from silver.time_to_conversion;`

## Scalability
- Solution would scale for large file systems. Placeholer logic for incremental load is kept based on last datetime approach.
- File systems can be replaced with cloud based storage system like S3, Cloud Storage with minimal code change on Airflow dag side.
- Postgres can be replaced with Redshift with minimal config change on Airflow and Dbt side.
- Bronze zone would work seamlessly with any json schema change from source side.
- Airflow can also be replaced with other E+L tool like Fivetran without affecting downstream Dbt transformation.

## References
- [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml)
- [Airflow docker-compose.yaml](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml)