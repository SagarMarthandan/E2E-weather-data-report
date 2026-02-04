"""
Airflow DAG to orchestrate the weather data pipeline.
It first ingests data using a Python script and then runs dbt transformations in a Docker container.
"""
import sys
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount

# Add the directory containing the ingestion script to the Python path
sys.path.append('/opt/airflow/api-request')
# Import the main function from the ingestion script
from insert_records import main

default_args = {
    'description': 'Orchestrator DAG for Weather API data pipeline',
    'start_date': datetime(2026, 2, 3),
    'catchup': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG with a 1-minute schedule interval
dag = DAG(
    dag_id="weather-api-dbt-orchestrator",
    default_args=default_args,
    schedule=timedelta(minutes=1),
)

with dag:
    # Task 1: Ingest data from the Weather API into the PostgreSQL database
    task1 = PythonOperator(
        task_id='ingest_data_task_1',
        python_callable=main
    )

    # Task 2: Run dbt transformations using the dbt-postgres Docker image
    task2 = DockerOperator(
        task_id='transform_data_task',
        # Use the official dbt-postgres image
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command='run',
        working_dir='/usr/app/',
        mounts=[
            Mount(
                source='/home/sagar/repos/weather-data-project/dbt/my_project', 
                target='/usr/app/', 
                type='bind'),
            Mount(
                source='/home/sagar/repos/weather-data-project/dbt/profiles.yml', 
                target='/root/.dbt/profiles.yml', 
                type='bind'),
        ],
        # Connect to the same Docker network as the database
        network_mode='weather-data-project_my-network',
        # Use the host's Docker socket
        docker_url='unix://var/run/docker.sock',
        auto_remove='success',
    ) 

    # Set task dependency: Ingestion must complete before transformation starts
    task1 >> task2   