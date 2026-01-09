"""
Simple ETL DAG
Orchestrates API ingestion and data transformation
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


# Default arguments for DAG
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="simple_etl_pipeline",
    default_args=default_args,
    description="Simple ETL pipeline using Airflow",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "api", "data-engineering"],
) as dag:

    ingest_task = BashOperator(
        task_id="ingest_api_data",
        bash_command="python src/ingest_api_data.py",
    )

    transform_task = BashOperator(
        task_id="transform_raw_data",
        bash_command="python src/transform_raw_data.py",
    )

    ingest_task >> transform_task
