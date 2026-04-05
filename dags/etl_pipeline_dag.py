from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.extract_data import extract_data
from datetime import datetime

with DAG(
    dag_id="etl_pipeline_dag",
    start_date=datetime(2026, 4, 5),
    schedule=None, # for the moment the trigger is manual
    catchup=False,
) as dag:
    extract_data = PythonOperator(
        task_id="extract_data_from_raw",
        python_callable=extract_data
    )
