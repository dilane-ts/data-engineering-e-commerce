from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from scripts.extract_data import extract_data
from scripts.transform import transform
from datetime import datetime

with DAG(
    dag_id="etl_pipeline_dag",
    start_date=datetime(2026, 4, 5),
    schedule=None, # for the moment the trigger is manual
    catchup=False,
) as dag:
    init_task = BashOperator(
        task_id="init_task",
        bash_command="rm -rf /opt/airflow/dags/data/{staging,warehouse} && mkdir -p /opt/airflow/dags/data/{staging,warehouse}"
    )
    extract_task = PythonOperator(
        task_id="extract_data_from_raw",
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id="transform_data_task",
        python_callable=transform
    )

    end_task = BashOperator(
        task_id='end_task',
        bash_command="echo 'Pipeline run successfully.'"
    )

    init_task >> extract_task >> transform_task >> end_task
