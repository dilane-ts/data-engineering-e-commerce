from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 2),
    'retries': 5
}

with DAG(
    dag_id='create_tables_dag',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:
    cmd = ""
    with open("/opt/airflow/dags/sql/create_tables.sql") as file:
        cmd = file.read()

    create_tables = SQLExecuteQueryOperator(
        task_id='create_tables',
        conn_id='dw_postgres',
        sql=cmd
    )