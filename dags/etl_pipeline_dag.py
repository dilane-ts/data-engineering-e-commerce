from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from scripts.extract_data import extract_data
from scripts.transform import transform_data
from scripts.load import load_customer, load_facts,load_products
from datetime import datetime



with DAG(
    dag_id="etl_pipeline_dag",
    start_date=datetime(2026, 4, 5),
    schedule=None, # for the moment the trigger is manual
    catchup=False,
) as dag:
    
    hook = PostgresHook(postgres_conn_id="dw_postgres")
    engine = hook.get_sqlalchemy_engine()
    parquet_warehouse_path = "/opt/airflow/dags/data/warehouse"

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
        python_callable=transform_data
    )

    load_customer_task = PythonOperator(
        task_id="load_customer_task",
        python_callable=load_customer,
        op_kwargs={'engine': engine, 'parquet_warehouse_path': parquet_warehouse_path}
    )

    load_products_task = PythonOperator(
        task_id="load_product_task",
        python_callable=load_products,
        op_kwargs={'engine': engine, 'parquet_warehouse_path': parquet_warehouse_path}
    )

    load_fact_sales_task = PythonOperator(
        task_id="load_fact_sales_task",
        python_callable=load_facts,
        op_kwargs={'engine': engine, 'parquet_warehouse_path': parquet_warehouse_path}
    )

    end_task = BashOperator(
        task_id='end_task',
        bash_command="echo 'Pipeline run successfully.'"
    )

    init_task >> extract_task >> transform_task 
    transform_task >> load_customer_task
    transform_task >> load_products_task
    load_products_task >> load_fact_sales_task
    load_customer_task >> load_fact_sales_task
    load_fact_sales_task >> end_task
