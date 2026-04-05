import pandas as pd
import os

parquet_staging_path = "/opt/airflow/dags/data/staging"
parquet_warehouse_path = "/opt/airflow/dags/data/warehouse"

def transform():
    order_items = pd.read_parquet(f"{parquet_staging_path}/order_items.parquet")
    orders = pd.read_parquet(f"{parquet_staging_path}/orders.parquet")
    payments = pd.read_parquet(f"{parquet_staging_path}/payments.parquet")

    new_orders = order_items.merge(orders, on='order_id', how='inner') 
    dim_facts = new_orders.loc[:, ["order_id", "product_id", "customer_id"]]
    
    # delivery delay
    dim_facts["delay_delivery"] = orders["order_estimated_delivery_date"] - orders["order_delivered_timestamp"]
    dim_facts['delay_delivery'] = dim_facts["delay_delivery"].dt.days

    # revenue 
    dim_facts["revenue"] = new_orders["price"] + new_orders["shipping_charges"]

    # payment method
    new_orders = new_orders.merge(payments, on='order_id', how='inner')
    dim_facts["payment_method"] = new_orders["payment_type"]

    # date dim
    dim_date = new_orders.loc[:, ["order_approved_at"]]
    dim_facts["date_id"] = dim_date["order_approved_at"]

    dim_date = dim_date.rename(columns={"order_approved_at": "date_id"})
    dim_date["day"] = dim_date["date_id"].dt.day.fillna(-1).astype("int")
    dim_date["month"] = dim_date["date_id"].dt.month.fillna(-1).astype("int")
    dim_date["year"] = dim_date["date_id"].dt.year.fillna(-1).astype("int")

    # save results
    dim_facts.to_parquet(f"{parquet_warehouse_path}/orders.parquet")
    dim_date.to_parquet(f"{parquet_warehouse_path}/date.parquet")

    print(f"# end for transformation: orders shape {dim_facts.shape}.")



    