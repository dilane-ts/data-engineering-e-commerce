import pandas as pd

data_path = "/opt/airflow/dags/data/raw"
parquet_save_path = "/opt/airflow/dags/data/staging"

def extract_data():
    # load data from csv file
    customers = pd.read_csv(f"{data_path}/df_Customers.csv")
    order_items = pd.read_csv(f"{data_path}/df_OrderItems.csv")
    orders = pd.read_csv(f"{data_path}/df_Orders.csv")
    payments = pd.read_csv(f"{data_path}/df_Payments.csv")
    products = pd.read_csv(f"{data_path}/df_Products.csv")

    customers = customers.drop("customer_zip_code_prefix", axis=1)
    customers.to_parquet(f"{parquet_save_path}/customers.parquet")

    products = products.loc[:, ["product_id", "product_category_name"]]

    orders = orders.drop(["order_status", "order_purchase_timestamp"], axis=1)
    orders.to_parquet(f"{parquet_save_path}/orders.parquet")

    order_items = order_items.drop("seller_id", axis=1)
    order_items.to_parquet(f"{parquet_save_path}/order_items.parquet")

    payments = payments.loc[:, ["order_id", "payment_type"]]
    payments.to_parquet(f"{parquet_save_path}/payments.parquet")
    