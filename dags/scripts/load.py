import pandas as pd

def load_customer(engine, parquet_warehouse_path):
    dim_customers = pd.read_parquet(f"{parquet_warehouse_path}/customers.parquet")
    dim_customers.to_sql('dim_customers',con=engine, if_exists='append', index=False)

    
def load_products(engine, parquet_warehouse_path):
    dim_products = pd.read_parquet(f"{parquet_warehouse_path}/products.parquet")
    dim_products.to_sql('dim_products',con=engine, if_exists='append', index=False)

def load_facts(engine, parquet_warehouse_path):
    fact_sales = pd.read_parquet(f"{parquet_warehouse_path}/orders.parquet")
    fact_sales.to_sql('fact_sales',con=engine, if_exists='append', index=False)
    