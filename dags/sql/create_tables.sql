-- drop tales if exists
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_products;
DROP TABLE IF EXISTS dim_customers;
-- DROP TABLE IF EXISTS dim_date;

-- activate extension 
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- create
CREATE TABLE dim_customers (
  customer_id varchar PRIMARY KEY,
  customer_city varchar,
  customer_state varchar
);

CREATE TABLE dim_products (
  product_id varchar PRIMARY KEY,
  product_category_name varchar
);

-- CREATE TABLE dim_date (
--   date_id date PRIMARY KEY,
--   day int,
--   month int,
--   year int
-- );

CREATE TABLE fact_sales (
  fact_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  order_id varchar,
  customer_id VARCHAR REFERENCES dim_customers(customer_id),
  product_id VARCHAR REFERENCES dim_products(product_id),
  approved_at DATE,
  delay_delivery INT,
  payment_method VARCHAR,
  revenue numeric
);