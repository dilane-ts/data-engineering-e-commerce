CREATE TABLE fact_sales (
  fact_id int PRIMARY KEY,
  order_id varchar,
  customer_id VARCHAR REFERENCES dim_customer(customer_id),
  product_id VARCHAR REFERENCES dim_product(product_id),
  date_id DATE REFERENCES dim_date(date_id),
  order_delivered_timestamp timestamp,
  revenue numeric,
);

CREATE TABLE dim_customer (
  customer_id varchar PRIMARY KEY,
  customer_city varchar,
  customer_state varchar
);

CREATE TABLE dim_product (
  product_id varchar PRIMARY KEY,
  product_category_name varchar
);

CREATE TABLE dim_date (
  date_id date PRIMARY KEY,
  day int,
  month int,
  year int
);