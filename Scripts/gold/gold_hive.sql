CREATE DATABASE gold;
-- drop DATABASE gold;
USE gold;
SHOW TABLES IN gold;

DROP TABLE dim_products;
DROP TABLE dim_customers;
DROP TABLE dim_reviews;
DROP TABLE dim_time;
DROP TABLE fact_sales;


-- FACT TABLE
CREATE EXTERNAL TABLE gold.fact_sales (
    customer_id string,
    product_id string,
    order_id string,
    order_status string,
    order_purchase_timestamp timestamp,
    order_approved_at timestamp,
    order_delivered_carrier_date timestamp,
    order_delivered_customer_date timestamp,
    order_estimated_delivery_date timestamp,
    delivery_time_days int,
    order_item_id int,
    seller_id string,
    shipping_limit_date timestamp,
    price double,
    freight_value double,
    product_category_name string,
    product_name_lenght int,
    product_description_lenght int,
    product_photos_qty int,
    product_weight_g int,
    product_length_cm int,
    product_height_cm int,
    product_width_cm int,
    customer_unique_id string,
    customer_zip_code_prefix int,
    customer_city string,
    customer_state string,
    total_price double
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/root/goldl/fact_sales/'




--DIM CUSTOMERS

CREATE EXTERNAL TABLE gold.dim_customers (
    customer_id string,
    customer_unique_id string,
    customer_city string,
    customer_state string,
    customer_zip_code_prefix int
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/root/goldl/dim_customers/';




-- DIM PRODUCTS

CREATE EXTERNAL TABLE gold.dim_products (
    product_id string,
    product_category_name string,
    product_weight_g int,
    product_length_cm int,
    product_height_cm int,
    product_width_cm int,
    product_photos_qty int
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/root/goldl/dim_products/';


-- DIM REVIEWS

CREATE EXTERNAL TABLE gold.dim_reviews (
    review_id string,
    order_id string,
    review_score int,
    is_positive boolean,
    review_creation_date timestamp
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/root/goldl/dim_reviews/';


-- DIM TIME

CREATE EXTERNAL TABLE gold.dim_time (
    order_id string,
    order_date date,
    year int,
    month int,
    day int
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/root/goldl/dim_time/';









