CREATE DATABASE silver;




CREATE EXTERNAL TABLE silver.silver_customers (
    customer_id string,
    customer_unique_id string,
    customer_zip_code_prefix int,
    customer_city string,
    customer_state string
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/root/silver/customers/';






CREATE EXTERNAL TABLE silver.silver_orders (
    order_id string,
    customer_id string,
    order_status string,
    order_purchase_timestamp timestamp,
    order_approved_at timestamp,
    order_delivered_carrier_date timestamp,
    order_delivered_customer_date timestamp,
    order_estimated_delivery_date timestamp
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/root/silver/orders/';




CREATE EXTERNAL TABLE silver.silver_order_items (
    order_id string,
    order_item_id int,
    product_id string,
    seller_id string,
    shipping_limit_date timestamp,
    price double,
    freight_value double
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/root/silver/order_items/';






CREATE EXTERNAL TABLE silver.silver_reviews (
    review_id string,
    order_id string,
    review_score int,
    review_comment_title string,
    review_comment_message string,
    review_creation_date timestamp,
    review_answer_timestamp timestamp
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/root/silver/reviews/';






CREATE EXTERNAL TABLE silver.silver_products (
    product_id string,
    product_category_name string,
    product_name_lenght int,
    product_description_lenght int,
    product_photos_qty int,
    product_weight_g int,
    product_length_cm int,
    product_height_cm int,
    product_width_cm int
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/user/root/silver/products/';


