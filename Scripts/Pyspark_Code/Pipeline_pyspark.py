
# coding: utf-8

# In[1]:


spark


# In[2]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum, isnan, when, count,to_timestamp,datediff,sum as _sum,to_date, year, month, dayofmonth
spark = SparkSession.builder.getOrCreate()


# In[23]:


# df0 = spark.read.csv("hdfs://namenode:8020/user/root/ecommerce/customers/", header=True, inferSchema=True)
# df1 = spark.read.csv("hdfs://namenode:8020/user/root/ecommerce/orders/", header=True, inferSchema=True)
# df2 = spark.read.csv("hdfs://namenode:8020/user/root/ecommerce/order_items/", header=True, inferSchema=True)
# df3 = spark.read.csv("hdfs://namenode:8020/user/root/ecommerce/reviews/", header=True, inferSchema=True)
# df4 = spark.read.csv("hdfs://namenode:8020/user/root/ecommerce/products/", header=True, inferSchema=True)


# In[24]:


# df0.write.mode("overwrite").parquet("hdfs://namenode:8020/user/root/ecommerce_parquet/customers/")
# df1.write.mode("overwrite").parquet("hdfs://namenode:8020/user/root/ecommerce_parquet/orders/")
# df2.write.mode("overwrite").parquet("hdfs://namenode:8020/user/root/ecommerce_parquet/order_items/")
# df3.write.mode("overwrite").parquet("hdfs://namenode:8020/user/root/ecommerce_parquet/reviews/")
# df4.write.mode("overwrite").parquet("hdfs://namenode:8020/user/root/ecommerce_parquet/products/")


# In[2]:


df0 = spark.read.parquet("hdfs://namenode:8020/user/root/ecommerce_parquet/customers/")
df1 = spark.read.parquet("hdfs://namenode:8020/user/root/ecommerce_parquet/orders/")
df2 = spark.read.parquet("hdfs://namenode:8020/user/root/ecommerce_parquet/order_items/")
df3 = spark.read.parquet("hdfs://namenode:8020/user/root/ecommerce_parquet/reviews/")
df4 = spark.read.parquet("hdfs://namenode:8020/user/root/ecommerce_parquet/products/")


# In[3]:


def get_printSchema(df,name):
    print(f" in the {name} table ")
    df.printSchema()
    


# In[72]:


get_printSchema(df0,"customers")
get_printSchema(df1,"orders")
get_printSchema(df2,"order_items")
get_printSchema(df3,"reviews")
get_printSchema(df4, "products")


# # Fix reviews table

# In[83]:


df3= df3.withColumn("review_score",col("review_score").cast("int"))    .withColumn("review_creation_date", to_timestamp("review_creation_date"))     .withColumn("review_answer_timestamp", to_timestamp("review_answer_timestamp")) 


# In[98]:


df4.toPandas()


# In[24]:


def get_describe(df,name):
    print(f"Describe for {name} table") 
    df.describe().show(vertical=True)


# In[25]:


get_describe(df0, "customers")
get_describe(df1,"orders")
get_describe(df2,"order_items")
get_describe(df3, "reviews")
get_describe(df4, "products")


# ## Check Duplicates (just keys)

# In[7]:


customers_dup = df0.groupBy("customer_id").count().filter("count > 1")


# In[8]:


customers_dup.show()


# In[9]:


orders_dup = df1.groupBy("order_id")     .count()     .filter("count > 1")

orders_dup.show()


# In[10]:


order_items_dup = df2.groupBy("order_id", "order_item_id")     .count()     .filter("count > 1")

order_items_dup.show()


# In[11]:


reviews_dup = df3.groupBy("review_id")     .count()     .filter("count > 1")

reviews_dup.show()


# In[12]:


products_dup = df4.groupBy("product_id")     .count()     .filter("count > 1")

products_dup.show()


# ## Get ALL duplicates summary

# In[13]:


def find_duplicates(df,keys,name):
    
    print(f"Checking duplicates for {name}")
    dub=df.groupBy(keys)         .count()         .filter("count > 1")
    dub.show()


# In[14]:


find_duplicates(df0, ["customer_id"], "customers")
find_duplicates(df1, ["order_id"], "orders")
find_duplicates(df2, ["order_id", "order_item_id"], "order_items")
find_duplicates(df3, ["review_id"], "reviews")
find_duplicates(df4, ["product_id"], "products")


# ## From the prevouse table reviews conatin dublicate

# ## check_dublicate_rows_(not just keys)

# In[32]:


df3.filter(
    df3.review_id.isin(
        df3.groupBy("review_id")
        .count()
        .filter("count > 1")
        .select("review_id")
        .rdd.flatMap(lambda x: x)
        .collect()
    )
).show(3)


# # Remove duplicates

# In[33]:


df3_clean = df3.dropDuplicates(["review_id"])


# In[34]:


df3_clean.filter(
    df3_clean.review_id.isin(
        df3_clean.groupBy("review_id")
        .count()
        .filter("count > 1")
        .select("review_id")
        .rdd.flatMap(lambda x: x)
        .collect()
    )
).show(3)


# ## Check NULL (missing values per column)

# In[55]:


def check_null(df,name):
    print(f"Null in the {name} table ")
    df.select([
        sum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]).show(vertical=True)


# In[56]:


check_null(df0, "customers")
check_null(df1, "orders")
check_null(df2,"order_items")
check_null(df3,"reviews")
check_null(df4,"products")


# ## Check BOTH NULL + NaN

# In[63]:


df0.select([
    count(when(col(c).isNull() | isnan(col(c)), c)).alias(c)
    for c in df0.columns
]).show()


# In[79]:


from pyspark.sql.functions import col, isnan, when, count
from pyspark.sql.types import DoubleType, FloatType

def get_null_nan_counter(df, name):
    print(f"Null/NaN in the {name} table")

    exprs = []
    for field in df.schema.fields:
        col_name = field.name
        col_type = field.dataType

        # Check if numeric
        if isinstance(col_type, (DoubleType, FloatType)):
            expr = count(when(col(col_name).isNull() | isnan(col(col_name)), col_name)).alias(col_name)
        else:
            expr = count(when(col(col_name).isNull(), col_name)).alias(col_name)

        exprs.append(expr)

    df.select(exprs).show(vertical=True)


# In[80]:


get_null_nan_counter(df0, "customers")
get_null_nan_counter(df1, "orders")
get_null_nan_counter(df2,"order_items")
get_null_nan_counter(df3,"reviews")
get_null_nan_counter(df4,"products")


# In[91]:


df2.filter(col("price") < 0).count()


# # in orders tabls we keep null becouse they repersent real world stated
# 

# In[94]:


df3 = df3.dropna(subset=["review_id", "order_id", "review_score"])  ##Drop rows here  :primary key missing,foreign key missing,critical measure missing


# In[95]:


df3= df3.fillna({
"review_comment_title": "no title",
    "review_comment_message": "no comment"
})


# In[96]:


get_null_nan_counter(df3,"reviews")


# ### we not address null in review_creation_date,review_answer_timestamp becouse **preserves business reality** event_based

# In[97]:


df4 = df4.fillna({
    "product_category_name": "unknown"
})


# In[99]:


df4 = df4.fillna({
    "product_name_lenght": 0,
    "product_description_lenght": 0,
    "product_photos_qty": 0,
    "product_weight_g": 0,
    "product_length_cm": 0,
    "product_height_cm": 0,
    "product_width_cm": 0
})


# In[104]:


get_null_nan_counter(df0, "customers")
get_null_nan_counter(df1, "orders")
get_null_nan_counter(df2,"order_items")
get_null_nan_counter(df3,"reviews")
get_null_nan_counter(df4,"products")


# In[108]:


df0.write.mode("overwrite").parquet("hdfs://namenode:8020/user/root/silver/customers/")
df1.write.mode("overwrite").parquet("hdfs://namenode:8020/user/root/silver/orders/")
df2.write.mode("overwrite").parquet("hdfs://namenode:8020/user/root/silver/order_items/")
df3.write.mode("overwrite").parquet("hdfs://namenode:8020/user/root/silver/reviews/")
df4.write.mode("overwrite").parquet("hdfs://namenode:8020/user/root/silver/products/")


# ## go to see silver_schema_hive_table file 

# In[ ]:


# you can also create using spark but before enable hive like this when build session  ***.enableHiveSupport()* \
## Spark will: Create schema in Hive Metastore 2-Link table to HDFS path- NOT move or create data
spark.sql("""
create external table silver.silver_customers (  
    product_id STRING,
    product_name STRING,
    price DOUBLE
)
STORED AS PARQUET
LOCATION "hdfs://namenode:8020/user/root/silver/customers/"

""") 


# In[109]:


# spark.sql("""
# CREATE TABLE products_hive (
#     product_id STRING,
#     product_name STRING,
#     price DOUBLE
# )
# STORED AS PARQUET
# """)


# # and after that you can write 
# ## if i write
# # df.write.mode("overwrite").saveAsTable("users")  store in users/hive /warhouse/users


# saveAsTable()
# Spark decides storage location   , Write data files, Create Hive metadata,Manage storage path
# Usually Hive warehouse directory
# Fully managed by Spark + Hive


# df = spark.sql("SELECT * FROM users")   ## this go to to autmaticaly
# df.show()


# ## Bronze → Silver (DONE) << Hive_able

# In[106]:


df0.show(5)


# # → Gold (NEXT) → Analytics / BI

# In[110]:


## join tables
## create KPIs
## build fact/dimension tables
## prepare data for dashboards (Power BI / Tableau)


# In[4]:


customers = spark.read.parquet("hdfs://namenode:8020/user/root/silver/customers/")
orders = spark.read.parquet("hdfs://namenode:8020/user/root/silver/orders/")
order_items = spark.read.parquet("hdfs://namenode:8020/user/root/silver/order_items/")
reviews = spark.read.parquet("hdfs://namenode:8020/user/root/silver/reviews/")
products = spark.read.parquet("hdfs://namenode:8020/user/root/silver/products/")


# # Fact Orders Table

# In[5]:


orders=orders.withColumn("delivery_time_days",datediff("order_delivered_customer_date", "order_purchase_timestamp")
                          )


# # Review analysis

# In[6]:


reviews= reviews.withColumn("is_positive",reviews.review_score>=4)


# In[7]:


orders.toPandas()


# In[8]:


orders.toPandas()


# In[9]:


order_items.toPandas()


# In[10]:


get_printSchema(customers,"customers")
get_printSchema(orders,"orders")
get_printSchema(order_items,"order_items")
get_printSchema(reviews,"reviews")
get_printSchema(products, "products")


# In[11]:


orders.join(
    order_items,
    "order_id",
    "left_anti"
).count()


# ## We will create 3 main Gold tables:
# ### -1 FACT SALES TABLE  (Grain 1row = 1 order item)

# In[12]:


# Start from orders (business event)
# Order is the “main event”
# Cleaner business logic
#  One row per order (or order context)
fact_sales = orders     .join(order_items, "order_id", "left")     .join(products, "product_id", "left")     .join(customers, "customer_id", "left")

# you want a sales fact table at order level
# business KPIs (revenue per order, customer behavior)



# ## Delivery KPI

# In[13]:


# delivery_delay_days
fact_sales=fact_sales.withColumn("delivery_time_days",datediff("order_delivered_customer_date", "order_purchase_timestamp")
                          )


# In[14]:


fact_sales =fact_sales.withColumn(
    "total_price",
    col("price") + col("freight_value")
)


# In[15]:


fact_sales.toPandas()


# In[16]:


fact_sales.printSchema()


# # DIM CUSTOMER

# In[17]:


dim_customers = customers.select(
    "customer_id",
    "customer_unique_id",
    "customer_city",
    "customer_state",
    "customer_zip_code_prefix"
)


# # DIM PRODUCT

# In[18]:


dim_products = products.select(
    "product_id",
    "product_category_name",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
    "product_photos_qty"
)                              ##.dropDuplicates(["product_id"])


# ## DIM REVIEW

# In[19]:


dim_reviews = reviews.select(
    "review_id",
    "order_id",
    "review_score",
    "is_positive",
    "review_creation_date"
)


# # DIM TIME 

# In[20]:


dim_time = orders.select(
    "order_id",
    to_date("order_purchase_timestamp").alias("order_date"),
    year("order_purchase_timestamp").alias("year"),
    month("order_purchase_timestamp").alias("month"),
    dayofmonth("order_purchase_timestamp").alias("day")
)


#  #               dim_customers
#                                   |
# # dim_products —  fact_sales  — dim_reviews
#                                   |
#  #                              dim_time

# # ********** STORE IN GOLD LAYER ************

# In[21]:


fact_sales.write.mode("overwrite").parquet("hdfs://namenode:8020/user/root/goldl/fact_sales/")
dim_customers.write.mode("overwrite").parquet("hdfs://namenode:8020/user/root/goldl/dim_customers/")
dim_products.write.mode("overwrite").parquet("hdfs://namenode:8020/user/root/goldl/dim_products/")
dim_reviews.write.mode("overwrite").parquet("hdfs://namenode:8020/user/root/goldl/dim_reviews/")
dim_time.write.mode("overwrite").parquet("hdfs://namenode:8020/user/root/goldl/dim_time/")


# ### in hive we created our structure
# 
# #### /user/root/goldl/
# ##    ├── fact_sales/
# ##    ├── dim_customers/
# ##    ├── dim_products/
# ##    ├── dim_reviews/
# ##    └── dim_time/

# ## Agg

# ## 2. CUSTOMER ANALYTICS TABLE
# ### Purpose:
# - customer behavior
# - segmentation
# -  activity

# In[ ]:


##1= Types of Gold layer * * final business outputs
# -- Star schema Gold (traditional BI)  (blueprint of warehouse)
# --  fact tables + dimension tables
# ** used in warehouses (Snowflake, BigQuery)

#2 = Aggregated Gold tables
# Used for dashboards
# -3 = Flat tables (denormalized)
# fact_sales (everything joined together)

#  Fast for ML
#  Simple queries


# In[22]:


# customer_gold = orders \
#     .join(customers, "customer_id") \
#     .groupBy("customer_id", "customer_city", "customer_state") \
#     .agg(
#         _sum("delivery_time_days").alias("total_delivery_days")
#     )


# # REVIEW ANALYTICS TABLE
# #### Purpose:
# 
# --- satisfaction analysis
# 
# --- sentiment proxy

# In[24]:


# review_gold = reviews.groupby("order_id", "is_positive").agg(_sum("review_score").alias("total_score"))


# # 4. PRODUCT PERFORMANCE TABLE
# ### Purpose:
# -- best products
# 
# -- category analysis

# In[25]:


# product_gold = order_items \
#     .join(products, "product_id") \
#     .groupBy("product_id", "product_category_name") \
#     .agg(
#         _sum("price").alias("total_revenue"),
#         _sum("freight_value").alias("total_freight")
#     )


# In[ ]:


## so now gold layer have Gold Layer:
# fact_sales        >>  main table
# customer_gold    >> behavior
# review_gold       >>  satisfaction
# product_gold     >>  performance


# In[18]:


# ## you can use also the next but this  More granular(each product per order)
# fact_order_items  = order_items \
#     .join(orders, "order_id", "left") \
#     .join(customers, "customer_id", "left") \
#     .join(products, "product_id", "left")
# # you need line-level analytics
# # product-level revenue per item
# # very detailed reporting


# In[17]:


# fact_sales.toPandas()

