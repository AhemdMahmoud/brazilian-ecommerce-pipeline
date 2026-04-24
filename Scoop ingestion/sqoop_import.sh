#!/bin/bash

# ==========================================
# Sqoop Import Script
# Import Olist Tables from MariaDB to HDFS
# ==========================================

DB_URL="jdbc:mysql://localhost:3306/ecommerce"
USER="student"
PASS="student"

# ==========================================
# 1. CUSTOMERS
# ==========================================
sqoop import \
--connect $DB_URL \
--username $USER \
--password $PASS \
--table customers \
--target-dir /staging_zone/Customers/ \
--as-parquetfile \
--num-mappers 1

# ==========================================
# 2. PRODUCTS
# ==========================================
sqoop import \
--connect $DB_URL \
--username $USER \
--password $PASS \
--table products \
--target-dir /staging_zone/Products/ \
--as-parquetfile \
--num-mappers 1

# ==========================================
# 3. ORDERS
# ==========================================
sqoop import \
--connect $DB_URL \
--username $USER \
--password $PASS \
--table orders \
--target-dir /staging_zone/Orders/ \
--as-parquetfile \
--num-mappers 1

# ==========================================
# 4. ORDER ITEMS
# ==========================================
sqoop import \
--connect $DB_URL \
--username $USER \
--password $PASS \
--table order_items \
--target-dir /staging_zone/Order_Items/ \
--as-parquetfile \
--num-mappers 1

# ==========================================
# 5. ORDER REVIEWS
# ==========================================
sqoop import \
--connect $DB_URL \
--username $USER \
--password $PASS \
--table order_reviews \
--target-dir /staging_zone/Order_Reviews/ \
--as-parquetfile \
--num-mappers 1

echo "All tables imported successfully."
