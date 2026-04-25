# 🛒 E-Commerce Data Warehouse Pipeline
### End-to-End Big Data Project — ITI 9-Month Professional Diploma | Big Data Track

<div align="center">

![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-66CCFF?style=for-the-badge&logo=apachehadoop&logoColor=black)
![Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Hive](https://img.shields.io/badge/Apache%20Hive-FDEE21?style=for-the-badge&logo=apachehive&logoColor=black)
![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![NiFi](https://img.shields.io/badge/Apache%20NiFi-728E9B?style=for-the-badge&logo=apache&logoColor=white)

</div>

---

## 📌 Project Overview

This project implements a **production-grade, end-to-end Big Data pipeline** built on the **Medallion Architecture (Bronze → Silver → Gold)** using a real-world Brazilian E-Commerce dataset (Olist). Raw transactional data is ingested from a MySQL relational database, transferred to HDFS, cleaned and transformed using Apache Spark (PySpark), and served through Apache Hive external tables — making it analytics-ready for BI tools like Power BI and Tableau.

> **Dataset:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — 100,000+ orders, 5 tables, 2016–2018

---

## 🏗️ Architecture

### Medallion Architecture Overview

```
┌──────────────┐     ┌───────────────┐     ┌───────────────┐     ┌───────────────┐
│    MySQL     │────▶│    BRONZE     │────▶│    SILVER     │────▶│     GOLD      │
│  Source DB   │     │  Raw / HDFS   │     │  Clean/Parq.  │     │  Star Schema  │
│  (Docker)    │     │  (CSV/Parq.)  │     │  (PySpark)    │     │  (Hive+Parq.) │
└──────────────┘     └───────────────┘     └───────────────┘     └───────────────┘
        │                                                                 │
        │                                                                 ▼
        │                                                        ┌───────────────┐
        │                                                        │  BI / Analytics│
        │                                                        │  Power BI /   │
        │                                                        │  Tableau      │
        │                                                        └───────────────┘
        │
   Two Ingestion Paths:
   ├── 🔴 Apache Sqoop  (JDBC bulk transfer)
   └── 🟢 Apache NiFi   (Flow-based, no-code ETL)
```

### 📸 Architecture Diagram

> ⬇️ **Place your architecture diagram image here**

<!-- 
    TO ADD YOUR DIAGRAM:
    1. Put your image file in the /diagrams/ folder (e.g., diagrams/architecture.png)
    2. Replace the line below with:
       ![Architecture Diagram](diagrams/architecture.png)
-->
![Pipeline architecture](https://github.com/AhemdMahmoud/brazilian-ecommerce-pipeline/blob/main/images/diagram1.png)

![Pipeline Animation](https://raw.githubusercontent.com/AhemdMahmoud/brazilian-ecommerce-pipeline/feature/mysql-to-hdfs/pipeline_animation%20(2).gif)
```
![Pipeline Animation](https://raw.githubusercontent.com/AhemdMahmoud/brazilian-ecommerce-pipeline/feature/mysql-to-hdfs/pipeline_animation%20(2).gif)
```

---

## 🗂️ Project Structure

```
ecommerce-data-warehouse/
│
├── 📁 data/                          # Raw CSV dataset files
│   ├── customers.csv
│   ├── orders.csv
│   ├── order_items.csv
│   ├── reviews.csv
│   └── products.csv
│
├── 📁 sql/                           # All SQL scripts
│   ├── DDL_ecommerce.sql             # MySQL source schema creation
│   ├── data_loaded.sql               # Load CSV data into MySQL
│   ├── silver_hive.sql               # Hive external tables — Silver layer
│   ├── gold_hive.sql                 # Hive external tables — Gold layer
│   ├── hvie__silver_test.sql         # Silver layer validation queries
│   ├── gold_test.sql                 # Gold layer validation queries
│   └── sqoop_mysql_cmd_hdfs.sql      # Apache Sqoop import commands
│
├── 📁 spark/                         # PySpark transformation scripts
│   ├── Pipeline_pyspark.py           # Full Bronze → Silver → Gold pipeline
│   └── Pipeline_pyspark.ipynb        # Jupyter Notebook version (interactive)
│
├── 📁 ingestion/                     # Data ingestion documentation
│   ├── sqoop_mysql_cmd_hdfs.sql      # Sqoop: MySQL → HDFS commands
│   ├── move_data_mysql_HDFS_without_sqoop.txt   # Manual HDFS put method
│   ├── put_data__inside_mysql_docker.txt         # Load CSVs into MySQL Docker
│   └── nifi/                         # Apache NiFi flow files
│       └── ecommerce_ingest_flow.xml # NiFi flow definition (export)
│
├── 📁 diagrams/                      # Architecture and schema diagrams
│   ├── architecture.png              # ← PUT YOUR ARCHITECTURE DIAGRAM HERE
│   └── star_schema.png               # Gold layer star schema ERD
│
├── 📄 docker-compose.yml             # Full stack Docker Compose file
└── 📄 README.md                      # This file
```

---

## 🛠️ Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Source DB** | MySQL 8.0 (Docker) | Relational transactional database |
| **Ingestion — Option A** | Apache Sqoop | JDBC bulk transfer: MySQL → HDFS |
| **Ingestion — Option B** | Apache NiFi | Visual flow-based ETL, no-code pipeline |
| **Storage** | Apache HDFS | Distributed file system (Data Lake) |
| **Processing** | Apache Spark / PySpark | Distributed data transformation & cleaning |
| **Query Engine** | Apache Hive | SQL interface & schema metastore |
| **File Format** | Apache Parquet | Columnar storage for Silver & Gold layers |
| **Orchestration** | Docker + Docker Compose | Container management for all services |
| **Schema Viewer** | DBeaver / DB Visualizer | Database schema exploration & SQL execution |
| **Language** | Python 3.x + HiveQL + SQL | Pipeline logic, DDL, and validation |
| **Notebook** | Jupyter (PySpark Kernel) | Interactive development & exploration |

---

## 🔄 Ingestion Methods — Sqoop vs NiFi

Two different technologies were used to move data from MySQL to HDFS, demonstrating flexibility in real-world data engineering scenarios.

### 🔴 Method 1 — Apache Sqoop (JDBC Bulk Transfer)

Sqoop is the traditional, production-grade tool for RDBMS → Hadoop bulk ingestion via JDBC.
![Sqoop](https://github.com/AhemdMahmoud/brazilian-ecommerce-pipeline/blob/main/images/Sqoop.png)
```bash
# Create HDFS target directories
for t in customers orders order_items reviews products
do
  hdfs dfs -mkdir -p /user/root/ecommerce/$t
done

# Import customers table
sqoop import \
  --connect jdbc:mysql://localhost:3306/ecommerce \
  --username root \
  --password your_password \
  --table customers \
  --target-dir /user/root/ecommerce/customers \
  --m 1

# Import orders table
sqoop import \
  --connect jdbc:mysql://localhost:3306/ecommerce \
  --username root \
  --password your_password \
  --table orders \
  --target-dir /user/root/ecommerce/orders \
  --m 1

# Import order_items (split by primary key for parallelism)
sqoop import \
  --connect jdbc:mysql://localhost:3306/ecommerce \
  --username root \
  --password your_password \
  --table order_items \
  --target-dir /user/root/ecommerce/order_items \
  --m 1 \
  --split-by order_item_id

# Import reviews
sqoop import \
  --connect jdbc:mysql://localhost:3306/ecommerce \
  --username root \
  --password your_password \
  --table reviews \
  --target-dir /user/root/ecommerce/reviews \
  --m 1

# Import products
sqoop import \
  --connect jdbc:mysql://localhost:3306/ecommerce \
  --username root \
  --password your_password \
  --table products \
  --target-dir /user/root/ecommerce/products \
  --m 1
```

**✅ When to use Sqoop:**
- Large-scale, scheduled batch ingestion
- Full table or incremental imports
- Production environments with existing Hadoop clusters

---

### 🟢 Method 2 — Apache NiFi (Visual Flow-Based ETL)

NiFi provides a drag-and-drop UI to build data flows without writing code. It supports real-time and batch ingestion with built-in error handling, provenance, and backpressure.

**NiFi Flow Design for this project:**

![NiFi_Flow](https://github.com/AhemdMahmoud/brazilian-ecommerce-pipeline/blob/main/images/nifi%20flow.png)

```
┌─────────────────────────────────────────────────────────────────┐
│                        NiFi Canvas Flow                         │
│                                                                 │
│  [ExecuteSQL]          [ConvertRecord]       [PutHDFS]          │
│  Query MySQL    ──▶    CSV → Parquet   ──▶   Write to HDFS      │
│  (per table)           (SchemaRegistry)      (/user/root/       │
│                                               ecommerce/)       │
│                                                                 │
│  Processors used:                                               │
│  • ExecuteSQL / QueryDatabaseTable  — read from MySQL           │
│  • ConvertRecord                    — transform format          │
│  • PutHDFS / PutParquet             — write to HDFS             │
│  • RouteOnAttribute                 — route per table           │
│  • LogAttribute                     — monitoring & debugging    │
└─────────────────────────────────────────────────────────────────┘
```

**NiFi Controller Services required:**
| Service | Purpose |
|---------|---------|
| `DBCPConnectionPool` | JDBC connection to MySQL |
| `AvroSchemaRegistry` | Schema definition for record conversion |
| `CSVReader / ParquetWriter` | Record format conversion |

**✅ When to use NiFi:**
- Real-time or near-real-time streaming ingestion
- Complex routing and transformation logic
- Teams preferring no-code / low-code visual pipelines
- When you need built-in data lineage and provenance tracking

> 📁 The NiFi flow template XML file is available in `ingestion/nifi/ecommerce_ingest_flow.xml`

---

### ⚖️ Sqoop vs NiFi — Quick Comparison

| Feature | Apache Sqoop | Apache NiFi |
|---------|-------------|-------------|
| Interface | CLI / Shell Scripts | Visual drag-and-drop UI |
| Use case | Batch bulk transfer | Batch + Real-time streaming |
| Learning curve | Low (SQL-like) | Medium (UI concepts) |
| Error handling | Basic | Advanced (backpressure, retry) |
| Data lineage | None built-in | Full provenance tracking |
| Real-time support | ❌ | ✅ |
| Format conversion | Basic | Rich (CSV, JSON, Parquet, Avro) |
| Scheduling | External (Cron/Airflow) | Built-in scheduler |
| Maintenance | Deprecated (no new features) | Actively maintained |

---

## 🐳 Docker Setup

All services run inside Docker containers orchestrated by Docker Compose.

### Prerequisites

- Docker Desktop installed and running
- At least **8GB RAM** allocated to Docker
- Ports `8080`, `9870`, `10000`, `3306`, `4040` available

### Start All Services

```bash
# Clone the repository
git clone https://github.com/AhemdMahmoud/brazilian-ecommerce-pipeline.git
cd brazilian-ecommerce-pipeline

# Start the full stack
docker-compose up -d

# Check all containers are running
docker ps
```

### Services & Ports

| Service | Container Name | Port | UI URL |
|---------|---------------|------|--------|
| HDFS NameNode | `namenode` | 9870 | http://localhost:9870 |
| HDFS DataNode | `datanode` | 9864 | http://localhost:9864 |
| YARN ResourceManager | `resourcemanager` | 8088 | http://localhost:8088 |
| Apache Hive | `hive-server` | 10000 | — |
| Apache Spark | `spark-master` | 4040 | http://localhost:4040 |
| Apache NiFi | `nifi` | 8443 | https://localhost:8443/nifi |
| MySQL | `mysql` | 3306 | via DB Visualizer |
![docker](https://github.com/AhemdMahmoud/brazilian-ecommerce-pipeline/blob/main/images/docker_image.png)
### Stop Services

```bash
docker-compose down
```

---

## 🗄️ DB Visualizer Setup

[DB Visualizer](https://www.dbvis.com/) (or DBeaver) is used to explore the MySQL schema and run HiveQL queries visually.

### Connect to MySQL

| Field | Value |
|-------|-------|
| Driver | MySQL |
| Host | `localhost` |
| Port | `3306` |
| Database | `ecommerce` |
| Username | `root` |
| Password | `secret` |

### Connect to Hive

| Field | Value |
|-------|-------|
| Driver | Apache Hive |
| Host | `localhost` |
| Port | `10000` |
| Database | `default` |
| Authentication | None (or NOSASL) |

![DB_Visualizer](https://github.com/AhemdMahmoud/brazilian-ecommerce-pipeline/blob/main/images/db_visulizer.png)

> 💡 **Tip:** After connecting, you can explore the `silver` and `gold` databases directly in the schema tree, and run analytical queries against the Gold layer Hive external tables.

---

## 🚀 Step-by-Step Pipeline Walkthrough

### Step 1 — Load Data into MySQL (Docker)

```bash
# Copy CSV files into the MySQL container
docker cp data/customers.csv  <mysql_container_id>:/var/lib/mysql-files/customers.csv
docker cp data/orders.csv     <mysql_container_id>:/var/lib/mysql-files/orders.csv
docker cp data/order_items.csv <mysql_container_id>:/var/lib/mysql-files/order_items.csv
docker cp data/reviews.csv    <mysql_container_id>:/var/lib/mysql-files/reviews.csv
docker cp data/products.csv   <mysql_container_id>:/var/lib/mysql-files/products.csv

# Enter the MySQL container
docker exec -it <mysql_container_id> bash
mysql -u root -p    # password: secret
```

Then run in DB Visualizer or MySQL CLI:
```sql
-- 1. Create the database and tables
source sql/DDL_ecommerce.sql;

-- 2. Load all CSV data
source sql/data_loaded.sql;
```

---

### Step 2 — Transfer MySQL → HDFS

**Option A: Apache Sqoop**
```bash
# Run from inside the Sqoop/namenode container
# All commands are in: ingestion/sqoop_mysql_cmd_hdfs.sql
bash ingestion/sqoop_import.sh
```

**Option B: Manual HDFS Put (without Sqoop)**
```bash
# Enter namenode container
docker exec -it namenode bash

# Create HDFS directories
for t in customers orders order_items reviews products; do
  hdfs dfs -mkdir -p /user/root/ecommerce/$t
done

# Put CSV files into HDFS
hdfs dfs -put /root/customers.csv   /user/root/ecommerce/customers/
hdfs dfs -put /root/orders.csv      /user/root/ecommerce/orders/
hdfs dfs -put /root/order_items.csv /user/root/ecommerce/order_items/
hdfs dfs -put /root/reviews.csv     /user/root/ecommerce/reviews/
hdfs dfs -put /root/products.csv    /user/root/ecommerce/products/
```

**Option C: Apache NiFi**
```
1. Open NiFi UI at https://localhost:8443/nifi
2. Import flow template: ingestion/nifi/ecommerce_ingest_flow.xml
3. Configure DBCPConnectionPool with MySQL credentials
4. Start the flow — data will automatically land in HDFS
```

---

### Step 3 — Bronze to Silver (PySpark Cleaning)

```bash
# Submit PySpark job (from spark container or local spark-submit)
spark-submit spark/Pipeline_pyspark.py

# Or run interactively in Jupyter Notebook
# Open spark/Pipeline_pyspark.ipynb
```

![silver_table](https://github.com/AhemdMahmoud/brazilian-ecommerce-pipeline/blob/main/images/hue/silver.png)

**What the Silver pipeline does:**

| Table | Actions |
|-------|---------|
| `customers` | Schema validation, null check |
| `orders` | Null timestamps preserved (business reality) |
| `order_items` | Null/NaN check on price & freight columns |
| `reviews` | Type cast, **deduplicate**, drop null keys, fill comment nulls |
| `products` | Fill category nulls with `'unknown'`, fill dimension nulls with `0` |

**Output:** Clean Parquet files written to `hdfs:///user/root/silver/`

---

### Step 4 — Create Silver Hive Tables

```sql
-- Run in Hive (via DB Visualizer, Beeline, or HiveQL shell)
source sql/silver_hive.sql;

-- Validate
SELECT * FROM silver.silver_customers LIMIT 10;
SELECT * FROM silver.silver_orders LIMIT 10;
```

---

### Step 5 — Silver to Gold (Star Schema Build)

The PySpark pipeline continues to build the Gold layer:

```
Silver Tables → PySpark Joins → Gold Parquet → Hive External Tables
```

**Star Schema:**

```
                    dim_customers
                         │
   dim_products ──── fact_sales ──── dim_reviews
                         │
                      dim_time
```

| Table | Grain | Key Columns |
|-------|-------|-------------|
| `fact_sales` | 1 row = 1 order item | order_id, customer_id, product_id, price, freight, delivery_time_days, total_price |
| `dim_customers` | 1 row = 1 customer | customer_id, unique_id, city, state, zip |
| `dim_products` | 1 row = 1 product | product_id, category, weight, dimensions, photos |
| `dim_reviews` | 1 row = 1 review | review_id, order_id, score, **is_positive**, date |
| `dim_time` | 1 row = 1 order | order_id, order_date, year, month, day |


![gold_Hdfs](https://github.com/AhemdMahmoud/brazilian-ecommerce-pipeline/blob/main/images/hue/gold.png)
![gold_hive](https://github.com/AhemdMahmoud/brazilian-ecommerce-pipeline/blob/main/images/hue/customer_dim__parquet_example_hue.png)

**Computed KPI columns added in Gold:**
- `delivery_time_days` = `datediff(order_delivered_customer_date, order_purchase_timestamp)`
- `total_price` = `price + freight_value`
- `is_positive` = `review_score >= 4`

---

### Step 6 — Create Gold Hive Tables

```sql
-- Run in Hive
source sql/gold_hive.sql;

-- Validate Gold layer
source sql/gold_test.sql;
```

---
![gold_hive](https://github.com/AhemdMahmoud/brazilian-ecommerce-pipeline/blob/main/images/hue/gold_table_hue.png)


## 📊 HDFS Directory Structure

```
/user/root/
│
├── ecommerce/               ← Bronze: Raw CSV files from MySQL
│   ├── customers/
│   ├── orders/
│   ├── order_items/
│   ├── reviews/
│   └── products/
│
├── ecommerce_parquet/       ← Bronze: Converted to Parquet
│   ├── customers/
│   ├── orders/
│   ├── order_items/
│   ├── reviews/
│   └── products/
│
├── silver/                  ← Silver: Cleaned Parquet
│   ├── customers/
│   ├── orders/
│   ├── order_items/
│   ├── reviews/
│   └── products/
│
└── goldl/                   ← Gold: Star Schema Parquet
    ├── fact_sales/
    ├── dim_customers/
    ├── dim_products/
    ├── dim_reviews/
    └── dim_time/
```

---

![hive_layer_schema](https://github.com/AhemdMahmoud/brazilian-ecommerce-pipeline/blob/main/images/hue/hive_layer_schema.png)
![hive_layer_schema](https://github.com/AhemdMahmoud/brazilian-ecommerce-pipeline/blob/main/images/hue/store.png)

## 🔍 Hive Database Structure

```sql
-- Silver Database
SHOW TABLES IN silver;
-- silver_customers, silver_orders, silver_order_items, silver_reviews, silver_products

-- Gold Database
SHOW TABLES IN gold;
-- fact_sales, dim_customers, dim_products, dim_reviews, dim_time
```

All tables are **EXTERNAL** — Hive manages metadata only; data lives in HDFS.

---

## 📈 Sample Analytical Queries (Gold Layer)

```sql
-- Total revenue by month
SELECT dt.year, dt.month, SUM(fs.total_price) AS monthly_revenue
FROM gold.fact_sales fs
JOIN gold.dim_time dt ON fs.order_id = dt.order_id
GROUP BY dt.year, dt.month
ORDER BY dt.year, dt.month;

-- Average delivery time by state
SELECT fs.customer_state, AVG(fs.delivery_time_days) AS avg_delivery_days
FROM gold.fact_sales fs
WHERE fs.delivery_time_days IS NOT NULL
GROUP BY fs.customer_state
ORDER BY avg_delivery_days ASC;

-- Customer satisfaction by product category
SELECT dp.product_category_name,
       COUNT(*) AS total_reviews,
       SUM(CASE WHEN dr.is_positive THEN 1 ELSE 0 END) AS positive_reviews,
       ROUND(SUM(CASE WHEN dr.is_positive THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS satisfaction_pct
FROM gold.fact_sales fs
JOIN gold.dim_products dp ON fs.product_id = dp.product_id
JOIN gold.dim_reviews dr ON fs.order_id = dr.order_id
GROUP BY dp.product_category_name
ORDER BY satisfaction_pct DESC;

-- Top 10 cities by revenue
SELECT dc.customer_city, SUM(fs.total_price) AS total_revenue
FROM gold.fact_sales fs
JOIN gold.dim_customers dc ON fs.customer_id = dc.customer_id
GROUP BY dc.customer_city
ORDER BY total_revenue DESC
LIMIT 10;
```

---

## ⚠️ Data Quality Notes

| Issue | Table | Resolution |
|-------|-------|-----------|
| Duplicate `review_id` | reviews | `dropDuplicates(["review_id"])` |
| Wrong types on review fields | reviews | Cast `review_score` → INT, parse timestamps |
| Null comment fields | reviews | Fill with `"no title"` / `"no comment"` |
| Null `product_category_name` | products | Fill with `"unknown"` |
| Null physical dimensions | products | Fill with `0` |
| Null delivery timestamps | orders | **Preserved** — represent real-world order states |

---
# Connect Power BI to Hive using ODBC
#### -- 1. Install Hive ODBC Driver then config settings

![A Hive ODBC driver_config](https://github.com/AhemdMahmoud/brazilian-ecommerce-pipeline/blob/main/images/powerbi_connect_hive_ODBC/Ah.png)
### Test
![hive_layer_schema](https://github.com/AhemdMahmoud/brazilian-ecommerce-pipeline/blob/main/images/powerbi_connect_hive_ODBC/hf.png)
### Load Data from Hive
![hive_layer_schema](https://github.com/AhemdMahmoud/brazilian-ecommerce-pipeline/blob/main/images/powerbi_connect_hive_ODBC/Screenshot%202026-04-25%20212715.png)

## 📋 Prerequisites & Requirements

```
Docker Desktop >= 4.x
Docker Compose >= 2.x
Python >= 3.8
Apache Spark >= 3.x  (inside container)
Apache Hadoop >= 3.x (inside container)
Apache Hive >= 3.x   (inside container)
Apache NiFi >= 1.x   (inside container)
DB Visualizer or DBeaver (local)
```

---

## 🤝 Acknowledgements

- **Dataset:** [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — Kaggle
- **Institution:** Information Technology Institute (ITI) — Egypt
- **Track:** Big Data Engineering — 9-Month Professional Diploma

---

## 📄 License

This project is developed for academic purposes as part of the ITI 9-Month Professional Diploma graduation requirement.

---

<div align="center">

**⭐ If you found this project helpful, please give it a star!**

*Built with ❤️ as part of ITI Big Data Track — 2026*

</div>
