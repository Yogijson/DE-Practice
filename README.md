# Data Engineering Practice — End-to-End Lakehouse Pipeline

A production-style data platform built with modern DE tools.

## Architecture
Bronze → Silver → Gold lakehouse pattern on Databricks + Delta Lake

## Stack
- **PySpark** — distributed data processing
- **Delta Lake** — ACID transactions, time travel, schema enforcement
- **Apache Airflow** — pipeline orchestration
- **dbt** — SQL transformations, data testing, lineage documentation
- **Snowflake** — Virtual Warehouses, Streams, Tasks, Time Travel, Zero-Copy Cloning

## Pipeline
- `bronze_orders` — raw ingestion (incremental load)
- `silver_orders` — cleaned and validated (4 data quality tests)
- `gold_order_summary` — aggregated revenue by status and date

## Data Quality
- Unique and not-null checks on primary keys
- Accepted values validation on status column
- Null checks scoped to valid rows only

## Tools
Python 3.11 · dbt-databricks 1.12 · Delta Lake · Databricks Community Edition
