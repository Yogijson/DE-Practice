FROM python:3.11-slim

RUN apt-get update && apt-get install -y git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY orders_lakehouse/requirements.txt .
RUN pip install --no-cache-dir \
    dbt-core==1.8.7 \
    dbt-spark==1.8.0 \
    dbt-databricks==1.8.3

COPY orders_lakehouse/ ./orders_lakehouse/

ENV DBT_DATABRICKS_TOKEN=""
ENV DBT_DATABRICKS_HOST=""
ENV DBT_DATABRICKS_HTTP_PATH=""

CMD ["dbt", "run", "--project-dir", "orders_lakehouse"]