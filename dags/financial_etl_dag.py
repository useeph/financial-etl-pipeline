"""Daily financial ETL pipeline.

Pipeline:
  1. extract:        yfinance -> local parquet
  2. load_s3:        local parquet -> S3
  3. load_snowflake: S3 -> Snowflake raw table (idempotent COPY INTO)

Tasks run sequentially; any failure stops downstream tasks.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make ../src importable inside the container
SRC_PATH = "/opt/airflow/src"
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

from extract import main as run_extract  # noqa: E402
from load_s3 import main as run_load_s3  # noqa: E402
from load_snowflake import copy_into_raw  # noqa: E402


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


with DAG(
    dag_id="financial_etl_pipeline",
    description="Daily S&P 500 prices: yfinance -> S3 -> Snowflake",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="0 22 * * 1-5",  # 10pm UTC, weekdays only (after US market close)
    catchup=False,
    max_active_runs=1,
    tags=["financial", "etl", "snowflake"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_prices",
        python_callable=run_extract,
        doc_md="Downloads daily OHLCV data for S&P 500 from yfinance, writes partitioned parquet.",
    )

    load_s3_task = PythonOperator(
        task_id="load_to_s3",
        python_callable=run_load_s3,
        doc_md="Uploads the latest parquet partition to s3://bucket/raw/stock_prices/date=*/",
    )

    load_snowflake_task = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=copy_into_raw,
        doc_md="Runs COPY INTO from S3 stage into FINANCIAL_ETL.RAW.STOCK_PRICES (idempotent).",
    )

    extract_task >> load_s3_task >> load_snowflake_task