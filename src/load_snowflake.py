"""Trigger COPY INTO from S3 stage into Snowflake raw table.

Idempotent: only loads new files not seen before (tracked by Snowflake's
LOAD_HISTORY automatically via COPY INTO).
"""
from __future__ import annotations

import logging
import os

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)


COPY_SQL = """
COPY INTO FINANCIAL_ETL.RAW.STOCK_PRICES (
  date, open, high, low, close, adj_close, volume, ticker,
  extracted_at, load_date, source_file
)
FROM (
  SELECT
    DATEADD(millisecond, $1:date::NUMBER, '1970-01-01'::DATE)::DATE,
    $1:open::FLOAT,
    $1:high::FLOAT,
    $1:low::FLOAT,
    $1:close::FLOAT,
    $1:adj_close::FLOAT,
    $1:volume::NUMBER(20, 0),
    $1:ticker::VARCHAR(10),
    $1:extracted_at::TIMESTAMP_NTZ,
    CURRENT_DATE() AS load_date,
    METADATA$FILENAME AS source_file
  FROM @FINANCIAL_ETL.RAW.S3_STAGE
)
FILE_FORMAT = (FORMAT_NAME = FINANCIAL_ETL.RAW.PARQUET_FORMAT)
PATTERN = '.*prices.parquet'
ON_ERROR = 'ABORT_STATEMENT';
"""


def get_snowflake_connection():
    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    # Strip container path prefix when running locally
    if not os.path.exists(key_path) and key_path.startswith("/opt/airflow/"):
        key_path = key_path.replace("/opt/airflow/", "")
    with open(key_path, "rb") as f:
        p_key = serialization.load_pem_private_key(f.read(), password=None)
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        private_key=pkb,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def copy_into_raw() -> dict:
    """Run COPY INTO. Returns load summary."""
    log.info("Connecting to Snowflake")
    conn = get_snowflake_connection()
    cur = conn.cursor()

    try:
        log.info("Running COPY INTO FINANCIAL_ETL.RAW.STOCK_PRICES")
        cur.execute(COPY_SQL)
        results = cur.fetchall()
        col_names = [desc[0].lower() for desc in cur.description] if cur.description else []

        total_loaded = 0
        total_errors = 0
        files_processed = 0

        for row in results:
            row_dict = dict(zip(col_names, row))

            if len(row_dict) == 1 and "status" in row_dict:
                log.info(f"  {row_dict['status']}")
                continue

            file_name = row_dict.get("file", "<unknown>")
            status = row_dict.get("status", "<unknown>")
            rows_loaded = row_dict.get("rows_loaded") or 0
            errors_seen = row_dict.get("errors_seen") or 0

            log.info(f"  {file_name}: status={status}, loaded={rows_loaded}, errors={errors_seen}")
            total_loaded += rows_loaded
            total_errors += errors_seen
            files_processed += 1

        cur.execute("SELECT COUNT(*) FROM FINANCIAL_ETL.RAW.STOCK_PRICES")
        total_in_table = cur.fetchone()[0]

        summary = {
            "files_processed": files_processed,
            "rows_loaded_this_run": total_loaded,
            "errors": total_errors,
            "total_rows_in_table": total_in_table,
        }
        log.info(f"Load summary: {summary}")
        return summary

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    copy_into_raw()