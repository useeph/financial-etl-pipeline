-- =============================================================
-- Snowflake setup for financial ETL pipeline
-- Run as ACCOUNTADMIN
-- =============================================================

USE ROLE ACCOUNTADMIN;

-- 1. Warehouse: smallest size, auto-suspend after 60s of idle
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- 2. Database and schemas
CREATE DATABASE IF NOT EXISTS FINANCIAL_ETL;
USE DATABASE FINANCIAL_ETL;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- 3. File format for parquet
CREATE OR REPLACE FILE FORMAT FINANCIAL_ETL.RAW.PARQUET_FORMAT
  TYPE = PARQUET;

-- 4. Storage integration — secure connection to S3 (no AWS keys stored in Snowflake)
CREATE OR REPLACE STORAGE INTEGRATION S3_FINANCIAL_ETL
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::910661160260:role/snowflake-s3-access-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://yusif-financial-etl-raw-2026/');

-- 5. External stage pointing at the partitioned S3 path
CREATE OR REPLACE STAGE FINANCIAL_ETL.RAW.S3_STAGE
  STORAGE_INTEGRATION = S3_FINANCIAL_ETL
  URL = 's3://yusif-financial-etl-raw-2026/raw/stock_prices/'
  FILE_FORMAT = FINANCIAL_ETL.RAW.PARQUET_FORMAT;

-- 6. Raw landing table with data lineage columns
CREATE OR REPLACE TABLE FINANCIAL_ETL.RAW.STOCK_PRICES (
  date          DATE,
  open          FLOAT,
  high          FLOAT,
  low           FLOAT,
  close         FLOAT,
  adj_close     FLOAT,
  volume        NUMBER(20, 0),
  ticker        VARCHAR(10),
  extracted_at  TIMESTAMP_NTZ,
  load_date     DATE,
  source_file   VARCHAR(500)
);

-- 7. COPY INTO — load parquet from S3 stage
-- Pandas writes dates as ms-since-epoch ints; DATEADD converts them back to DATE
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

-- 8. Verification
SELECT COUNT(*) AS row_count,
       COUNT(DISTINCT ticker) AS ticker_count,
       MIN(date) AS earliest,
       MAX(date) AS latest
FROM FINANCIAL_ETL.RAW.STOCK_PRICES;