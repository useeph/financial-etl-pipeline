# Data Dictionary

## Raw Layer

### `FINANCIAL_ETL.RAW.STOCK_PRICES`

Immutable landing zone. Loaded via Snowflake `COPY INTO` from S3 stage.

| Column | Type | Description |
|---|---|---|
| `date` | DATE | Trading date in NYSE local time |
| `open` | FLOAT | Opening price, USD |
| `high` | FLOAT | Intraday high, USD |
| `low` | FLOAT | Intraday low, USD |
| `close` | FLOAT | Closing price, USD (unadjusted) |
| `adj_close` | FLOAT | Closing price adjusted for splits and dividends |
| `volume` | NUMBER(20,0) | Shares traded |
| `ticker` | VARCHAR(10) | Stock symbol (e.g. `AAPL`) |
| `extracted_at` | TIMESTAMP_NTZ | UTC timestamp when the row was pulled from yfinance |
| `load_date` | DATE | Date the row was loaded into Snowflake (`CURRENT_DATE()` at COPY INTO time) |
| `source_file` | VARCHAR(500) | S3 key of the source file (`METADATA$FILENAME`) |

**Grain:** one row per ticker per trading day, per ingestion. Duplicates expected across daily snapshots; dedup happens in staging.

## Staging Layer

### `FINANCIAL_ETL.ANALYTICS_STAGING.STG_STOCK_PRICES`

dbt view. Clean and deduplicate raw rows.

| Column | Type | Description |
|---|---|---|
| `price_date` | DATE | Trading date |
| `ticker` | VARCHAR(10) | Stock symbol, upper-cased and trimmed |
| `open_price` | FLOAT | Opening price |
| `high_price` | FLOAT | Intraday high |
| `low_price` | FLOAT | Intraday low |
| `close_price` | FLOAT | Closing price (unadjusted) |
| `adj_close_price` | FLOAT | Closing price adjusted for splits/dividends |
| `volume` | NUMBER(20,0) | Shares traded |
| `source_extracted_at` | TIMESTAMP_NTZ | When the row was pulled from yfinance |
| `warehouse_loaded_date` | DATE | When the row was loaded into Snowflake |
| `source_file_path` | VARCHAR(500) | S3 key of the source file |

**Dedup logic:** for each `(ticker, price_date)` pair, keep the row with the latest `source_extracted_at`.

## Marts Layer

### `FINANCIAL_ETL.ANALYTICS_ANALYTICS.DIM_COMPANY`

Slowly Changing Dimension Type 2. One row per ticker per period of attribute stability.

| Column | Type | Description |
|---|---|---|
| `company_key` | VARCHAR | Surrogate key — MD5 hash of `(ticker, valid_from)` |
| `ticker` | VARCHAR(10) | Stock symbol (natural key) |
| `sector` | VARCHAR | Industry sector |
| `valid_from` | TIMESTAMP | Start of validity window |
| `valid_to` | TIMESTAMP | End of validity window (`9999-12-31` for current rows) |
| `is_current` | BOOLEAN | TRUE if this row reflects the current state |
| `scd_id` | VARCHAR | dbt-generated SCD identifier |
| `dbt_loaded_at` | TIMESTAMP | When this row was created by dbt |

### `FINANCIAL_ETL.ANALYTICS_ANALYTICS.DIM_DATE`

Calendar dimension generated from `dbt.date_spine`. Covers 2020-01-01 to 2030-12-31.

| Column | Type | Description |
|---|---|---|
| `date_key` | DATE | The date (primary key) |
| `year` | NUMBER | Calendar year |
| `quarter` | NUMBER | Calendar quarter 1–4 |
| `month` | NUMBER | Calendar month 1–12 |
| `month_name` | VARCHAR | Three-letter month name (e.g. `Jan`) |
| `week_of_year` | NUMBER | ISO week 1–53 |
| `day_of_month` | NUMBER | 1–31 |
| `day_of_week` | NUMBER | 0=Sunday, 6=Saturday |
| `day_name` | VARCHAR | Three-letter day name (e.g. `Mon`) |
| `is_weekday` | BOOLEAN | TRUE for Mon–Fri |
| `is_weekend` | BOOLEAN | TRUE for Sat/Sun |

### `FINANCIAL_ETL.ANALYTICS_ANALYTICS.FCT_DAILY_PRICES`

Star schema fact. One row per ticker per trading day.

| Column | Type | Description |
|---|---|---|
| `price_key` | VARCHAR | Surrogate key — MD5 hash of `(ticker, price_date)` |
| `company_key` | VARCHAR | FK to `dim_company` (point-in-time join on `valid_from`/`valid_to`) |
| `price_date` | DATE | FK to `dim_date.date_key` |
| `ticker` | VARCHAR(10) | Stock symbol (degenerate dim) |
| `open_price` | FLOAT | Opening price |
| `high_price` | FLOAT | Intraday high |
| `low_price` | FLOAT | Intraday low |
| `close_price` | FLOAT | Closing price (unadjusted) |
| `adj_close_price` | FLOAT | Closing price adjusted for splits/dividends |
| `volume` | NUMBER(20,0) | Shares traded |
| `daily_change` | FLOAT | `close_price - open_price` |
| `daily_pct_change` | FLOAT | `(close - open) / open * 100` |
| `daily_range` | FLOAT | `high_price - low_price` |
| `dollar_volume` | FLOAT | `close_price * volume` |
| `source_extracted_at` | TIMESTAMP_NTZ | Provenance from staging |
| `warehouse_loaded_date` | DATE | Provenance from staging |

**Clustered by:** `(price_date, ticker)` — optimizes the most common query pattern (date-range filters with optional ticker filter).