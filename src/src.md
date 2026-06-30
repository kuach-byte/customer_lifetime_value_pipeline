# Source Documentation

## Overview

Two standalone scripts that ingest the Online Retail II dataset into PostgreSQL and export a data quality summary to CSV.

---

## `ingestion.py` — Raw Data Importer

Downloads the dataset from Kaggle and loads it into PostgreSQL via chunked ingestion.

### Prerequisites

| Requirement | Detail |
|---|---|
| Python packages | `pandas`, `sqlalchemy`, `psycopg2`, `kaggle`, `python-dotenv` |
| `.env` variables | `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `KAGGLE_USERNAME`, `KAGGLE_KEY` |

### What it does

1. Validates all env vars and Kaggle credentials.
2. Downloads `mashlyn/online-retail-ii-uci` from Kaggle if no CSV exists in `./data/`.
3. Creates `online_retail_raw` and `import_log` tables (idempotent — no drops).
4. Ingests the CSV in 50,000-row chunks with column renaming and date coercion.
5. Logs the import (filename, row count, file size) to `import_log`.
6. Prints a verification summary (total rows, unique invoices/customers/products/countries).

### Output table: `online_retail_raw`

| Column | Type |
|---|---|
| `invoice_no` | `VARCHAR(50)` |
| `stock_code` | `VARCHAR(50)` |
| `description` | `TEXT` |
| `quantity` | `FLOAT` |
| `invoice_date` | `TIMESTAMP` |
| `unit_price` | `FLOAT` |
| `customer_id` | `VARCHAR(50)` |
| `country` | `VARCHAR(100)` |
| `imported_at` | `TIMESTAMP` (auto) |


---

## `exp_summary.py` — DQ Summary Exporter

Queries the `dq1_summary` view and exports it as a CSV.

### Prerequisites

| Requirement | Detail |
|---|---|
| Python packages | `pandas`, `sqlalchemy`, `psycopg2`, `python-dotenv` |
| `.env` variables | `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` |
| DB dependency | `public.dq1_summary` view must exist |

### What it does

1. Validates env vars and tests the DB connection.
2. Queries `dq1_summary` ordered by `dimension`, `check_name`.
3. Exports to `data/exports/dq_summary.csv` (creates directory if missing).

### Exported columns

`dimension`, `check_name`, `metric_name`, `metric_value`, `rate`, `n_total`
