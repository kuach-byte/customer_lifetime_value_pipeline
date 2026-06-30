# dbt Models Documentation

This document provides a concise overview of the dbt models built for the online retail dataset (`public.online_retail_raw`). Models are organized by layer: **staging**, **intermediate**, and **data quality (DQ)**.

---

## Architecture Note

During the early stages of the project, a dimensional data warehouse was developed using dbt, including staging, intermediate, and mart models following a star schema design. While this architecture was appropriate for reporting and business intelligence, it proved unsuitable for the requirements of the CLV prediction pipeline. Customer Lifetime Value modelling required customer-level feature engineering, machine learning model benchmarking, K-Means clustering, per-cluster model selection, and iterative experimentation. These workflows involved complex transformations and dependencies that extended beyond the scope of dbt's SQL-centric transformation layer.

For this reason, the modelling pipeline was implemented separately in Python using notebook-based workflows. In addition, the dbt marts were aggregated at a monthly reporting grain, whereas the CLV models required a single customer snapshot with detailed behavioural features (such as recency, frequency, tenure, purchase gaps, and product diversity) at the individual customer level. Maintaining both pipelines would have introduced redundant transformations and duplicated business logic. Instead, the dbt warehouse continued to serve as the source of clean transactional data, while the Python pipeline generated the analytical outputs that were ultimately exported as Power BI fact and dimension tables.

---

## Staging Layer

### `stg_retail_cleaned`
Cleans and standardizes raw retail transactions from `public.online_retail_raw`: trims text fields, casts data types (quantity, unit_price, invoice_date), and adds a `country_invalid` flag based on format and an accepted-country list.

---

## Intermediate Layer

### `int_retail_deduplicated`
Removes duplicate `(invoice_no, stock_code)` combinations from `stg_retail_cleaned`, keeping only the most recent record per combination (via `ROW_NUMBER()` ordered by `invoice_date DESC`).

### `int_retail_enriched`
Builds on `int_retail_deduplicated` by adding a calculated `revenue` column (`quantity * unit_price`) for downstream analytics.

### `int_retail_filtered`
Filters `int_retail_enriched` to remove logically inconsistent rows where `quantity > 0` and `unit_price < 0`.

### `int_customer_activity_months`
Derives a deduplicated customer-month activity table from `int_retail_filtered`, with one row per `(customer_id, activity_month)` where a transaction occurred (nulls excluded).

---

## Data Quality (DQ) Layer

These models assess the quality of the raw source data (`public.online_retail_raw`) across the standard DQ dimensions, and roll results up into row-level and business-facing summaries.

### `dq1_completeness`
Checks for null/empty values across key columns (invoice_no, stock_code, description, quantity, invoice_date, unit_price, customer_id, country). Outputs one summary row with per-column completeness rates and row-level completeness counts.

### `dq1_validity`
Validates format, range, and domain rules per column (e.g., invoice_no pattern, stock_code character set, quantity/unit_price bounds, valid country list, future-dated invoices). Outputs invalid counts and validity rates per column plus a row-level `fully_valid` rate.

### `dq1_uniqueness`
Detects duplicate `(invoice_no, stock_code)` combinations, reporting duplicate group/row counts and a composite uniqueness rate.

### `dq1_consistency`
Checks cross-column business rules: sale rows (`quantity > 0`) must have `unit_price > 0`, and return rows (`quantity < 0`) must also have `unit_price > 0`. (Chronological ordering check is intentionally skipped — no reliable row-order column exists.)

### `dq1_summary`
Unpivots the completeness, validity, uniqueness, and consistency models into a single stacked table (one row per dimension/check/metric), including an overall DQ score and a PASS/WARN/FAIL status per check.

### `dq1_row_level_flags`
Row-level model combining all DQ dimensions (completeness, validity, consistency, uniqueness) into a single output per source row, with an `affected_row` flag and an `error_count` of total rule violations.

### `dq1_usable_data_summary`
Business-facing summary built on `dq1_row_level_flags`, computing the overall percentage of usable (fully clean) data, error distribution, a quality grade (EXCELLENT/GOOD/ACCEPTABLE/POOR/UNACCEPTABLE), and a recommended drop/retain strategy for affected rows.
