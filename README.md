# Customer Lifetime Value (CLV) Pipeline

### dbt-Powered Analytics Foundation + ML-Ready Feature Layer

---

## Overview

This project implements a **production-style customer analytics pipeline** to support **Customer Lifetime Value (CLV) modeling**.

It establishes a **clean, scalable feature layer in dbt**, which is extended using Python for **exploratory data analysis (EDA)** and **advanced feature engineering**.

The pipeline follows a modern data stack design:

* **dbt** → deterministic transformations & feature generation
* **Python** → statistical analysis & ML feature engineering

---

## Objectives

1. Perform comprehensive **data quality validation**
2. Build a **time-consistent customer feature layer**
3. Implement **RFM (Recency, Frequency, Monetary) modeling**
4. Ensure **incremental and scalable transformations**
5. Enable downstream use cases:

   * Exploratory Data Analysis (EDA)
   * Survival analysis (time-to-event modeling)
   * CLV prediction using machine learning

---

## Tech Stack

* **dbt (Postgres adapter)** — transformation layer
* **PostgreSQL** — data warehouse
* **Python** — EDA, feature engineering, machine learning

---

## Data Modeling Approach

The project follows a layered architecture:

### 1. Staging Layer (`stg_`)

* Cleans and standardizes raw data
* Handles:

  * Null values
  * Type casting
  * Column selection

---

### 2. Intermediate Layer (`int_`)

* Applies business logic
* Includes:

  * Deduplication
  * Transaction filtering
  * Dataset enrichment

---

### 3. Dimension Layer (`dim_`)

* Monthly date dimension with deterministic indexing
* Supports:

  * Time-series expansion
  * Cohort analysis
  * Feature engineering

---

### 4. Mart Layer (`fct_`)

Analytics-ready feature tables.

#### `fct_composite_grain`

* Core monthly snapshot table
* Grain: `(customer_id, snapshot_date)`
* Tracks:

  * Activity flags
  * Customer lifecycle progression
  * Months since first purchase

---

#### `fct_customer_rfm`

* Computes RFM metrics per snapshot

Features:

* **Recency** — time since last purchase
* **Frequency** — number of transactions
* **Monetary** — total revenue

---

## Incremental Modeling

Key models are built using **incremental materialization**:

* Avoids full table rebuilds
* Processes only new snapshot periods

Performance improvement:

> **O(customers × months) → O(new data only)**




## Data Quality & Testing

### Data Quality Framework

The pipeline enforces:

* **Consistency**
* **Validity**
* **Uniqueness**
* **Completeness**

---

### dbt Testing

* `not_null` constraints
* `unique` keys
* Custom tests (e.g. recency consistency)

This ensures:

* Reliable feature generation
* Trustworthy downstream ML inputs

---

## Data Quality Visualization

![Data Quality Summary](assets/dq_summary.png)

📄 Full Report: [Download PDF](assets/data_quality/dq_summary.pdf)




## Feature Engineering Strategy

### In dbt (Current Scope)

Deterministic, reusable features:

* RFM metrics
* Customer tenure
* Activity indicators
* Time-consistent snapshots

---

### In Python (Next Phase)

Advanced feature engineering:

* Distribution analysis (EDA)
* Log transformations for skewed features
* Feature scaling & normalization
* Survival modeling inputs:

  * Time-to-event (T)
  * Censoring indicators

---

### Sample output

SELECT *
FROM fct_customer_rfm
ORDER BY customer_id, snapshot_date
LIMIT 10;

| customer_id | snapshot_date | had_activity | months_since_first_purchase | recency_days   frequency | monetary_value |
| ----------- | ------------- | ------------ | --------------------------- | ------------ | --------- | -------------- |
| 13178.0     | 2011-02-01    | true         | 15                          | 0            | 15        | 587.27625      |
| 12474.0     | 2011-05-01    | true         | 16                          | 0            | 38        | 191.59974      |
| 13069.0     | 2011-09-01    | true         | 16                          | 0            | 36        | 217.35622      |
| 12474.0     | 2011-10-01    | true         | 21                          | 0            | 49        | 217.67540      |
| 12395.0     | 2010-11-01    | false        | 8                           | 2            | 2         | 567.51333      |
| 12588.0     | 2010-09-01    | false        | 6                           | 119          | 1         | 154.55000      |
| 12808.0     | 2010-10-01    | false        | 2                           | 0            | 1         | 146.52500      |
| 13388.0     | 2010-11-01    | false        | 8                           | 199          | 0         | 508.55000      |
| 13245.0     | 2011-07-01    | false        | 10                          | 270          | 0         | 558.72000      |
| 12527.0     | 2011-02-01    | true         | 9                           | 0            | 3         | 167.93000      |



# Data Quality Report — 

## 1. Overview

### Objective

This report evaluates the data quality of UCI online retail  dataset across key dimensions and outlines recommended remediation strategies to
prepare the data for downstream analytics and modeling.

### Scope

* Source: UCI online retail dataset
* Tooling:

  * dbt (data quality models)
  * PostgreSQL (data storage)
  * Python (data extraction)
  * Excel (dashboard visualization)

---

## 2. Data Quality Framework

The analysis is structured across four core dimensions:

| Dimension    | Description                                      |
| ------------ | ------------------------------------------------ |
| Completeness | Presence of required data (null/missing values)  |
| Validity     | Conformance to expected formats and value ranges |
| Uniqueness   | Absence of duplicate records                     |
| Consistency  | Logical coherence across fields                  |

---

## 3. Summary of Findings

### Key Insights

* Completeness issues are primarily driven by missing `Description`
* Uniqueness issues indicate significant duplicate transactions
* Validity issues are relatively low but present in pricing fields
* Consistency issues exist in transactional logic



## 4. Detailed Findings & Remediation

---

### 4.1 Completeness

#### Issue: Missing `description`

* **Description:** Records exist without a product decription
* **Impact:** no impact since it is not analytically critical

#### Handling strategy:
Removing rows where 'description` is invalid leads to loss of data but the column 'description' is not 
analytically critical in CLV modeling, Retention analysis and Churn Prediction. So the entire column is getting
dropped.

---

### 4.2 Validity

#### Issue: Invalid 'description' and 'country'
* **Description:** Records exist with invalid
* **Impact:** no impact since it is not analytically critical

* **Country:** Records exist with invalid countries according to my data_quality models
* **Impact:** geography is not really critical for analytics but it does add context,

#### Recommended Handling:
'description' column will be dropped but invalid country columns will remain flagged


### 4.3 Uniqueness

#### Issue: Duplicate Transactions

* **Description:** Duplicate combinations of (`invoice_no`, `stock_code`)
* **Impact:** Double-counting of sales metrics

#### Recommended Handling:

* Deduplicate using window functions



### 4.4 Consistency

#### Issue: Inconsistent Transaction Logic

* **Description:** Mismatch between quantity and price logic
* **Impact:** Leads to incorrect derived metrics

#### Recommended Handling:

for rows with quantity > 0 but unit_price < 0 will be drop since they are like 1% - 2% of the data
fro rows with quantity < 0, unit_price will be set to 0 since returns don't have negative prices

---

## 6. Data Cleaning Strategy

### Proposed Pipeline (dbt)

```text
raw → staging (cleaning) → intermediate → marts (analytics-ready)
```

### Steps:

1. Drop column 'description'
2. Deduplicate transactions
3. Standardize formats and data types
4. for rows with quantity > 0 but unit_price < 0 will be drop since they are like 1% - 2% of the data
5. a new row for revenue will be created
6. run post-cleaning data quality checks
---

##  Risks & Limitations

* Some anomalies may reflect real business scenarios (e.g., refunds)
* Dropping records may reduce dataset completeness
* Assumptions may need validation with domain experts

---


## Author

**William Kuach Aleu**

* GitHub: https://github.com/kuach-byte

---
