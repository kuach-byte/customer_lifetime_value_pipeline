# CLV Pipeline Notebooks — Documentation

## Overview

Five notebooks implement the full Customer Lifetime Value pipeline, from model benchmarking through cluster analysis, production training, persona assignment, and Power BI export.

`03_clv_modeling` and `04_cluster_persona` both depend only on `01_models_comparison` and `02_cluster_performance` (specifically `state_table.parquet` and `customer_cluster_assignments.parquet`). They do **not** depend on each other and can run in either order. `power_bi_exports` is the convergence point that requires outputs from all four prior notebooks.

---

## `01_models_comparison.ipynb` — Model Benchmarking

Builds a single customer snapshot and benchmarks four CLV models head-to-head.

**Input:** `public.int_retail_filtered` (PostgreSQL)

**What it does:**

1. Loads and cleans transaction data (removes cancellations `C*`, negative quantity/price).
2. Builds an order-level table per customer.
3. Constructs a **state table** at snapshot date `2011-04-01` with a 180-day holdout window. Features per customer include: frequency, recency, tenure, monetary stats, gap timing, product breadth, recent activity (last 30d), and behavioral flags (`is_new`, `is_dormant`).
4. Splits customers **60/20/20** (train/val/test) at the customer level — no customer appears in multiple sets.
5. Benchmarks four models: **LightGBM Hurdle**, **BG/NBD**, **Pareto/NBD**, **RFM heuristic**.

---

## `02_cluster_performance.ipynb` — Clustering & Per-Cluster Model Selection

Segments customers into K=5 clusters and identifies the best-performing model per cluster.

**Inputs:** `state_table.parquet`, `model_evaluation_mart.parquet`, `data_splits.json`

**What it does:**

1. Defines 18 clustering features (frequency, monetary, recency, tenure, gap timing, product breadth, recent activity, behavioral flags).
2. Pre-processing pipeline (train-only fit, applied to val/test):
   - Drops `purchase_regularity` (data artefact — extreme p90 skew).
   - Imputes NaNs with train medians.
   - Applies `log1p` to 10 right-skewed features.
   - Scales with `StandardScaler`.
3. Runs KMeans for K=2–10, selecting **K=5** by silhouette score.
4. Assigns all customers to clusters; evaluates each model's MAE/Spearman/top-decile capture **within each cluster** on the test set.
5. Records the best model per cluster.

---

## `03_clv_modeling.ipynb` — Production Model Training

Retrains the winning cluster-routed models on all customers to maximise signal.

**Inputs:** `state_table.parquet`, `customer_cluster_assignments.parquet`, `model_cluster_results.parquet`

**What it does:**

1. Reads `CLUSTER_MODEL_MAP` from `model_cluster_results` (best model per cluster by MAE).
2. Builds `all_customers`: state table features + cluster assignment + target columns.
3. Applies the same boolean cast → median imputation → log1p → StandardScaler sequence as `02_cluster_performance`, but **not identical inputs**: since this stage has no train/val/test split, imputation uses medians computed over the full customer population (`all_customers[LGBM_FEATURES].median()`), not the train-only medians used in `02_cluster_performance`. Saves `scaler_final.pkl`.
4. Trains a **LightGBM Hurdle Model** (3 sub-models) on **all customers, across every cluster** — there is no persona- or cluster-based exclusion at training time:
   - **Activation classifier** — predicts `had_activity` (all customers).
   - **Frequency regressor** — predicts `future_purchase_count` (active customers only).
   - **Monetary regressor** — predicts `future_avg_order_value` (active customers only).
   A separate **RFM heuristic** (`frequency_raw × avg_order_value_raw`) is also computed for **all customers** — it isn't a "trained" model, just a formula applied uniformly.

   `predicted_revenue` for each customer is then chosen via cluster-level routing: for each cluster, whichever model (LightGBM or RFM) had the lowest test-set MAE in `02_cluster_performance` is the one whose prediction is used. Per the notebook's own output, the routing map is:

   | Cluster | Persona (per `04_cluster_persona`) | Model routed |
   |---|---|---|
   | 0 | Emerging Customers | **RFM** |
   | 1 | Established Customers | LightGBM |
   | 2 | At-risk Customers | LightGBM |
   | 3 | Premium Customers | LightGBM |
   | 4 | Lapsed One-Time Buyers | LightGBM |

   So Cluster 0 (Emerging Customers) is the one routed to RFM, not Premium Customers — Premium Customers' `predicted_revenue` comes from LightGBM like every other cluster except Cluster 0.
5. Combines sub-model outputs into `predicted_revenue` and discounts to `discounted_clv` (annual rate: 10%).
6. Assigns `clv_tier` (`Very Low` / `Low` / `Medium` / `High` / `VIP`).

---

## `04_cluster_persona.ipynb` — Cluster Persona Assignment

Translates cluster IDs into named business personas with future CLV validation.

**Inputs:** `state_table.parquet`, `customer_cluster_assignments.parquet`

**What it does:**

1. Profiles each cluster across 7 RFM features (`recency_days`, `frequency`, `total_revenue`, `avg_order_value`, `tenure_days`, `avg_unique_products`, `revenue_volatility`).
2. Computes an **index table** (cluster median ÷ population median × 100) to identify the top 4 defining traits per cluster.
3. Assigns personas:

| Cluster | Persona | Key trait |
|---|---|---|
| 0 | Emerging Customers | Lowest tenure; revenue low due to short history |
| 1 | Established Customers | High frequency/revenue but fading engagement, majority dormant |
| 2 | At-risk Customers | Long tenure, below-average revenue, chronic low engagement |
| 3 | Premium Customers | Highest revenue/frequency, most recently active, zero dormancy |
| 4 | Lapsed One-Time Buyers | Largest segment; single-order, longest gap since purchase |

4. Validates personas against actual 180-day future revenue lift vs. population average.

**Output saved to `saved_analysis_states/`:**

| File | Content |
|---|---|
| `cluster_persona_summary.parquet` / `.csv` | `cluster`, `persona_name`, `description`, `n_customers`, `pct_of_total`, `top_traits_index` |
| `persona_future_revenue_lift.png` | Bar chart: future revenue lift by persona |

---

## `power_bi_exports.ipynb` — Power BI Data Model Export

Assembles, validates, and writes the four-table Power BI data model to PostgreSQL.

**Inputs:** All `saved_analysis_states/` parquets from prior notebooks.

**What it does:**

1. Builds four tables:

| Table | Grain | Key columns |
|---|---|---|
| `fact_customers` | 1 row per customer | `customer_id`, `country`, `cluster`, `persona_name`, `model_used`, `predicted_revenue`, `discounted_clv`, `clv_tier`, `p_active`, `risk_level` |
| `dim_cluster_persona` | 1 row per cluster | `persona_name`, `description`, `n_customers`, `avg_frequency`, `avg_order_value`, `avg_total_revenue`, `avg_recency_days`, `avg_tenure_days`, `avg_discounted_clv` |
| `fact_model_performance` | 1 row per cluster × model | `mae`, `rmse`, `spearman_corr`, `top_decile_capture` |
| `dim_model_routing` | 1 row per cluster | `model_used`, predicted revenue stats |

2. Derives `risk_level` from `p_active`:

| `p_active` | `risk_level` |
|---|---|
| ≥ 0.66 | Low |
| 0.33 – 0.65 | Medium |
| < 0.33 | High |

3. Runs a 5-gate pre-write validation (row counts, nulls, foreign key consistency on `cluster`, category integrity for `clv_tier`/`risk_level`, duplicate grain check).
4. Writes all four tables to `powerbi` schema in `replace` mode.

**Known caveat:** `fact_model_performance.spearman_corr` has 9 expected nulls where cluster-model slices had fewer than 3 customers. Handle with `COALESCE` in Power BI rather than filtering rows.

**Intentionally excluded from Python** (built as DAX measures in Power BI): `recommended_action`, `revenue_at_risk`, `VIP_customer_flag`.

**Power BI relationships:**
```
fact_customers.cluster  →  dim_cluster_persona.cluster     (many-to-one)
fact_customers.cluster  →  fact_model_performance.cluster  (many-to-one)
fact_customers.cluster  →  dim_model_routing.cluster       (many-to-one)
```