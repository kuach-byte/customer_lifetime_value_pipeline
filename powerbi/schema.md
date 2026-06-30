



# PowerBI Schema Documentation

[ER Diagram →](assets/powerbi_er_diagram.png)

---


## Tables

### `dim_cluster_persona`
Central dimension table. One row per customer cluster.

| Column | Type | Notes |
|---|---|---|
| `cluster` | int | PK |
| `persona_name` | string | |
| `description` | string | |
| `n_customers` | int | |
| `pct_of_total` | float | |
| `avg_frequency` | float | |
| `avg_order_value` | float | |
| `avg_total_revenue` | float | |
| `avg_recency_days` | float | |
| `avg_tenure_days` | float | |
| `avg_discounted_clv` | float | |

---

### `fact_customers`
One row per customer. Contains CLV predictions, segmentation, and risk metrics.

| Column | Type | Notes |
|---|---|---|
| `customer_id` | string | PK |
| `country` | string | |
| `cluster` | int | FK → `dim_cluster_persona` |
| `persona_name` | string | |
| `model_used` | string | |
| `predicted_revenue` | float | |
| `discounted_clv` | float | |
| `raw_clv` | float | |
| `discount_amount` | float | |
| `discount_rate` | float | |
| `freq_conditional` | float | |
| `aov_conditional` | float | |
| `p_active` | float | |
| `clv_tier` | string | Very Low / Low / Medium / High / VIP |
| `risk_level` | string | Low / Medium / High |

---

### `fact_model_performance`
One row per cluster × model. Stores model evaluation metrics (~20 rows).

| Column | Type | Notes |
|---|---|---|
| `cluster` | int | PK, FK → `dim_cluster_persona` |
| `model_used` | string | PK |
| `n_customers` | int | |
| `mae` | float | |
| `rmse` | float | |
| `top_decile_capture` | float | |
| `spearman_corr` | float | Nullable when sample too small |

---

### `dim_model_routing`
One row per cluster × model. Shows which model was assigned to each cluster.

| Column | Type | Notes |
|---|---|---|
| `cluster` | int | PK, FK → `dim_cluster_persona` |
| `model_used` | string | PK |
| `n_customers` | int | |
| `mean_predicted_revenue` | float | |
| `median_predicted_revenue` | float | |
| `min_predicted_revenue` | float | |
| `max_predicted_revenue` | float | |

---

## Relationships

| Child table | FK | Parent table |
|---|---|---|
| `fact_customers` | `cluster` | `dim_cluster_persona` |
| `fact_model_performance` | `cluster` | `dim_cluster_persona` |
| `dim_model_routing` | `cluster` | `dim_cluster_persona` |
