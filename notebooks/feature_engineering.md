# Feature Engineering

## Overview

Customer Lifetime Value (CLV) prediction depends on transforming raw transactional records into meaningful customer-level behavioural features. Rather than training directly on invoices, this project constructs a **single customer snapshot** at a fixed observation date (`2011-04-01`), summarising each customer's historical purchasing behaviour while reserving the following 180 days as the prediction horizon.

This snapshot-based approach mirrors real-world forecasting, where only information available at the prediction date can be used. Every engineered feature is derived exclusively from transactions occurring before the snapshot date, ensuring that no future information leaks into model training.

---

## Feature Engineering Workflow

The customer state table is generated through six sequential stages:

1. Aggregate historical purchasing behaviour.
2. Compute recent customer activity.
3. Derive behavioural indicators.
4. Assign customer country.
5. Generate future target variables.
6. Assemble the final customer state table.

The resulting dataset contains one record per customer and serves as the foundation for clustering, model benchmarking, production model training, and Power BI reporting.

---

## Historical Customer Features

Historical features summarise each customer's purchasing history before the snapshot date.

### Purchase Frequency

| Feature            | Description                                   |
| ------------------ | --------------------------------------------- |
| `frequency`        | Total number of historical purchases.         |
| `repeat_frequency` | Number of repeat purchases (`frequency - 1`). |

These variables capture purchasing intensity and customer engagement.

---

### Monetary Behaviour

| Feature           | Description                         |
| ----------------- | ------------------------------------ |
| `total_revenue`   | Total historical customer revenue.  |
| `avg_order_value` | Mean order value across purchases.  |
| `std_order_value` | Standard deviation of order values. |

These features describe both customer value and spending consistency.

---

### Customer Lifecycle

| Feature        | Description                                     |
| -------------- | ------------------------------------------------ |
| `tenure_days`  | Days since the customer's first purchase.       |
| `recency_days` | Days since the customer's most recent purchase. |

Together they distinguish long-standing customers from newly acquired or inactive customers.

---

### Purchase Timing

Customer purchasing cadence is represented using inter-purchase intervals.

| Feature        | Description                               |
| -------------- | ------------------------------------------ |
| `avg_gap_days` | Average number of days between purchases. |
| `std_gap_days` | Variability in purchase intervals.        |
| `max_gap_days` | Longest historical purchase gap.          |

These features capture purchasing regularity and customer stability.

---

### Product Breadth

| Feature                 | Description                                  |
| ------------------------ | --------------------------------------------- |
| `unique_products_total` | Total unique products purchased.             |
| `avg_unique_products`   | Average number of unique products per order. |

These variables approximate purchasing diversity and customer breadth.

---

## Recent Activity Features

Recent purchasing behaviour often provides stronger predictive power than lifetime aggregates.

Using the 30 days preceding the snapshot, two additional features are created:

| Feature            | Description                                    |
| ------------------ | ------------------------------------------------ |
| `orders_last_30d`  | Orders placed during the previous 30 days.     |
| `revenue_last_30d` | Revenue generated during the previous 30 days. |

These variables help distinguish actively engaged customers from customers whose purchasing behaviour is declining.

---

## Behavioural Features

Two customer flags and one stability metric are derived from historical behaviour.

| Feature              | Description                                            |
| --------------------- | -------------------------------------------------------- |
| `is_new`             | Customer tenure is 30 days or less.                    |
| `is_dormant`         | No purchase for more than 90 days before the snapshot. |
| `revenue_volatility` | Relative variability of customer spending.             |

An additional metric, `purchase_regularity`, was initially engineered to measure consistency in purchase intervals. During clustering experiments it exhibited extreme skew and limited discriminative value and was therefore excluded from the final modelling pipeline while retained for reproducibility.

---

## Customer Attribute

| Feature   | Description                                |
| --------- | -------------------------------------------- |
| `country` | Most frequently observed customer country. |

The modal country is used to represent each customer's geographic location.

---

## Future Target Variables

Target variables are constructed exclusively from transactions occurring during the 180-day holdout period following the snapshot date.

| Target                   | Description                                               |
| ------------------------- | ------------------------------------------------------------ |
| `future_purchase_count`  | Number of purchases in the prediction window.             |
| `future_revenue`         | Total revenue generated in the prediction window.         |
| `future_avg_order_value` | Average order value during the prediction window.         |
| `had_activity`           | Binary indicator of whether the customer purchased again. |

These variables are used only during model training and evaluation and are never available as model inputs.

---

## Feature Usage

Not every engineered feature is used by every model.

* **Customer Clustering:** 17 behavioural features are used to segment customers into homogeneous groups.
* **LightGBM Models:** 17 predictive features are used to train the production hurdle model.
* **Log Transformation:** Highly right-skewed numerical variables undergo a `log1p` transformation before scaling.
* **Standardisation:** Continuous features are standardised using `StandardScaler` before clustering and model training.

This separation allows each modelling stage to use the most appropriate representation of customer behaviour while maintaining a consistent customer snapshot.

---

## Data Leakage Prevention

Preventing target leakage is essential for realistic CLV prediction.

All predictive features are engineered exclusively from transactions that occurred before the snapshot date. Future outcomes are generated only after feature engineering is complete and are stored separately as training targets.

The following columns are explicitly excluded from every predictive feature set:

* `future_revenue`
* `future_purchase_count`
* `future_avg_order_value`
* `had_activity`

An automated validation step confirms that no target variable enters the model feature matrix, ensuring that evaluation reflects genuine predictive performance rather than information leakage.

---

## Final Output

The completed feature engineering process produces a single customer state table containing one row per customer with historical behavioural features, customer attributes, and future target variables.

This state table forms the foundation for:

* Customer clustering
* Model benchmarking
* Production CLV prediction
* Customer persona assignment
* Power BI data export
