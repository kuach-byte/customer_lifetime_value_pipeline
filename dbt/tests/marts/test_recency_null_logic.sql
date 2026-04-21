-- tests/marts/test_recency_null_logic.sql

SELECT *
FROM {{ ref('fct_customer_rfm') }}
WHERE recency_days IS NULL
  AND had_activity = FALSE