-- tests/marts/test_activity_recency_alignment.sql

SELECT *
FROM {{ ref('fct_customer_rfm') }}
WHERE had_activity = TRUE
  AND recency_days IS DISTINCT FROM 0