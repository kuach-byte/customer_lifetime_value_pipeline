-- tests/test_monetary_requires_transactions.sql

SELECT *
FROM {{ ref('fct_customer_rfm') }}
WHERE monetary_value IS NOT NULL
  AND recency_days IS NULL