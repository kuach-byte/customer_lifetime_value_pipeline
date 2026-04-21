-- Ensure no activity is assigned to a future month beyond actual transactions

WITH max_tx AS (
    SELECT
        customer_id,
        MAX(DATE_TRUNC('month', invoice_date))::date AS max_activity_month
    FROM {{ ref('int_retail_filtered') }}
    GROUP BY customer_id
)

SELECT
    am.customer_id,
    am.activity_month,
    m.max_activity_month
FROM {{ ref('int_customer_activity_months') }} am
JOIN max_tx m
  ON am.customer_id = m.customer_id
WHERE am.activity_month > m.max_activity_month