-- Ensure every activity_month actually exists in source transactions

SELECT
    am.customer_id,
    am.activity_month
FROM {{ ref('int_customer_activity_months') }} am

LEFT JOIN {{ ref('int_retail_filtered') }} t
    ON am.customer_id = t.customer_id
   AND DATE_TRUNC('month', t.invoice_date)::date = am.activity_month

WHERE t.customer_id IS NULL