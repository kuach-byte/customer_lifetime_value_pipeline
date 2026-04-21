{{ config(materialized='table') }}

SELECT DISTINCT
    customer_id,
    DATE_TRUNC('month', invoice_date)::date AS activity_month
FROM {{ ref('int_retail_filtered') }}
WHERE customer_id IS NOT NULL