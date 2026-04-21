{{ config(
    materialized='incremental',
    unique_key=['customer_id', 'snapshot_date']
) }}

WITH customer_base AS (

    SELECT
        customer_id,
        DATE_TRUNC('month', MIN(invoice_date))::date AS first_month,
        DATE_TRUNC('month', MAX(invoice_date))::date AS last_month
    FROM {{ ref('int_retail_filtered') }}
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id

),

-- Only process new months when incremental
date_filtered AS (

    SELECT *
    FROM {{ ref('dim_date_months') }}

    {% if is_incremental() %}

    WHERE month_start >= (
        SELECT COALESCE(MAX(snapshot_date), '1900-01-01') - INTERVAL '1 month'
        FROM {{ this }}
    )

    {% endif %}

),

customer_snapshot AS (

    SELECT
        c.customer_id,
        d.month_start AS snapshot_date,
        c.first_month,
        d.month_index AS snapshot_month_index

    FROM customer_base c

    JOIN date_filtered d
      ON d.month_start BETWEEN c.first_month AND c.last_month

),

first_month_index AS (

    SELECT
        c.customer_id,
        d.month_index AS first_month_index
    FROM customer_base c
    JOIN {{ ref('dim_date_months') }} d
      ON d.month_start = c.first_month

),

enriched_snapshot AS (

    SELECT
        cs.customer_id,
        cs.snapshot_date,

        -- Activity flag
        (am.customer_id IS NOT NULL) AS had_activity,

        -- Lifecycle metric
        (cs.snapshot_month_index - fmi.first_month_index + 1)
            AS months_since_first_purchase

    FROM customer_snapshot cs

    JOIN first_month_index fmi
      ON cs.customer_id = fmi.customer_id

    LEFT JOIN {{ ref('int_customer_activity_months') }} am
      ON cs.customer_id = am.customer_id
     AND am.activity_month = cs.snapshot_date

)

SELECT *
FROM enriched_snapshot