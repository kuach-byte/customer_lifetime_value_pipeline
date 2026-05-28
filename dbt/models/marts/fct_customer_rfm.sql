{{ config(
    materialized='incremental',
    unique_key=['customer_id', 'snapshot_date']
) }}

WITH base AS (

    SELECT
        customer_id,
        snapshot_date,
        had_activity,
        months_since_first_purchase
    FROM {{ ref('fct_composite_grain') }}

    {% if is_incremental() %}

    WHERE snapshot_date >= (
        SELECT COALESCE(MAX(snapshot_date), '1900-01-01') - INTERVAL '1 month'
        FROM {{ this }}
    )

    {% endif %}

),

transactions AS (

    SELECT
        customer_id,
        invoice_date,
        invoice_no,
        revenue
    FROM {{ ref('int_retail_filtered') }}
    WHERE customer_id IS NOT NULL

),

--  Pre-aggregate transactions at month level
transactions_monthly AS (

    SELECT
        customer_id,
        DATE_TRUNC('month', invoice_date)::date AS txn_month,
        COUNT(DISTINCT invoice_no) AS txn_count,
        SUM(revenue) AS total_revenue,
        MAX(invoice_date) AS last_invoice_date
    FROM transactions
    GROUP BY customer_id, DATE_TRUNC('month', invoice_date)

),

rfm_aggregates AS (

    SELECT
        b.customer_id,
        b.snapshot_date,

        -- RECENCY
        CASE 
            WHEN b.had_activity THEN 0
            WHEN MAX(tm.last_invoice_date) IS NOT NULL THEN
                DATE_PART('day', b.snapshot_date - MAX(tm.last_invoice_date))
            ELSE NULL
        END AS recency_days,

        -- FREQUENCY (cumulative)
        GREATEST(SUM(tm.txn_count) - 1, 0) AS frequency,

        -- MONETARY
        CASE 
            WHEN SUM(tm.txn_count) > 0 THEN
                SUM(tm.total_revenue) / SUM(tm.txn_count)
            ELSE NULL
        END AS monetary_value

    FROM base b

    LEFT JOIN transactions_monthly tm
        ON b.customer_id = tm.customer_id
       AND tm.txn_month <= b.snapshot_date   -- still safe, but much smaller now

    GROUP BY
        b.customer_id,
        b.snapshot_date,
        b.had_activity

),

final AS (

    SELECT
        b.customer_id,
        b.snapshot_date,

        -- from composite grain
        b.had_activity,
        b.months_since_first_purchase,

        -- RFM
        r.recency_days,
        r.frequency,
        r.monetary_value

    FROM base b
    LEFT JOIN rfm_aggregates r
      ON b.customer_id = r.customer_id
     AND b.snapshot_date = r.snapshot_date

)

SELECT *
FROM final