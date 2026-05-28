-- tests/marts/test_recency_increases.sql

WITH ordered AS (

    SELECT
        customer_id,
        snapshot_date,
        recency_days,
        LAG(recency_days) OVER (
            PARTITION BY customer_id
            ORDER BY snapshot_date
        ) AS prev_recency,
        had_activity
    FROM {{ ref('fct_customer_rfm') }}

)

SELECT *
FROM ordered
WHERE
    had_activity = FALSE
    AND prev_recency IS NOT NULL
    AND recency_days < prev_recency