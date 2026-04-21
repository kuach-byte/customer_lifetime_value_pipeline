-- tests/test_snapshot_monotonicity.sql

SELECT *
FROM (
    SELECT
        customer_id,
        snapshot_date,
        LAG(snapshot_date) OVER (
            PARTITION BY customer_id
            ORDER BY snapshot_date
        ) AS prev_snapshot
    FROM {{ ref('fct_customer_rfm') }}
) t
WHERE prev_snapshot IS NOT NULL
  AND snapshot_date <= prev_snapshot