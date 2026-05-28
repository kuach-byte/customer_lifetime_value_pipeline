WITH ordered AS (
    SELECT
        customer_id,
        snapshot_date,
        LAG(snapshot_date) OVER (
            PARTITION BY customer_id
            ORDER BY snapshot_date
        ) AS prev_date
    FROM {{ ref('fct_composite_grain') }}
)

SELECT *
FROM ordered
WHERE prev_date IS NOT NULL
  AND snapshot_date != prev_date + INTERVAL '1 month'