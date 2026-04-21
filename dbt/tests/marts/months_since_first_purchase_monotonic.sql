WITH ordered AS (
    SELECT
        customer_id,
        snapshot_date,
        months_since_first_purchase,
        LAG(months_since_first_purchase) OVER (
            PARTITION BY customer_id
            ORDER BY snapshot_date
        ) AS prev_value
    FROM {{ ref('fct_composite_grain') }}
)

SELECT *
FROM ordered
WHERE prev_value IS NOT NULL
  AND months_since_first_purchase <= prev_value