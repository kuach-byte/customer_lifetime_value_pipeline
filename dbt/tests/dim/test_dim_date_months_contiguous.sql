WITH ordered AS (

    SELECT
        month_start,
        LAG(month_start) OVER (ORDER BY month_start) AS prev_month
    FROM {{ ref('dim_date_months') }}

)

SELECT *
FROM ordered
WHERE prev_month IS NOT NULL
  AND month_start != (prev_month + INTERVAL '1 month')