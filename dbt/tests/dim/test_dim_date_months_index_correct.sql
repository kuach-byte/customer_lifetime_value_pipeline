SELECT *
FROM {{ ref('dim_date_months') }}
WHERE month_index != (
    (EXTRACT(YEAR FROM month_start) * 12 + EXTRACT(MONTH FROM month_start))
)