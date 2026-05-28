WITH bounds AS (

    SELECT
        MIN(month_start) AS min_month,
        MAX(month_start) AS max_month
    FROM {{ ref('dim_date_months') }}

)

SELECT *
FROM bounds
WHERE min_month != '2009-12-01'::date
   OR max_month != '2011-12-01'::date