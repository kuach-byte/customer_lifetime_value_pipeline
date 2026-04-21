WITH base AS (

    SELECT *
    FROM {{ ref('int_retail_enriched') }}

),

-- 1. NOT NULL violations (revenue)
not_null_violations AS (

    SELECT 
        *,
        'not_null_revenue' AS failure_type
    FROM base
    WHERE revenue IS NULL

),


-- 2. Expression violations (revenue correctness)
expression_violations AS (

    SELECT 
        *,
        'invalid_revenue_calculation' AS failure_type
    FROM base
    WHERE ABS(revenue - (quantity * unit_price)) > 0.0001

),

-- 3. At least one revenue present (table-level)
at_least_one_violation AS (

    SELECT 
        *,
        'no_revenue_present' AS failure_type
    FROM base
    WHERE NOT EXISTS (
        SELECT 1 FROM base WHERE revenue IS NOT NULL
    )

),

-- Combine all failures
final AS (

    SELECT * FROM not_null_violations

    UNION ALL

    SELECT * FROM expression_violations

    UNION ALL

    SELECT * FROM at_least_one_violation

)

SELECT *
FROM final