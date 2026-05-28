WITH base AS (

    SELECT *
    FROM {{ ref('int_retail_deduplicated') }}

),

-- 1. NOT NULL violations
not_null_violations AS (

    SELECT 
        *,
        'not_null'::text AS failure_type
    FROM base
    WHERE 
        invoice_no IS NULL
        OR stock_code IS NULL
        OR quantity IS NULL
        OR unit_price IS NULL
        OR invoice_date IS NULL
        OR country IS NULL
        OR country_invalid IS NULL
        OR rn IS NULL

),

-- 2. Accepted values violations
accepted_values_violations AS (

    SELECT 
        *,
        'accepted_values'::text AS failure_type
    FROM base
    WHERE 
        country_invalid NOT IN (0, 1)
        OR rn != 1

),

-- 3. Uniqueness violations
uniqueness_violations AS (

    SELECT
        invoice_no,
        stock_code,
        NULL::integer AS quantity,
        NULL::numeric AS unit_price,
        NULL::timestamp AS invoice_date,
        NULL::text AS customer_id,
        NULL::text AS country,
        NULL::integer AS country_invalid,
        NULL::integer AS rn,
        'duplicate_key'::text AS failure_type
    FROM (

        SELECT
            invoice_no,
            stock_code
        FROM base
        GROUP BY invoice_no, stock_code
        HAVING COUNT(*) > 1

    ) dupes

),

-- Combine all failures
final AS (

    SELECT * FROM not_null_violations

    UNION ALL

    SELECT * FROM accepted_values_violations

    UNION ALL

    SELECT * FROM uniqueness_violations

)

SELECT *
FROM final