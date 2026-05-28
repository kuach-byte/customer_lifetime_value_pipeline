WITH base AS (

    SELECT *
    FROM {{ ref('int_retail_filtered') }}

),

-- 1. Logical inconsistency violations
invalid_transaction_logic AS (

    SELECT
        *,
        'invalid_transaction_logic' AS failure_type
    FROM base
    WHERE quantity > 0 AND unit_price < 0

),

final AS (

    SELECT * FROM invalid_transaction_logic

)

SELECT *
FROM final