{{ config(
    materialized='table'
) }}

WITH base AS (

    SELECT * FROM {{ ref('stg_retail_cleaned') }}

),

ranked AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY invoice_no, stock_code 
            ORDER BY invoice_date DESC
        ) AS rn
    FROM base

)

SELECT *
FROM ranked
WHERE rn = 1