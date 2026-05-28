-- tests/test_numeric_sanity.sql

SELECT *
FROM {{ ref('stg_retail_cleaned') }}
WHERE 
    quantity IS NULL
    OR unit_price IS NULL
    OR invoice_date IS NULL