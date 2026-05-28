-- tests/test_no_whitespace.sql

SELECT *
FROM {{ ref('stg_retail_cleaned') }}
WHERE 
    invoice_no != TRIM(invoice_no)
    OR stock_code != TRIM(stock_code)
    OR customer_id != TRIM(customer_id)
    OR country != TRIM(country)