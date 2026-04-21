{{ config(
    materialized='view',
) }}

WITH source AS (

    SELECT *
    FROM public.online_retail_raw

),

standardized AS (

    SELECT
        TRIM(invoice_no) AS invoice_no,
        TRIM(stock_code) AS stock_code,
        CAST(quantity AS INTEGER) AS quantity,
        CAST(unit_price AS NUMERIC(10,2)) AS unit_price,
        CAST(invoice_date AS TIMESTAMP) AS invoice_date,
        TRIM(customer_id) AS customer_id,
        TRIM(country) AS country
    FROM source

),

flagged AS (

    SELECT
        *,
        CASE
            WHEN country !~ '^[A-Za-z ]+$' THEN 1
            WHEN country NOT IN (
                'United Kingdom','France','EIRE','Germany','Netherlands','Spain',
                'Belgium','Switzerland','Portugal','Australia','Norway','Italy',
                'Channel Islands','Finland','Cyprus','Sweden','Austria','Denmark',
                'Japan','Poland','USA','Singapore','Iceland','Canada','Greece',
                'Israel','Lithuania','Malta','RSA','European Community',
                'United Arab Emirates','Saudi Arabia','Czech Republic','Lebanon',
                'Brazil','Bahrain'
            )
            THEN 1
            ELSE 0
        END AS country_invalid
    FROM standardized   

)

SELECT *
FROM flagged