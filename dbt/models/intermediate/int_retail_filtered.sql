{{ config(
    materialized='table'
) }}

WITH base AS (

    SELECT * FROM {{ ref('int_retail_enriched') }}

),

filtered AS (

    SELECT *
    FROM base
    WHERE NOT (quantity > 0 AND unit_price < 0)

)

SELECT *
FROM filtered