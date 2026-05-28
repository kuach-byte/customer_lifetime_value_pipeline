{{ config(
    materialized='table'
) }}

WITH base AS (

    SELECT * FROM {{ ref('int_retail_deduplicated') }}

),

enriched AS (

    SELECT
        *,
        quantity * unit_price AS revenue
    FROM base

)

SELECT *
FROM enriched