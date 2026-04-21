{{ config(
    materialized='table',
    indexes=[
        {'columns': ['month_start'], 'unique': True}
    ]
) }}

WITH date_spine AS (

    SELECT 
        date_month::date AS month_start
    FROM (
        {{ dbt_utils.date_spine(
            datepart="month",
            start_date="'2009-12-01'::date",
            end_date="'2012-01-01'::date"
        ) }}
    ) t

)

SELECT 
    month_start,

    -- Stable deterministic index
    (EXTRACT(YEAR FROM month_start) * 12 + EXTRACT(MONTH FROM month_start)) 
        AS month_index,

    EXTRACT(YEAR FROM month_start) AS year,
    EXTRACT(MONTH FROM month_start) AS month_num

FROM date_spine