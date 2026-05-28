SELECT *
FROM {{ ref('fct_composite_grain') }}
WHERE snapshot_date > CURRENT_DATE