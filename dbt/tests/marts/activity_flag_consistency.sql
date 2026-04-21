-- tests/marts/activity_flag_consistency.sql

SELECT f.*
FROM {{ ref('fct_composite_grain') }} f
LEFT JOIN {{ ref('int_customer_activity_months') }} a
  ON f.customer_id = a.customer_id
 AND f.snapshot_date = a.activity_month
WHERE
    (f.had_activity = TRUE AND a.customer_id IS NULL)
 OR (f.had_activity = FALSE AND a.customer_id IS NOT NULL)