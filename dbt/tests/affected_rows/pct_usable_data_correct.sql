SELECT *
FROM {{ ref('dq1_usable_data_summary') }}
WHERE 
    ROUND(100.0 * clean_rows / NULLIF(total_rows, 0), 2)
    != pct_usable_data