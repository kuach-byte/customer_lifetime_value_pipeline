SELECT *
FROM {{ ref('dq1_usable_data_summary') }}
WHERE clean_rows + affected_rows != total_rows