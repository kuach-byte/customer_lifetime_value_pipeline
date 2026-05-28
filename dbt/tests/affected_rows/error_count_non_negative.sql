SELECT *
FROM {{ ref('dq1_row_level_flags') }}
WHERE error_count < 0