SELECT *
FROM {{ ref('dq1_row_level_flags') }}
WHERE 
    (affected_row = 0 AND error_count > 0)
    OR
    (affected_row = 1 AND error_count = 0)