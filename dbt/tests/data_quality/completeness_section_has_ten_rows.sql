SELECT 1
FROM (
    SELECT COUNT(*) AS cnt
    FROM {{ ref('dq1_summary') }}
    WHERE dimension = 'completeness'
) t
WHERE cnt != 10