SELECT 1
FROM (
    SELECT COUNT(*) AS cnt
    FROM {{ ref('dq1_summary') }}
    WHERE dimension = 'overall'
) t
WHERE cnt != 1