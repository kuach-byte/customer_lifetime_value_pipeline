SELECT 1
FROM (
    SELECT COUNT(*) AS cnt
    FROM {{ ref('dq1_summary') }}
    WHERE dimension = 'consistency'
) t
WHERE cnt != 2