SELECT 1
FROM (
    SELECT COUNT(DISTINCT dimension) AS cnt
    FROM {{ ref('dq1_summary') }}
) t
WHERE cnt != 5