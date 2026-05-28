SELECT 1
FROM (
    SELECT COUNT(*) AS cnt
    FROM {{ ref('dq1_summary') }}
    WHERE dimension = 'validity'
) t
WHERE cnt != 9