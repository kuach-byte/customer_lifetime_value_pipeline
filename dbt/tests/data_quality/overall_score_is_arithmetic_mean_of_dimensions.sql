WITH dim_scores AS (
    SELECT AVG(rate) AS avg_rate
    FROM {{ ref('dq1_summary') }}
    WHERE dimension IN ('completeness','validity','uniqueness','consistency')
),
overall AS (
    SELECT rate AS overall_rate
    FROM {{ ref('dq1_summary') }}
    WHERE dimension = 'overall'
)

SELECT *
FROM dim_scores d
JOIN overall o ON 1=1
WHERE ABS(d.avg_rate - o.overall_rate) > 0.0001