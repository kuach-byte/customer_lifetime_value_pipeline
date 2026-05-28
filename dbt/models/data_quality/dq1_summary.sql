-- dq1_summary.sql
-- Consolidated Data Quality Summary

{{ config(materialized='table') }}

WITH completeness AS (
    SELECT * FROM {{ ref('dq1_completeness') }}
),
validity AS (
    SELECT * FROM {{ ref('dq1_validity') }}
),
uniqueness AS (
    SELECT * FROM {{ ref('dq1_uniqueness') }}
),
consistency AS (
    SELECT * FROM {{ ref('dq1_consistency') }}
),

completeness_rows AS (
    SELECT n_total, 'completeness' AS dimension, 'invoice_no'   AS check_name, 'incomplete_count'   AS metric_name, invoice_no_incomplete_count::NUMERIC   AS metric_value, invoice_no_completeness_rate   AS rate, run_at FROM completeness
    UNION ALL
    SELECT n_total, 'completeness', 'stock_code',   'incomplete_count', stock_code_incomplete_count::NUMERIC,   stock_code_completeness_rate,   run_at FROM completeness
    UNION ALL
    SELECT n_total, 'completeness', 'description',  'incomplete_count', description_incomplete_count::NUMERIC,  description_completeness_rate,  run_at FROM completeness
    UNION ALL
    SELECT n_total, 'completeness', 'quantity',      'incomplete_count', quantity_incomplete_count::NUMERIC,     quantity_completeness_rate,     run_at FROM completeness
    UNION ALL
    SELECT n_total, 'completeness', 'invoice_date',  'incomplete_count', invoice_date_incomplete_count::NUMERIC, invoice_date_completeness_rate, run_at FROM completeness
    UNION ALL
    SELECT n_total, 'completeness', 'unit_price',    'incomplete_count', unit_price_incomplete_count::NUMERIC,   unit_price_completeness_rate,   run_at FROM completeness
    UNION ALL
    SELECT n_total, 'completeness', 'customer_id',   'incomplete_count', customer_id_incomplete_count::NUMERIC,  customer_id_completeness_rate,  run_at FROM completeness
    UNION ALL
    SELECT n_total, 'completeness', 'country',       'incomplete_count', country_incomplete_count::NUMERIC,      country_completeness_rate,      run_at FROM completeness
    UNION ALL
    SELECT n_total, 'completeness', 'ALL_COLUMNS',   'fully_complete_row_count', fully_complete_row_count::NUMERIC,  fully_complete_row_rate,       run_at FROM completeness
    UNION ALL
    SELECT n_total, 'completeness', 'ALL_COLUMNS',   'incomplete_row_count',     incomplete_row_count::NUMERIC,      1.0 - fully_complete_row_rate, run_at FROM completeness
),

validity_rows AS (
    SELECT n_total, 'validity', 'country',      'invalid_count', country_invalid_count::NUMERIC,      country_validity_rate,      run_at FROM validity
    UNION ALL
    SELECT n_total, 'validity', 'stock_code',   'invalid_count', stock_code_invalid_count::NUMERIC,   stock_code_validity_rate,   run_at FROM validity
    UNION ALL
    SELECT n_total, 'validity', 'invoice_no',   'invalid_count', invoice_no_invalid_count::NUMERIC,   invoice_no_validity_rate,   run_at FROM validity
    UNION ALL
    SELECT n_total, 'validity', 'description',  'invalid_count', description_invalid_count::NUMERIC,  description_validity_rate,  run_at FROM validity
    UNION ALL
    SELECT n_total, 'validity', 'quantity',     'invalid_count', quantity_invalid_count::NUMERIC,     quantity_validity_rate,     run_at FROM validity
    UNION ALL
    SELECT n_total, 'validity', 'invoice_date', 'invalid_count', invoice_date_invalid_count::NUMERIC, invoice_date_validity_rate, run_at FROM validity
    UNION ALL
    SELECT n_total, 'validity', 'unit_price',   'invalid_count', unit_price_invalid_count::NUMERIC,   unit_price_validity_rate,   run_at FROM validity
    UNION ALL
    SELECT n_total, 'validity', 'ALL_COLUMNS',  'fully_valid_row_count', fully_valid_row_count::NUMERIC,  fully_valid_row_rate,       run_at FROM validity
    UNION ALL
    SELECT n_total, 'validity', 'ALL_COLUMNS',  'invalid_row_count',     invalid_row_count::NUMERIC,      1.0 - fully_valid_row_rate, run_at FROM validity
),

uniqueness_rows AS (
    SELECT n_total, 'uniqueness', 'invoice_no + stock_code', 'duplicate_group_count', composite_duplicate_group_count::NUMERIC, composite_uniqueness_rate, run_at FROM uniqueness
    UNION ALL
    SELECT n_total, 'uniqueness', 'invoice_no + stock_code', 'duplicate_row_count',   composite_duplicate_row_count::NUMERIC,   composite_uniqueness_rate, run_at FROM uniqueness
),

consistency_rows AS (
    SELECT n_total, 'consistency', 'quantity>0 → price>0',           'failing_row_count', sale_with_zero_price_count::NUMERIC,          sales_price_consistency_rate,  run_at FROM consistency
    UNION ALL
    SELECT n_total, 'consistency', 'quantity<0 → price>0 (returns)', 'failing_row_count', return_with_nonpositive_price_count::NUMERIC, return_price_consistency_rate, run_at FROM consistency
),

-- -------------------------------------------------------
-- Overall score: AVG across all unpivoted rates.
-- Computed directly from base tables (no CTE references
-- inside subqueries) to satisfy PostgreSQL scoping rules.
-- -------------------------------------------------------
overall_avg AS (
    SELECT ROUND(AVG(r), 4) AS avg_rate
    FROM (
        SELECT invoice_no_completeness_rate   AS r FROM completeness UNION ALL
        SELECT stock_code_completeness_rate       FROM completeness UNION ALL
        SELECT description_completeness_rate      FROM completeness UNION ALL
        SELECT quantity_completeness_rate         FROM completeness UNION ALL
        SELECT invoice_date_completeness_rate     FROM completeness UNION ALL
        SELECT unit_price_completeness_rate       FROM completeness UNION ALL
        SELECT customer_id_completeness_rate      FROM completeness UNION ALL
        SELECT country_completeness_rate          FROM completeness UNION ALL
        SELECT fully_complete_row_rate            FROM completeness UNION ALL
        SELECT 1.0 - fully_complete_row_rate      FROM completeness UNION ALL
        SELECT country_validity_rate              FROM validity UNION ALL
        SELECT stock_code_validity_rate           FROM validity UNION ALL
        SELECT invoice_no_validity_rate           FROM validity UNION ALL
        SELECT description_validity_rate          FROM validity UNION ALL
        SELECT quantity_validity_rate             FROM validity UNION ALL
        SELECT invoice_date_validity_rate         FROM validity UNION ALL
        SELECT unit_price_validity_rate           FROM validity UNION ALL
        SELECT fully_valid_row_rate               FROM validity UNION ALL
        SELECT 1.0 - fully_valid_row_rate         FROM validity UNION ALL
        SELECT composite_uniqueness_rate          FROM uniqueness UNION ALL
        SELECT composite_uniqueness_rate          FROM uniqueness UNION ALL
        SELECT sales_price_consistency_rate       FROM consistency UNION ALL
        SELECT return_price_consistency_rate      FROM consistency
    ) all_rates
),

overall_score AS (
    SELECT
        c.n_total,
        'overall'     AS dimension,
        'ALL'         AS check_name,
        'dq_score'    AS metric_name,
        NULL::NUMERIC AS metric_value,
        o.avg_rate    AS rate,
        c.run_at
    FROM (SELECT DISTINCT n_total, run_at FROM completeness) c
    CROSS JOIN overall_avg o
),

all_checks AS (
    SELECT * FROM completeness_rows
    UNION ALL
    SELECT * FROM validity_rows
    UNION ALL
    SELECT * FROM uniqueness_rows
    UNION ALL
    SELECT * FROM consistency_rows
    UNION ALL
    SELECT * FROM overall_score
)

SELECT
    dimension,
    check_name,
    metric_name,
    metric_value,
    ROUND(rate, 4) AS rate,
    n_total,
    CASE
        WHEN rate >= 0.99 THEN 'PASS'
        WHEN rate >= 0.95 THEN 'WARN'
        ELSE 'FAIL'
    END            AS status,
    run_at
FROM all_checks
ORDER BY
    dimension,
    check_name,
    metric_name