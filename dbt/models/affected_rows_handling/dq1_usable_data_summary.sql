-- dq1_usable_data_summary.sql
-- Business-focused DQ summary with actionable recommendations
-- True usable data % without double-counting

{{ config(
    materialized='table',
    description='Actionable DQ summary with drop strategy recommendations'
) }}

WITH dq_flags AS (
    SELECT 
        affected_row,
        error_count
    FROM {{ ref('dq1_row_level_flags') }}
),

summary_stats AS (
    SELECT 
        COUNT(*) AS total_rows,
        
        -- Core metrics (calculated once for reuse)
        SUM(CASE WHEN affected_row = 1 THEN 1 ELSE 0 END) AS affected_rows,
        SUM(CASE WHEN affected_row = 0 THEN 1 ELSE 0 END) AS clean_rows,
        
        -- Error distribution counts
        SUM(CASE WHEN error_count = 0 THEN 1 ELSE 0 END) AS zero_errors,
        SUM(CASE WHEN error_count = 1 THEN 1 ELSE 0 END) AS one_error,
        SUM(CASE WHEN error_count BETWEEN 2 AND 3 THEN 1 ELSE 0 END) AS two_to_three_errors,
        SUM(CASE WHEN error_count >= 4 THEN 1 ELSE 0 END) AS four_plus_errors,
        
        -- Statistical metrics
        AVG(CASE WHEN affected_row = 1 THEN error_count END) AS avg_errors_per_affected_row,
        MIN(error_count) AS min_errors,
        MAX(error_count) AS max_errors
        
    FROM dq_flags
),

-- -------------------------------------------------------
-- BUSINESS METRICS (calculated once, reused)
-- -------------------------------------------------------
business_metrics AS (
    SELECT 
        *,
        
        -- Single source of truth for usable percentage
        ROUND(100.0 * clean_rows / NULLIF(total_rows, 0), 2) AS pct_usable_data,
        
        -- Percentage distribution (more useful than raw counts)
        ROUND(100.0 * one_error / NULLIF(total_rows, 0), 2) AS pct_one_error,
        ROUND(100.0 * two_to_three_errors / NULLIF(total_rows, 0), 2) AS pct_two_to_three_errors,
        ROUND(100.0 * four_plus_errors / NULLIF(total_rows, 0), 2) AS pct_four_plus_errors
        
    FROM summary_stats
)

-- -------------------------------------------------------
-- FINAL: Actionable business output
-- -------------------------------------------------------
SELECT 
    total_rows,
    affected_rows,
    clean_rows,
    
    -- Core metric (only one!)
    pct_usable_data,
    
    -- Error distribution (percentages for dashboards)
    zero_errors,
    pct_one_error,
    pct_two_to_three_errors,
    pct_four_plus_errors,
    
    -- Statistical context
    avg_errors_per_affected_row,
    min_errors,
    max_errors,
    
    -- Quality grade (using pct_usable_data directly)
    CASE 
        WHEN pct_usable_data >= 99.0 THEN 'EXCELLENT'
        WHEN pct_usable_data >= 95.0 THEN 'GOOD'
        WHEN pct_usable_data >= 90.0 THEN 'ACCEPTABLE'
        WHEN pct_usable_data >= 80.0 THEN 'POOR'
        ELSE 'UNACCEPTABLE'
    END AS data_quality_grade,
    
    -- BUSINESS DECISION SUPPORT (the BIG upgrade)
    CASE 
        WHEN pct_usable_data >= 95 THEN 'SAFE TO DROP INVALID ROWS'
        WHEN pct_usable_data >= 85 THEN 'REVIEW BEFORE DROPPING'
        WHEN pct_usable_data >= 70 THEN 'SEGMENT - Keep clean subset only'
        ELSE 'DO NOT DROP - Data loss too high'
    END AS drop_strategy_recommendation,
    
    -- Actionable insight for stakeholders
    CASE 
        WHEN pct_usable_data >= 95 
            THEN CONCAT('Safe to filter out ', affected_rows, ' invalid rows (', 
                        ROUND(100.0 - pct_usable_data, 1), '% loss)')
        WHEN pct_usable_data >= 85 
            THEN CONCAT('Review ', affected_rows, ' affected rows before dropping (', 
                        ROUND(100.0 - pct_usable_data, 1), '% loss)')
        ELSE CONCAT('Critical: Only ', pct_usable_data, '% usable - Investigate root causes')
    END AS actionable_insight,
    
    CURRENT_TIMESTAMP AS computed_at
    
FROM business_metrics