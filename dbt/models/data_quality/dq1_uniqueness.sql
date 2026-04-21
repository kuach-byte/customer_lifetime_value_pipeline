-- dq1_uniqueness.sql
-- Uniqueness checks for public.online_retail_raw
-- Checks: invoice_no alone, stock_code alone, and composite (invoice_no, stock_code)

{{ config(materialized='table') }}

WITH source AS (
    SELECT
        invoice_no,
        stock_code
    FROM public.online_retail_raw
),

total AS (
    SELECT COUNT(*) AS n_total
    FROM source
    WHERE invoice_no IS NOT NULL
      AND stock_code IS NOT NULL
),


-- -------------------------------------------------------
-- Duplicate detection: composite (invoice_no, stock_code)
-- -------------------------------------------------------
composite_counts AS (
    SELECT
        invoice_no,
        stock_code,
        COUNT(*) AS occurrence_count
    FROM source
    WHERE invoice_no IS NOT NULL
      AND stock_code IS NOT NULL
    GROUP BY invoice_no, stock_code
),

composite_dupes AS (
    SELECT
        COUNT(*) AS composite_duplicate_group_count,
        COALESCE(SUM(occurrence_count) - COUNT(*), 0) AS composite_duplicate_row_count,
        COALESCE(SUM(occurrence_count), 0) AS composite_total_in_dupes
    FROM composite_counts
    WHERE occurrence_count > 1
),
-- -------------------------------------------------------
-- Uniqueness metrics into one row
-- -------------------------------------------------------
combined AS (
    SELECT
        t.n_total,

        c.composite_duplicate_group_count,
        c.composite_duplicate_row_count,
        ROUND(
            1.0 - c.composite_duplicate_row_count::NUMERIC / NULLIF(t.n_total,0)
        , 4)   AS composite_uniqueness_rate

    FROM total t
    CROSS JOIN composite_dupes   c
)


SELECT
    n_total,

    -- composite uniqueness
    composite_duplicate_group_count,
    composite_duplicate_row_count,
    composite_uniqueness_rate,

    -- Metadata
    'dq1_uniqueness' AS dq_model,
    CURRENT_TIMESTAMP AS run_at

FROM combined
