-- dq1_consistency.sql
-- Consistency checks for public.online_retail_raw
-- Rules:
--   1. If quantity > 0 then unit_price > 0
--   2. invoice_date should be in chronological order (no earlier date after a later date within same invoice_no)
--   3. If quantity < 0 then unit_price should be positive (returns don't carry negative prices)

{{ config(materialized='table') }}

WITH 
source AS (
    SELECT 
        quantity,
        unit_price
    FROM public.online_retail_raw
),

total AS (
    SELECT COUNT(*) AS n_total FROM source
),

-- -------------------------------------------------------
-- Rule 1: Sale rows (quantity > 0) must have unit_price > 0
-- -------------------------------------------------------
rule1 AS (
    SELECT
        COUNT(*) FILTER (
            WHERE quantity > 0 AND (unit_price IS NULL OR unit_price <= 0)
        ) AS sale_with_zero_price_count,

        COUNT(*) FILTER (WHERE quantity > 0) AS total_sale_rows
    FROM source
),


-- RULE 2 skipped:
-- No column exists that defines row order (e.g., created_at or ingestion timestamp),
-- so chronological validation cannot be performed reliably.
-- import_at columns does not define row order since the entire dataset was imported at 2026-04-10 14:57:11.723495

-- -------------------------------------------------------
-- Rule 3: Return rows (quantity < 0) must have unit_price > 0
-- -------------------------------------------------------
rule3 AS (
    SELECT
        COUNT(*) FILTER (
            WHERE quantity < 0 AND (unit_price IS NULL OR unit_price <= 0)
        ) AS return_with_nonpositive_price_count,

        COUNT(*) FILTER (WHERE quantity < 0) AS total_return_rows
    FROM source
),

-- -------------------------------------------------------
-- Combine into a single summary row
-- -------------------------------------------------------
combined AS (
    SELECT
        t.n_total,

        -- Rule 1
        r1.sale_with_zero_price_count,
        r1.total_sale_rows,
        ROUND(
            1.0 - r1.sale_with_zero_price_count::NUMERIC / NULLIF(r1.total_sale_rows, 0)
        , 4) AS sales_price_consistency_rate,
        -- Rule 3
        r3.return_with_nonpositive_price_count,
        r3.total_return_rows,
        ROUND(
            1.0 - r3.return_with_nonpositive_price_count::NUMERIC / NULLIF(r3.total_return_rows, 0)
        , 4) AS return_price_consistency_rate

    FROM total t
    CROSS JOIN rule1 r1
    CROSS JOIN rule3 r3
)

SELECT
    n_total,

    -- Rule 1: positive quantity → positive price
    sale_with_zero_price_count,
    total_sale_rows,
    sales_price_consistency_rate,

    -- Rule 3: returns → positive price
    return_with_nonpositive_price_count,
    total_return_rows,
    return_price_consistency_rate,

    -- Metadata
    'dq1_consistency' AS dq_model,
    CURRENT_TIMESTAMP AS run_at

FROM combined
