-- dq1_completeness.sql
-- Completeness checks for public.online_retail_raw
-- Acceptance criteria per column, row-level completeness, and aggregate metrics

{{ config(materialized='table') }}

with source AS (
    SELECT
        invoice_no,
        stock_code,
        description,
        quantity,
        invoice_date,
        unit_price,
        country
    FROM public.online_retail_raw
),

total AS (
    SELECT COUNT(*) AS n_total FROM source
),

-- -------------------------------------------------------
-- Column-level: flag each row as incomplete per column
-- -------------------------------------------------------
row_flags AS (
    SELECT
        -- Keys for traceability
        invoice_no,
        stock_code,

        -- invoice_no: not null, not empty
        CASE
            WHEN invoice_no IS NULL OR TRIM(invoice_no) = '' THEN 1 ELSE 0
        END AS invoice_no_incomplete,

        -- stock_code: not null, not empty
        CASE
            WHEN stock_code IS NULL OR TRIM(stock_code) = '' THEN 1 ELSE 0
        END AS stock_code_incomplete,

        -- description: not null (empty string is acceptable per spec)
        CASE
            WHEN description IS NULL THEN 1 ELSE 0
        END AS description_incomplete,

        -- quantity: not null
        CASE
            WHEN quantity IS NULL THEN 1 ELSE 0
        END AS quantity_incomplete,

        -- invoice_date: not null
        CASE
            WHEN invoice_date IS NULL THEN 1 ELSE 0
        END AS invoice_date_incomplete,

        -- unit_price: not null and >= 0
        CASE
            WHEN unit_price IS NULL THEN 1 ELSE 0
        END AS unit_price_incomplete,

        -- customer_id: nullable by spec — never flagged as incomplete
        0 AS customer_id_incomplete,

        -- country: not null, not empty
        CASE
            WHEN country IS NULL OR TRIM(country) = '' THEN 1 ELSE 0
        END AS country_incomplete

    FROM source
),

-- -------------------------------------------------------
-- Row-level completeness
-- -------------------------------------------------------
row_completeness AS (
    SELECT
        *,
        CASE
            WHEN
                invoice_no_incomplete  = 0
                AND stock_code_incomplete  = 0
                AND description_incomplete = 0
                AND quantity_incomplete     = 0
                AND invoice_date_incomplete = 0
                AND unit_price_incomplete   = 0
                AND customer_id_incomplete  = 0
                AND country_incomplete      = 0
            THEN 1
            ELSE 0
        END AS is_fully_complete
    FROM row_flags
),

-- -------------------------------------------------------
-- Column-level aggregates
-- -------------------------------------------------------
col_agg AS (
    SELECT
        n_total,

        -- Incomplete counts per column
        SUM(invoice_no_incomplete)  AS invoice_no_incomplete_count,
        SUM(stock_code_incomplete)  AS stock_code_incomplete_count,
        SUM(description_incomplete) AS description_incomplete_count,
        SUM(quantity_incomplete)    AS quantity_incomplete_count,
        SUM(invoice_date_incomplete)AS invoice_date_incomplete_count,
        SUM(unit_price_incomplete)  AS unit_price_incomplete_count,
        SUM(customer_id_incomplete) AS customer_id_incomplete_count,
        SUM(country_incomplete)     AS country_incomplete_count,

        -- Fully complete rows
        SUM(is_fully_complete)      AS fully_complete_row_count

    FROM row_completeness
    CROSS JOIN total
    GROUP BY n_total
),

-- -------------------------------------------------------
-- Final metric output (one summary row + column rates)
-- -------------------------------------------------------
metrics AS (
    SELECT
        n_total,

        -- Column completeness rates
        ROUND(1.0 - invoice_no_incomplete_count::NUMERIC  / NULLIF(n_total,0), 4) AS invoice_no_completeness_rate,
        ROUND(1.0 - stock_code_incomplete_count::NUMERIC  / NULLIF(n_total,0), 4) AS stock_code_completeness_rate,
        ROUND(1.0 - description_incomplete_count::NUMERIC / NULLIF(n_total,0), 4) AS description_completeness_rate,
        ROUND(1.0 - quantity_incomplete_count::NUMERIC    / NULLIF(n_total,0), 4) AS quantity_completeness_rate,
        ROUND(1.0 - invoice_date_incomplete_count::NUMERIC/ NULLIF(n_total,0), 4) AS invoice_date_completeness_rate,
        ROUND(1.0 - unit_price_incomplete_count::NUMERIC  / NULLIF(n_total,0), 4) AS unit_price_completeness_rate,
        1.0                                                                        AS customer_id_completeness_rate, -- always complete by spec
        ROUND(1.0 - country_incomplete_count::NUMERIC     / NULLIF(n_total,0), 4) AS country_completeness_rate,

        -- Row-level completeness
        fully_complete_row_count,
        ROUND(fully_complete_row_count::NUMERIC / NULLIF(n_total,0), 4)           AS fully_complete_row_rate,
        n_total - fully_complete_row_count                                         AS incomplete_row_count,

        -- Incomplete counts (kept for drill-down / summary join)
        invoice_no_incomplete_count,
        stock_code_incomplete_count,
        description_incomplete_count,
        quantity_incomplete_count,
        invoice_date_incomplete_count,
        unit_price_incomplete_count,
        customer_id_incomplete_count,
        country_incomplete_count,

        -- Metadata
        'dq1_completeness' AS dq_model,
        CURRENT_TIMESTAMP  AS run_at

    FROM col_agg
)

SELECT
    n_total,

    invoice_no_completeness_rate,
    stock_code_completeness_rate,
    description_completeness_rate,
    quantity_completeness_rate,
    invoice_date_completeness_rate,
    unit_price_completeness_rate,
    customer_id_completeness_rate,
    country_completeness_rate,

    fully_complete_row_count,
    fully_complete_row_rate,
    incomplete_row_count,

    invoice_no_incomplete_count,
    stock_code_incomplete_count,
    description_incomplete_count,
    quantity_incomplete_count,
    invoice_date_incomplete_count,
    unit_price_incomplete_count,
    customer_id_incomplete_count,
    country_incomplete_count,

    dq_model,
    run_at
FROM metrics