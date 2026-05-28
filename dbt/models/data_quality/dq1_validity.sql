-- dq1_validity.sql
-- Validity checks for public.online_retail_raw
-- Each rule is flagged per row; summary metrics are computed at the bottom

{{ config(materialized='table') }}

WITH source AS (
    SELECT
        country,
        invoice_no,
        stock_code,
        description,
        quantity,
        invoice_date,
        unit_price
    FROM public.online_retail_raw
),

total AS (
    SELECT COUNT(*) AS n_total FROM source
),

-- -------------------------------------------------------
-- Constants  (edit thresholds here without touching logic)
-- -------------------------------------------------------
params AS (
    SELECT
        10000   AS max_quantity,
        99999.99 AS max_unit_price
),

-- -------------------------------------------------------
-- Row-level validity flags  (1 = invalid, 0 = valid)
-- -------------------------------------------------------
row_flags AS (
    SELECT
        s.country,
        s.stock_code,
        s.invoice_no,
        s.description,
        s.quantity,
        s.invoice_date,
        s.unit_price,

        CASE
            WHEN TRIM(s.country) !~ '^[A-Za-z ]+$' THEN 1
            WHEN s.country NOT IN (
                'United Kingdom','France','EIRE','Germany','Netherlands','Spain',
                'Belgium','Switzerland','Portugal','Australia','Norway','Italy',
                'Channel Islands','Finland','Cyprus','Sweden','Austria','Denmark',
                'Japan','Poland','USA','Singapore','Iceland','Canada','Greece',
                'Israel','Lithuania','Malta','RSA','European Community',
                'United Arab Emirates','Saudi Arabia','Czech Republic','Lebanon',
                'Brazil','Bahrain'
            )
            THEN 1
            ELSE 0
        END AS country_invalid,



        CASE
            WHEN s.stock_code IS NULL THEN 1
            WHEN TRIM(s.stock_code) !~ '^[A-Za-z0-9 ]+$' THEN 1
            ELSE 0
        END AS stock_code_invalid,
        -- invoice_no: standard retail format  C?[0-9]{6}
        --   e.g. 536365 or C536365 (cancellations)
        CASE
            WHEN s.invoice_no IS NULL
              OR TRIM(s.invoice_no) !~ '^C?[0-9]{6}$'
            THEN 1 ELSE 0
        END AS invoice_no_invalid,

        -- description: reject NULLs, strings of only special/repeated chars
        --   flags: only non-alphanumeric chars  OR  >= 5 identical chars in a row
        CASE
            WHEN s.description IS NULL                          THEN 1
            WHEN TRIM(s.description) ~ '^[^a-zA-Z0-9 ,.\-()/]+$'      THEN 1
            WHEN TRIM(s.description) ~* '(.)\1{4,}'                   THEN 1
            ELSE 0
        END AS description_invalid,
        -- quantity: must be a real value (column type handles float),
        --   > 0 for sales (negatives handled in consistency), <= max
        CASE
            WHEN s.quantity IS NULL                  THEN 1
            WHEN s.quantity = 0                      THEN 1
            WHEN s.quantity > p.max_quantity    THEN 1
            ELSE 0
        END AS quantity_invalid,

        -- invoice_date: not in the future, valid calendar date
        --   (if stored as TEXT, cast attempt is wrapped safely below)
        CASE
            WHEN s.invoice_date IS NULL              THEN 1
            WHEN s.invoice_date > CURRENT_TIMESTAMP  THEN 1
            ELSE 0
        END AS invoice_date_invalid,

        -- unit_price: >= 0, <= max, max 2 decimal places
        CASE
            WHEN s.unit_price IS NULL                                  THEN 1
            WHEN s.unit_price < 0                                      THEN 1
            WHEN s.unit_price > p.max_unit_price                       THEN 1
            WHEN ROUND(s.unit_price::NUMERIC, 2) <> s.unit_price::NUMERIC THEN 1
            ELSE 0
        END AS unit_price_invalid


    FROM source s
    CROSS JOIN params p
),

-- -------------------------------------------------------
-- Row-level validity summary
-- -------------------------------------------------------
row_validity AS (
    SELECT
        *,

        (
        country_invalid +
        stock_code_invalid +
        invoice_no_invalid +
        description_invalid +
        quantity_invalid +
        invoice_date_invalid +
        unit_price_invalid) AS invalid_flag_count,        
        CASE
            WHEN
                stock_code_invalid = 0
                AND invoice_no_invalid  = 0
                AND description_invalid  = 0
                AND quantity_invalid     = 0
                AND invoice_date_invalid = 0
                AND unit_price_invalid   = 0
            THEN 1
            ELSE 0
        END AS is_fully_valid
    FROM row_flags
),

-- -------------------------------------------------------
-- Aggregate metrics
-- -------------------------------------------------------
agg AS (
    SELECT
        t.n_total,
        SUM(country_invalid) AS country_invalid_count,
        SUM(stock_code_invalid) AS stock_code_invalid_count,
        SUM(invoice_no_invalid)  AS invoice_no_invalid_count,
        SUM(description_invalid) AS description_invalid_count,
        SUM(quantity_invalid)    AS quantity_invalid_count,
        SUM(invoice_date_invalid)AS invoice_date_invalid_count,
        SUM(unit_price_invalid)  AS unit_price_invalid_count,
        SUM(is_fully_valid)      AS fully_valid_row_count
    FROM row_validity
    CROSS JOIN total t
    GROUP BY t.n_total
)

SELECT
    n_total,

    -- Invalid counts
    country_invalid_count,
    stock_code_invalid_count,
    invoice_no_invalid_count,
    description_invalid_count,
    quantity_invalid_count,
    invoice_date_invalid_count,
    unit_price_invalid_count,

    -- Validity rates per column
    ROUND(1.0 - country_invalid_count::NUMERIC / NULLIF(n_total,0), 4) AS country_validity_rate,
    ROUND(1.0 - stock_code_invalid_count::NUMERIC / NULLIF(n_total,0), 4) AS stock_code_validity_rate,
    ROUND(1.0 - invoice_no_invalid_count::NUMERIC  / NULLIF(n_total,0), 4) AS invoice_no_validity_rate,
    ROUND(1.0 - description_invalid_count::NUMERIC / NULLIF(n_total,0), 4) AS description_validity_rate,
    ROUND(1.0 - quantity_invalid_count::NUMERIC    / NULLIF(n_total,0), 4) AS quantity_validity_rate,
    ROUND(1.0 - invoice_date_invalid_count::NUMERIC/ NULLIF(n_total,0), 4) AS invoice_date_validity_rate,
    ROUND(1.0 - unit_price_invalid_count::NUMERIC  / NULLIF(n_total,0), 4) AS unit_price_validity_rate,

    -- Row-level validity
    fully_valid_row_count,
    ROUND(fully_valid_row_count::NUMERIC / NULLIF(n_total,0), 4)           AS fully_valid_row_rate,
    n_total - fully_valid_row_count                                         AS invalid_row_count,

    -- Metadata
    'dq1_validity' AS dq_model,
    CURRENT_TIMESTAMP AS run_at

FROM agg
