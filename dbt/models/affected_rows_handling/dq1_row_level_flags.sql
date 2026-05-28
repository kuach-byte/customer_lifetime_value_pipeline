-- dq1_row_level_flags.sql
-- Row-level data quality flags combining ALL dimensions
-- Uses simple row_id instead of hash for perfect accuracy
-- Reuses upstream logic where possible, reconstructs only when necessary

{{ config(
    materialized='table',
    description='Row-level DQ flags - single source of truth for row quality'
) }}

WITH 
-- -------------------------------------------------------
-- SOURCE: Create stable row identifier ONCE
-- -------------------------------------------------------
source_enriched AS (
    SELECT 
        -- Simple, fast, unique row identifier
        ROW_NUMBER() OVER (ORDER BY invoice_date, invoice_no) AS row_id,
        
        -- Original columns
        invoice_no,
        stock_code,
        description,
        quantity,
        invoice_date,
        unit_price,
        country
        
    FROM public.online_retail_raw
),

-- -------------------------------------------------------
-- COMPLETENESS: Row-level flags
-- NOTE: Upstream dq1_completeness is aggregated only
-- Reconstructing here (but could be refactored upstream)
-- -------------------------------------------------------
completeness_flags AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY invoice_date, invoice_no) AS row_id,
        
        -- Completeness flags (1 = incomplete/missing)
        CASE WHEN invoice_no IS NULL OR TRIM(invoice_no) = '' THEN 1 ELSE 0 END AS incomplete_invoice_no,
        CASE WHEN stock_code IS NULL OR TRIM(stock_code) = '' THEN 1 ELSE 0 END AS incomplete_stock_code,
        CASE WHEN description IS NULL THEN 1 ELSE 0 END AS incomplete_description,
        CASE WHEN quantity IS NULL THEN 1 ELSE 0 END AS incomplete_quantity,
        CASE WHEN invoice_date IS NULL THEN 1 ELSE 0 END AS incomplete_invoice_date,
        CASE WHEN unit_price IS NULL THEN 1 ELSE 0 END AS incomplete_unit_price,
        CASE WHEN country IS NULL OR TRIM(country) = '' THEN 1 ELSE 0 END AS incomplete_country
        
    FROM public.online_retail_raw
),

-- -------------------------------------------------------
-- VALIDITY: Row-level flags
-- NOTE: Upstream dq1_validity is aggregated only
-- Reconstructing here with exact same logic
-- -------------------------------------------------------
validity_flags AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY invoice_date, invoice_no) AS row_id,
        
        -- Validity flags (1 = invalid format/value)
        CASE 
            WHEN TRIM(country) !~ '^[A-Za-z ]+$' THEN 1
            WHEN country NOT IN (
                'United Kingdom','France','EIRE','Germany','Netherlands','Spain',
                'Belgium','Switzerland','Portugal','Australia','Norway','Italy',
                'Channel Islands','Finland','Cyprus','Sweden','Austria','Denmark',
                'Japan','Poland','USA','Singapore','Iceland','Canada','Greece',
                'Israel','Lithuania','Malta','RSA','European Community',
                'United Arab Emirates','Saudi Arabia','Czech Republic','Lebanon',
                'Brazil','Bahrain'
            ) THEN 1
            ELSE 0
        END AS invalid_country,
        
        CASE 
            WHEN stock_code IS NULL THEN 1
            WHEN TRIM(stock_code) !~ '^[A-Za-z0-9 ]+$' THEN 1
            ELSE 0
        END AS invalid_stock_code,
        
        CASE 
            WHEN invoice_no IS NULL OR TRIM(invoice_no) !~ '^C?[0-9]{6}$' THEN 1
            ELSE 0
        END AS invalid_invoice_no,
        
        CASE 
            WHEN description IS NULL THEN 1
            WHEN TRIM(description) ~ '^[^a-zA-Z0-9 ,.\-()/]+$' THEN 1
            WHEN TRIM(description) ~* '(.)\1{4,}' THEN 1
            ELSE 0
        END AS invalid_description,
        
        CASE 
            WHEN quantity IS NULL THEN 1
            WHEN quantity = 0 THEN 1
            WHEN quantity > 10000 THEN 1
            ELSE 0
        END AS invalid_quantity,
        
        CASE 
            WHEN invoice_date IS NULL THEN 1
            WHEN invoice_date > CURRENT_TIMESTAMP THEN 1
            ELSE 0
        END AS invalid_invoice_date,
        
        CASE 
            WHEN unit_price IS NULL THEN 1
            WHEN unit_price < 0 THEN 1
            WHEN unit_price > 99999.99 THEN 1
            WHEN ROUND(unit_price::NUMERIC, 2) <> unit_price::NUMERIC THEN 1
            ELSE 0
        END AS invalid_unit_price
        
    FROM public.online_retail_raw
),

-- -------------------------------------------------------
-- CONSISTENCY: Cross-column rules
-- -------------------------------------------------------
consistency_flags AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY invoice_date, invoice_no) AS row_id,
        
        -- Rule: quantity > 0 → unit_price > 0
        CASE 
            WHEN quantity > 0 AND (unit_price IS NULL OR unit_price <= 0) THEN 1 
            ELSE 0 
        END AS inconsistent_sales_price,
        
        -- Rule: quantity < 0 → unit_price > 0 (returns)
        CASE 
            WHEN quantity < 0 AND (unit_price IS NULL OR unit_price <= 0) THEN 1 
            ELSE 0 
        END AS inconsistent_return_price
        
    FROM public.online_retail_raw
),

-- -------------------------------------------------------
-- UNIQUENESS: Duplicate detection
-- -------------------------------------------------------
uniqueness_flags AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY invoice_date, invoice_no) AS row_id,
        
        -- Flag duplicates (keep first, mark rest)
        CASE 
            WHEN ROW_NUMBER() OVER (
                PARTITION BY invoice_no, stock_code 
                ORDER BY invoice_date, invoice_no
            ) > 1 THEN 1 
            ELSE 0 
        END AS is_duplicate
        
    FROM public.online_retail_raw
),

-- -------------------------------------------------------
-- COMBINE: Single LEFT JOIN per dimension (reuses row_id)
-- -------------------------------------------------------
all_flags AS (
    SELECT 
        s.row_id,
        s.invoice_no,
        s.stock_code,
        s.description,
        s.quantity,
        s.invoice_date,
        s.unit_price,
        s.country,
        
        -- Completeness
        COALESCE(c.incomplete_invoice_no, 0) AS incomplete_invoice_no,
        COALESCE(c.incomplete_stock_code, 0) AS incomplete_stock_code,
        COALESCE(c.incomplete_description, 0) AS incomplete_description,
        COALESCE(c.incomplete_quantity, 0) AS incomplete_quantity,
        COALESCE(c.incomplete_invoice_date, 0) AS incomplete_invoice_date,
        COALESCE(c.incomplete_unit_price, 0) AS incomplete_unit_price,
        COALESCE(c.incomplete_country, 0) AS incomplete_country,
        
        -- Validity
        COALESCE(v.invalid_country, 0) AS invalid_country,
        COALESCE(v.invalid_stock_code, 0) AS invalid_stock_code,
        COALESCE(v.invalid_invoice_no, 0) AS invalid_invoice_no,
        COALESCE(v.invalid_description, 0) AS invalid_description,
        COALESCE(v.invalid_quantity, 0) AS invalid_quantity,
        COALESCE(v.invalid_invoice_date, 0) AS invalid_invoice_date,
        COALESCE(v.invalid_unit_price, 0) AS invalid_unit_price,
        
        -- Consistency
        COALESCE(cons.inconsistent_sales_price, 0) AS inconsistent_sales_price,
        COALESCE(cons.inconsistent_return_price, 0) AS inconsistent_return_price,
        
        -- Uniqueness
        COALESCE(u.is_duplicate, 0) AS is_duplicate
        
    FROM source_enriched s
    LEFT JOIN completeness_flags c USING (row_id)
    LEFT JOIN validity_flags v USING (row_id)
    LEFT JOIN consistency_flags cons USING (row_id)
    LEFT JOIN uniqueness_flags u USING (row_id)
),

-- -------------------------------------------------------
-- CALCULATE: Row-level metrics (clean, no repetition)
-- -------------------------------------------------------
row_metrics AS (
    SELECT 
        *,
        
        -- Calculate error count once
        (
            incomplete_invoice_no + incomplete_stock_code + incomplete_description +
            incomplete_quantity + incomplete_invoice_date + incomplete_unit_price +
            incomplete_country +
            invalid_country + invalid_stock_code + invalid_invoice_no +
            invalid_description + invalid_quantity + invalid_invoice_date +
            invalid_unit_price +
            inconsistent_sales_price + inconsistent_return_price +
            is_duplicate
        ) AS error_count
        
    FROM all_flags
)

-- -------------------------------------------------------
-- FINAL: Original columns + DQ flags
-- -------------------------------------------------------
SELECT 
    -- Original data
    invoice_no,
    stock_code,
    description,
    quantity,
    invoice_date,
    unit_price,
    country,
    
    -- DQ flags
    CASE WHEN error_count > 0 THEN 1 ELSE 0 END AS affected_row,
    error_count,
    
    -- Optional: individual flags (commented to save space, uncomment if needed)
    -- incomplete_invoice_no, incomplete_stock_code, incomplete_description,
    -- incomplete_quantity, incomplete_invoice_date, incomplete_unit_price,
    -- incomplete_country, invalid_country, invalid_stock_code, invalid_invoice_no,
    -- invalid_description, invalid_quantity, invalid_invoice_date, invalid_unit_price,
    -- inconsistent_sales_price, inconsistent_return_price, is_duplicate,
    
    CURRENT_TIMESTAMP AS dq_checked_at
    
FROM row_metrics
ORDER BY row_id