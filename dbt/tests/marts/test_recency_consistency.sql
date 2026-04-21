WITH base AS (

    SELECT *
    FROM {{ ref('fct_customer_rfm') }}

)

SELECT *
FROM base
WHERE recency_days < 0