{{ config(materialized='table') }}

SELECT
    customer_id,
    customer_name,
    account_balance,
    market_segment
FROM {{ ref('stg_customers') }}
