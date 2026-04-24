{{ config(materialized='table') }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
)
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.amount_cents,
    o.status,
    c.customer_name,
    c.account_balance,
    c.market_segment,
    o.loaded_at
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
