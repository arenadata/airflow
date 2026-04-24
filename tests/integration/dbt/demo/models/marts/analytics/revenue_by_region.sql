{{ config(materialized='table') }}

WITH orders AS (
    SELECT * FROM {{ ref('int_orders_enriched') }}
)
SELECT
    COALESCE(market_segment, 'Unknown') AS market_segment,
    date_trunc('month', order_date)::date AS order_month,
    count(DISTINCT order_id) AS order_count,
    count(DISTINCT customer_id) AS unique_customers,
    round(sum(amount_cents) / 100.0, 2) AS revenue
FROM orders
GROUP BY 1, 2
