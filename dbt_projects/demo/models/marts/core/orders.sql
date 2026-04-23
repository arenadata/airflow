{{ config(materialized='table') }}

SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.amount_cents,
    o.status,
    CASE o.status
        WHEN 'O' THEN 'Open'
        WHEN 'F' THEN 'Fulfilled'
        WHEN 'P' THEN 'Processing'
    END AS status_label,
    o.customer_name,
    o.market_segment
FROM {{ ref('int_orders_enriched') }} o
