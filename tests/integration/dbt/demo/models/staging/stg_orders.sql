SELECT
    o_orderkey    AS order_id,
    o_custkey     AS customer_id,
    o_orderdate   AS order_date,
    o_totalprice  AS amount_cents,
    o_orderstatus AS status,
    now()         AS loaded_at
FROM {{ source('src', 'orders') }}
