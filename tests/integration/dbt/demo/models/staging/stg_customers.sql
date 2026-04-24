SELECT
    c_custkey    AS customer_id,
    c_name       AS customer_name,
    c_nationkey  AS nation_id,
    c_acctbal    AS account_balance,
    c_mktsegment AS market_segment
FROM {{ source('src', 'customers') }}
