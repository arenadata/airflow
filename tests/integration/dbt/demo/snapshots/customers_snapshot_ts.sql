{% snapshot customers_snapshot_ts %}
{{
    config(
        target_schema='snapshots',
        unique_key='c_custkey',
        strategy='timestamp',
        updated_at='c_updated_at',
        schedule='@daily',
        sql_cluster='dev',
        py_cluster='dev',
        daily_sql_cluster='dev',
        bf_cluster='dev'
    )
}}
select
    c_custkey,
    c_name,
    c_nationkey,
    c_acctbal,
    c_mktsegment,
    c_updated_at
from {{ source('src', 'customers_scd') }}
{% endsnapshot %}
