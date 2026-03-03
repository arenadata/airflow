-- Mart model: business logic transformation
{{ config(materialized='table') }}

with users as (
    select * from {{ ref('stg_users') }}
),

user_stats as (
    select
        id as user_id,
        name,
        email,
        created_at,
        date_trunc('month', created_at) as signup_month,
        extract(year from created_at) as signup_year
    from users
)

select * from user_stats
