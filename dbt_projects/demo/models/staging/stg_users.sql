-- Staging model: raw data from source
{{ config(materialized='view') }}

select
    id,
    name,
    email,
    created_at,
    updated_at
from {{ source('raw', 'users') }}
where deleted_at is null
