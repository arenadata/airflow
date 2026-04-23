select customer_id from {{ ref('stg_customers') }} where customer_id is null
