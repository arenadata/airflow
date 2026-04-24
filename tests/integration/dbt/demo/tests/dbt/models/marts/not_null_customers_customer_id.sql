select customer_id from {{ ref('customers') }} where customer_id is null
