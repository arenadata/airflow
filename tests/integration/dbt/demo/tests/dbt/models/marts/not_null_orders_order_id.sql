select order_id from {{ ref('orders') }} where order_id is null
