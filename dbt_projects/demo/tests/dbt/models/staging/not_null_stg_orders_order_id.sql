select order_id from {{ ref('stg_orders') }} where order_id is null
