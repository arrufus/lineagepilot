select id as order_id, customer_id, product_id, quantity, ordered_at from {{ source('raw', 'orders') }}
