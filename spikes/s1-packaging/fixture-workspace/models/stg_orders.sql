select id as order_id, customer_id, amount
from {{ source('shop', 'orders') }}
