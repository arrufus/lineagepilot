select
    order_id,
    customer_id,
    amount_usd as order_total,
    ordered_at
from {{ ref('stg_orders') }}
