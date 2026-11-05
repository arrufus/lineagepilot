select
    order_id,
    customer_id,
    amount_usd as total_usd,
    ordered_at
from {{ ref('stg_orders') }}
