select
    id as order_id,
    customer_id,
    {{ cents_to_dollars('amount_cents') }} as amount_usd,
    ordered_at
from {{ source('shop', 'raw_orders') }}
