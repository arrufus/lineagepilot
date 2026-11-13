-- Orders as the till records them: one row per order, money in dollars.

with till as (

    select * from {{ source('ecom', 'raw_orders') }}

)

select
    id as order_id,
    customer_id,
    location_id,
    cast(ordered_at as timestamp) as ordered_at,
    {{ cents_to_dollars('subtotal_cents') }} as subtotal,
    {{ cents_to_dollars('tax_cents') }} as tax_paid,
    {{ cents_to_dollars('(subtotal_cents + tax_cents)') }} as order_total
from till
