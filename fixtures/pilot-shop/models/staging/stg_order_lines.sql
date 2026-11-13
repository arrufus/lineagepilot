-- What each order contained: one row per product on an order.

with lines as (

    select * from {{ source('ecom', 'raw_order_lines') }}

)

select
    id as order_line_id,
    order_id,
    product_sku,
    quantity
from lines
