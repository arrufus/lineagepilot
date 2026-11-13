-- One row per product on an order, priced, with when the order was placed.

with lines as (

    select * from {{ ref('stg_order_lines') }}

),

placed as (

    select order_id, ordered_at from {{ ref('stg_orders') }}

),

products as (

    select * from {{ ref('stg_products') }}

)

select
    lines.order_line_id,
    lines.order_id,
    placed.ordered_at,
    lines.product_sku,
    products.category,
    products.is_drink,
    lines.quantity,
    lines.quantity * products.unit_price as line_total
from lines
inner join placed on placed.order_id = lines.order_id
inner join products on products.product_sku = lines.product_sku
