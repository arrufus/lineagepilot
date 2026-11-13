-- The catalogue: one row per product the shop sells.

with catalogue as (

    select * from {{ source('ecom', 'raw_products') }}

)

select
    sku as product_sku,
    title as product_title,
    category,
    category = 'drink' as is_drink,
    {{ cents_to_dollars('unit_price_cents') }} as unit_price
from catalogue
