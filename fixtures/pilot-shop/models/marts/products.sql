-- What the shop sells, as the catalogue lists it.

select * from {{ ref('stg_products') }}
