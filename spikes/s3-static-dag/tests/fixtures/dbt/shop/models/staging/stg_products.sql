select id as product_id, name, price from {{ ref('products') }}
