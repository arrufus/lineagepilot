select o.order_id, o.customer_id, o.quantity * p.price as line_total
from {{ ref('stg_orders') }} o join {{ ref('stg_products') }} p using (product_id)
