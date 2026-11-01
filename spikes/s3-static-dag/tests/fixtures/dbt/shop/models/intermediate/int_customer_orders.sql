select c.customer_id, count(o.order_id) as n_orders
from {{ ref('stg_customers') }} c left join {{ ref('stg_orders') }} o using (customer_id)
group by 1
