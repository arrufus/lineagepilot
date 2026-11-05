select
    customers.customer_id,
    customers.full_name,
    sum(orders.order_total) as lifetime_value
from {{ ref('stg_customers') }} as customers
left join {{ ref('orders') }} as orders
    on customers.customer_id = orders.customer_id
group by 1, 2
