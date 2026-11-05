-- Reads orders, but not the column the rename changes
select
    customer_id,
    count(order_id) as order_count
from {{ ref('orders') }}
group by 1
