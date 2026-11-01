select c.customer_id, c.n_orders, sum(p.amount) as paid
from {{ ref('int_customer_orders') }} c left join {{ ref('stg_payments') }} p on true
group by 1, 2
