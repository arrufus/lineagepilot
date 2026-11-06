select d.customer_id, sum(f.amount) as ltv
from {{ ref('dim_customers') }} d join {{ ref('fct_orders') }} f using (customer_id)
group by 1
