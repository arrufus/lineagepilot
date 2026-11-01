select customer_id, sum(amount) as revenue from {{ ref('fct_orders') }} group by 1
