select customer_id, sum(line_total) as revenue from {{ ref('fct_orders') }} group by 1
