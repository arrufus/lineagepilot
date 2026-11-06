{{ config(materialized='incremental', tags=['hourly'], unique_key='order_id') }}
select i.order_id, i.customer_id, i.line_total, p.amount
from {{ ref('int_order_items') }} i left join {{ ref('stg_payments') }} p using (order_id)
