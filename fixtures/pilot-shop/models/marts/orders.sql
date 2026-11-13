-- One row per order, with how much was on it.

with placed as (

    select * from {{ ref('stg_orders') }}

),

baskets as (

    select
        order_id,
        count(*) as line_count,
        sum(quantity) as item_count,
        sum(case when is_drink then quantity else 0 end) as drink_count
    from {{ ref('order_items') }}
    group by order_id

)

select
    placed.*,
    coalesce(baskets.line_count, 0) as line_count,
    coalesce(baskets.item_count, 0) as item_count,
    coalesce(baskets.drink_count, 0) as drink_count
from placed
left join baskets on baskets.order_id = placed.order_id
