with signed_up as (

    -- Everyone who has signed up, whether or not they have ordered

    select * from {{ ref('stg_customers') }}

),

spending as (

    select
        customer_id,
        count(distinct order_id) > 1 as has_ordered_again,
        count(distinct order_id) as order_count,
        min(ordered_at) as first_ordered_at,
        sum(order_total) as total_spent
    from {{ ref('orders') }}
    group by customer_id

),

profiles as (

    select
        signed_up.*,
        coalesce(spending.total_spent, 0) as lifetime_spend,
        coalesce(spending.order_count, 0) as order_count,
        spending.first_ordered_at,
        case when spending.has_ordered_again then 'regular' else 'new' end as customer_type
    from signed_up
    left join spending on spending.customer_id = signed_up.customer_id

)

select * from profiles
