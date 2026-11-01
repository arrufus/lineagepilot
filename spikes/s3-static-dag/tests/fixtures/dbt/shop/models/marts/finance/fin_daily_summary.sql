select count(*) as n, sum(amount) as total from {{ ref('fct_orders') }}
