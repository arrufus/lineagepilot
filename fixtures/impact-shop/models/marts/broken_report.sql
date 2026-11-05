-- Deliberately not valid SQL (design section 14): dbt compiles it, nothing can parse it
select order_id, order_total
from {{ ref('orders') }}
where ((
