select
    customer_id,
    case when lifetime_value >= 100 then 'gold' else 'standard' end as tier
from {{ ref('customer_revenue') }}
