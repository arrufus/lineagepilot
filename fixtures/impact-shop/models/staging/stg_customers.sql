select
    id as customer_id,
    full_name,
    signed_up_at
from {{ source('shop', 'raw_customers') }}
