-- Customers as the sign-up form records them.

with signups as (

    select * from {{ source('ecom', 'raw_customers') }}

)

select
    id as customer_id,
    name as customer_name,
    cast(signed_up_on as date) as signed_up_on
from signups
