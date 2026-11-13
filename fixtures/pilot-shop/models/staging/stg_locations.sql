-- The shop's branches, and the sales tax each one charges.

with branches as (

    select * from {{ source('ecom', 'raw_locations') }}

)

select
    id as location_id,
    branch_name,
    tax_rate,
    cast(opened_on as date) as opened_on
from branches
