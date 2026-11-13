-- The shop's branches, as the semantic layer sees them.

select
    location_id,
    branch_name,
    tax_rate,
    opened_on
from {{ ref('stg_locations') }}
