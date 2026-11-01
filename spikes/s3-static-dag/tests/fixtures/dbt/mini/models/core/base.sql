select 1 as id from {{ source("raw", "events") }}
