select id as customer_id, name, country_code from {{ source('raw', 'customers') }}
