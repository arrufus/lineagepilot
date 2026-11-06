{{ config(tags=['deprecated']) }}
select * from {{ ref('dim_products') }}
