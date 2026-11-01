{{ config(tags=["pii"]) }}
select id as payment_id, order_id, amount from {{ source('raw', 'payments') }}
