{% snapshot snap_customers %}
{{ config(target_schema='snapshots', unique_key='id', strategy='check', check_cols='all') }}
select * from {{ source('raw', 'customers') }}
{% endsnapshot %}
