{% snapshot snap_base %}
{{ config(unique_key='id', strategy='check', check_cols='all') }}
select * from {{ ref('base') }}
{% endsnapshot %}
