{# a comment with ref('not_a_dep') inside must be ignored #}
{{ config(materialized="incremental", tags=["hourly", "pii"]) }}
select * from {{ ref('base') }} join {{ ref("lookup") }} using (id)
