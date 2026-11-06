{% set t = 'base' %}
select * from {{ ref(t) }} union all select * from {{ ref('other_pkg', 'shared') }}
