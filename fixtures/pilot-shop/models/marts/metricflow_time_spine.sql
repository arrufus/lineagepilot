-- One row per day, which MetricFlow needs to put metrics on a time axis.

with recursive days as (

    select cast('2024-01-01' as date) as date_day
    union all
    select cast(date_day + interval 1 day as date) from days
    where date_day < cast('2026-12-31' as date)

)

select date_day from days
