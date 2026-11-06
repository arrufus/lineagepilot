select * from {{ ref('customer_ltv') }} cross join {{ ref('fin_daily_summary') }}
