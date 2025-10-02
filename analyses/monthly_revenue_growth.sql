-- analyses/monthly_revenue_growth.sql

with monthly_revenue as (

    -- Розраховуємо загальний дохід за кожен місяць
    select
        date_trunc('month', order_date_kyiv)::date as sales_month,
        sum(total_revenue) as revenue
    from {{ ref('fct_sales') }}
    group by 1

),

revenue_with_lag as (

    -- Додаємо колонку з доходом за попередній місяць за допомогою віконної функції LAG
    select
        sales_month,
        revenue,
        lag(revenue, 1) over (order by sales_month) as previous_month_revenue
    from monthly_revenue

)

-- Розраховуємо відсоткове зростання
select
    sales_month,
    revenue,
    previous_month_revenue,
    (revenue - previous_month_revenue) / previous_month_revenue * 100 as growth_percentage
from revenue_with_lag
order by sales_month