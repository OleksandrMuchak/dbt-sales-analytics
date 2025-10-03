-- Monthly revenue with growth percentage compared to previous month
with monthly_revenue as (
    select
        DATE_TRUNC('month', order_date_kyiv)::date as sales_month,
        sum(total_revenue) as revenue
    from {{ ref('fct_sales') }}
    group by 1
),
revenue_with_lag as (
    select
        sales_month,
        revenue,
        LAG(revenue, 1) OVER (ORDER BY sales_month) as previous_month_revenue
    from monthly_revenue
)
select
    sales_month,
    revenue,
    previous_month_revenue,
    ROUND(
        ((revenue - previous_month_revenue) / NULLIF(previous_month_revenue, 0) * 100)::numeric, 
        2
    ) as growth_percentage
from revenue_with_lag
where previous_month_revenue IS NOT NULL
order by sales_month