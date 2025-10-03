-- Agent metrics with ranking based on total revenue
with agent_sales as (
    select
        sales_agent_name,
        reference_id,
        (total_amount + coalesce(total_rebill_amount, 0) - coalesce(returned_amount, 0)) as revenue,
        discount_amount
    from {{ ref('stg_sales_data') }}
    where sales_agent_name != 'N/A'
),
agent_metrics as (
    select
        sales_agent_name,
        ROUND(avg(revenue)::numeric, 2) as average_revenue_per_sale,
        count(distinct reference_id) as total_sales,
        ROUND(avg(discount_amount)::numeric, 2) as average_discount,
        ROUND(sum(revenue)::numeric, 2) as total_revenue
    from agent_sales
    group by sales_agent_name
)
select
    sales_agent_name,
    average_revenue_per_sale,
    total_sales,
    average_discount,
    total_revenue,
    RANK() OVER (ORDER BY total_revenue DESC) as agent_rank
from agent_metrics
order by total_revenue desc