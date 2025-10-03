--Agents with their average discount compared to global average
with agent_avg_discount as (
    select
        sales_agent_name,
        ROUND(avg(discount_amount)::numeric, 2) as avg_discount
    from {{ ref('stg_sales_data') }}
    where discount_amount > 0 and sales_agent_name != 'N/A'
    group by 1
),
overall_avg_discount as (
    select
        ROUND(avg(discount_amount)::numeric, 2) as global_avg_discount
    from {{ ref('stg_sales_data') }}
    where discount_amount > 0 and sales_agent_name != 'N/A'
)
select
    agent_avg_discount.sales_agent_name,
    agent_avg_discount.avg_discount,
    overall_avg_discount.global_avg_discount
from agent_avg_discount
cross join overall_avg_discount
where agent_avg_discount.avg_discount > overall_avg_discount.global_avg_discount
order by agent_avg_discount.avg_discount desc