-- analyses/agents_above_average_discount.sql

-- Спочатку розраховуємо середню знижку для кожного агента
with agent_avg_discount as (
    select
        sales_agent_name,
        avg(discount_amount) as avg_discount
    from {{ ref('stg_sales_data') }}
    where discount_amount > 0 and sales_agent_name != 'N/A'
    group by 1
),

-- Потім розраховуємо загальну середню знижку по всій компанії
overall_avg_discount as (
    select
        avg(discount_amount) as global_avg_discount
    from {{ ref('stg_sales_data') }}
    where discount_amount > 0 and sales_agent_name != 'N/A'
)

-- Вибираємо тільки тих агентів, чия середня знижка вища за загальну
select
    agent_avg_discount.sales_agent_name,
    agent_avg_discount.avg_discount,
    overall_avg_discount.global_avg_discount
from agent_avg_discount
cross join overall_avg_discount
where agent_avg_discount.avg_discount > overall_avg_discount.global_avg_discount
order by agent_avg_discount.avg_discount desc