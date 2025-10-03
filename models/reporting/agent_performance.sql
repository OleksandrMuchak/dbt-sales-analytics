-- analyses/agent_performance.sql

-- Використовуємо stg_sales_data, щоб аналізувати кожен продаж окремо,
-- навіть якщо над ним працювало кілька агентів
with agent_sales as (
    select
        sales_agent_name,
        reference_id,
        -- Розраховуємо дохід для кожного окремого рядка продажу
        (total_amount + coalesce(total_rebill_amount, 0) - coalesce(returned_amount, 0)) as revenue,
        discount_amount
    from {{ ref('stg_sales_data') }}
    where sales_agent_name != 'N/A'
)
-- Агрегуємо дані по кожному агенту
select
    sales_agent_name,
    avg(revenue) as average_revenue_per_sale,
    count(distinct reference_id) as total_sales,
    avg(discount_amount) as average_discount,
    sum(revenue) as total_revenue,
    -- Ранжуємо агентів за їх загальним доходом
    rank() over (order by sum(revenue) desc) as agent_rank
from agent_sales
group by sales_agent_name
order by total_revenue desc