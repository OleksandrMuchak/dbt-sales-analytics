-- models/marts/fct_sales.sql
with stg_sales as (
    select * from {{ ref('stg_sales_data') }}
),
aggregated_sales as (
    select
        reference_id,
        max(product_name) as product_name,
        max(country) as country,
        max(campaign_name) as campaign_name,
        max(source) as source,
        max(order_date_kyiv) as order_date_kyiv,
        max(return_date_kyiv) as return_date_kyiv,
        sum(total_amount) as total_amount,
        sum(coalesce(total_rebill_amount, 0)) as total_rebill_amount,
        sum(coalesce(returned_amount, 0)) as returned_amount,
        sum(number_of_rebills) as number_of_rebills,
        sum(discount_amount) as discount_amount,
        list(distinct sales_agent_name) as sales_agents
    from stg_sales
    group by 1
),
final as (
    select
        reference_id,
        product_name,
        array_to_string(sales_agents, ', ') as sales_agents_list,
        country,
        campaign_name,
        source,
        (total_amount + total_rebill_amount - returned_amount) as total_revenue,
        total_rebill_amount as rebill_revenue,
        number_of_rebills,
        discount_amount,
        returned_amount,
        order_date_kyiv,
        (order_date_kyiv at time zone 'Europe/Kyiv') at time zone 'UTC' as order_date_utc,
        (order_date_kyiv at time zone 'Europe/Kyiv') at time zone 'America/New_York' as order_date_ny,
        return_date_kyiv,
        (return_date_kyiv at time zone 'Europe/Kyiv') at time zone 'UTC' as return_date_utc,
        (return_date_kyiv at time zone 'Europe/Kyiv') at time zone 'America/New_York' as return_date_ny,
        date_diff('day', order_date_kyiv::date, return_date_kyiv::date) as days_to_return
    from aggregated_sales
)
select * from final