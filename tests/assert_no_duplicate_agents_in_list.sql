-- No agent appears more than once in the sales_agents_list field
with sales_with_duplicates as (
    select
        reference_id,
        sales_agents_list,
        array_length(
            string_to_array(sales_agents_list, ', '), 
            1
        ) as agent_count,
        array_length(
            array(
                select distinct unnest(string_to_array(sales_agents_list, ', '))
            ),
            1
        ) as unique_agent_count
    from {{ ref('fct_sales') }}
    where sales_agents_list IS NOT NULL
)

select *
from sales_with_duplicates
where agent_count != unique_agent_count