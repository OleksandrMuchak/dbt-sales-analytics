-- If there are rebills, there should be rebill revenue and opposite
select
    reference_id,
    number_of_rebills,
    rebill_revenue
from {{ ref('fct_sales') }}
where 
    (number_of_rebills = 0 AND rebill_revenue != 0)
    OR 
    (number_of_rebills > 0 AND rebill_revenue = 0)