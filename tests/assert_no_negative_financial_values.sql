-- No Negative amounts 

select
    reference_id,
    total_amount,
    discount_amount,
    rebill_revenue
from {{ ref('fct_sales') }}
where 
    total_amount < 0
    OR discount_amount < 0
    OR rebill_revenue < 0