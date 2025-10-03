-- Incorrect revenue calculations would lead to wrong financial reporting
select
    reference_id,
    total_revenue,
    (total_amount + rebill_revenue - returned_amount) as calculated_revenue,
    abs(total_revenue - (total_amount + rebill_revenue - returned_amount)) as difference
from {{ ref('fct_sales') }}
where abs(total_revenue - (total_amount + rebill_revenue - returned_amount)) > 0.01