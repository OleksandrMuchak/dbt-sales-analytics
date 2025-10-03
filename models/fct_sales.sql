with sales as (
    select * from {{ ref('stg_sales_data') }}
)
select
    reference_id,
    product_code,
    product_name,
    subscription_start_date,
    subscription_deactivation_date,
    order_date_kyiv,
    total_amount,
    discount_amount,
    original_amount,
    returned_amount,
    total_rebill_amount,
    has_chargeback,
    has_refund
from sales
