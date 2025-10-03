-- Missing timezone conversions would indicate data processing errors
select
    reference_id,
    order_date_kyiv,
    order_date_utc,
    order_date_ny
from {{ ref('fct_sales') }}
where 
    (order_date_kyiv IS NOT NULL AND order_date_utc IS NULL)
    OR
    (order_date_kyiv IS NOT NULL AND order_date_ny IS NULL)