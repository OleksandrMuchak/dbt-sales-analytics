-- models/staging/stg_sales_data.sql
with source as (
    select * from {{ ref('sales_data') }}
),
renamed as (
    select
        "Reference ID" as reference_id,
        "Country" as country,
        "Product Code" as product_code,
        coalesce("Product Name", 'N/A') as product_name,
        -- ВИПРАВЛЕНО: Використовуємо strptime для правильного парсингу дат
        strptime("Subscription Start Date", '%B %d, %Y, %I:%M %p') as subscription_start_date,
        strptime("Subscription Deactivation Date", '%B %d, %Y, %I:%M %p') as subscription_deactivation_date,
        "Subscription Duration Months"::integer as subscription_duration_months,
        strptime("Order Date Kyiv", '%B %d, %Y, %I:%M %p') as order_date_kyiv,
        strptime("Return Date Kyiv", '%B %d, %Y, %I:%M %p') as return_date_kyiv,
        strptime("Last Rebill Date Kyiv", '%B %d, %Y, %I:%M %p') as last_rebill_date_kyiv,
        "Has Chargeback" as has_chargeback,
        "Has Refund" as has_refund,
        coalesce("Sales Agent Name", 'N/A') as sales_agent_name,
        coalesce("Source", 'N/A') as source,
        coalesce("Campaign Name", 'N/A') as campaign_name,
        "Total Amount"::decimal as total_amount,
        "Discount Amount"::decimal as discount_amount,
        "Number Of Rebills"::integer as number_of_rebills,
        "Original Amount"::decimal as original_amount,
        "Returned Amount"::decimal as returned_amount,
        "Total Rebill Amount"::decimal as total_rebill_amount
    from source
)
select * from renamed