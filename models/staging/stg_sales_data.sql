with source as (
    select * from {{ ref('sales_data') }}
),
renamed as (
    select
        "Reference ID" as reference_id,
        "Country" as country,
        "Product Code" as product_code,
        coalesce("Product Name", 'N/A') as product_name,
        
        CASE 
            WHEN "Subscription Start Date" = 'nan' OR "Subscription Start Date" IS NULL 
            THEN NULL
            ELSE TO_TIMESTAMP("Subscription Start Date", 'Month DD, YYYY, HH12:MI AM')
        END as subscription_start_date,
        
        CASE 
            WHEN "Subscription Deactivation Date" = 'nan' OR "Subscription Deactivation Date" IS NULL 
            THEN NULL
            ELSE TO_TIMESTAMP("Subscription Deactivation Date", 'Month DD, YYYY, HH12:MI AM')
        END as subscription_deactivation_date,
        
        COALESCE(CAST("Subscription Duration Months" AS INTEGER), 0) as subscription_duration_months,
        
        CASE 
            WHEN "Order Date Kyiv" = 'nan' OR "Order Date Kyiv" IS NULL 
            THEN NULL
            ELSE TO_TIMESTAMP("Order Date Kyiv", 'Month DD, YYYY, HH12:MI AM')
        END as order_date_kyiv,
        
        CASE 
            WHEN "Return Date Kyiv" = 'nan' OR "Return Date Kyiv" IS NULL 
            THEN NULL
            ELSE TO_TIMESTAMP("Return Date Kyiv", 'Month DD, YYYY, HH12:MI AM')
        END as return_date_kyiv,
        
        CASE 
            WHEN "Last Rebill Date Kyiv" = 'nan' OR "Last Rebill Date Kyiv" IS NULL 
            THEN NULL
            ELSE TO_TIMESTAMP("Last Rebill Date Kyiv", 'Month DD, YYYY, HH12:MI AM')
        END as last_rebill_date_kyiv,
        
        "Has Chargeback" as has_chargeback,
        "Has Refund" as has_refund,
        coalesce("Sales Agent Name", 'N/A') as sales_agent_name,
        coalesce("Source", 'N/A') as source,
        coalesce("Campaign Name", 'N/A') as campaign_name,
        COALESCE(CAST("Total Amount" AS DECIMAL), 0) as total_amount,
        COALESCE(CAST("Discount Amount" AS DECIMAL), 0) as discount_amount,
        COALESCE(CAST("Number Of Rebills" AS INTEGER), 0) as number_of_rebills,
        COALESCE(CAST("Original Amount" AS DECIMAL), 0) as original_amount,
        COALESCE(CAST("Returned Amount" AS DECIMAL), 0) as returned_amount,
        COALESCE(CAST("Total Rebill Amount" AS DECIMAL), 0) as total_rebill_amount
    from source
)
select * from renamed