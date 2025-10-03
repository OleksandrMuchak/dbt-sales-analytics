# GYB Sales Analytics 

## Database
**PostgreSQL**

## Project Overview

This dbt project creates a sales data mart (`fct_sales`) with one unique record per sale, aggregating multiple agent interactions.

## Setup Instructions

```bash
# 1. Install dbt-postgres
pip install dbt-postgres

# 2. Configure profiles.yml (~/.dbt/profiles.yml)
gyb_sales:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: your_username
      password: your_password
      port: 5432
      dbname: gyb_db
      schema: public
      threads: 4

# 3. Install dbt packages
dbt deps

# 4. Load seed data
dbt seed

# 5. Run models
dbt run

# 6. Run tests
dbt test
```

## Project Structure

```
├── analyses/
│   ├── monthly_revenue_growth.sql          # Month-over-month revenue growth %
│   ├── agent_performance.sql               # Agent metrics with rankings
│   └── agents_above_average_discount.sql   # Agents exceeding avg discount
├── models/
│   ├── staging/
│   │   ├── stg_sales_data.sql             # Cleaned sales data
│   │   └── schema.yml                      # Staging tests
│   └── marts/
│       ├── fct_sales.sql                   # Sales fact table
│       └── schema.yml                      # Fact table tests
├── seeds/
│   └── sales_data.csv                      # Source data
├── tests/
│   ├── assert_no_duplicate_agents_in_list.sql
│   ├── assert_revenue_components_add_up.sql
│   ├── assert_rebills_match_revenue.sql
│   ├── assert_valid_timezone_conversions.sql
│   └── assert_no_negative_financial_values.sql
├── dbt_project.yml
└── packages.yml
```

## fct_sales Table Columns

- `reference_id` - Unique sale ID
- `product_name` - Product sold
- `sales_agents_list` - Comma-separated list of agents
- `country` - Country code
- `campaign_name` - Marketing campaign
- `source` - Sale channel 
- `total_revenue` - Net revenue 
- `rebill_revenue` - Revenue from rebills only
- `number_of_rebills` - Count of rebill payments
- `discount_amount` - Total discount given
- `returned_amount` - Amount refunded
- `total_amount` - Initial payment amount
- `order_date_kyiv/utc/ny` - Order date in 3 timezones
- `return_date_kyiv/utc/ny` - Return date in 3 timezones
- `days_to_return` - Days between order and return

## Analytical Queries

### 1. Monthly Revenue Growth
```bash
# Calculate month-over-month revenue growth percentage
dbt compile -s analyses/monthly_revenue_growth
```

### 2. Agent Performance with Rankings
```bash
# Average revenue, sales count, average discount per agent with ranking
dbt compile -s analyses/agent_performance
```

### 3. Agents Above Average Discount
```bash
# Identify agents offering discounts above company average
dbt compile -s analyses/agents_above_average_discount
```

## Data Quality

- Empty fields filled with 'N/A'
- NULL numeric values replaced with 0
- Automated tests covering:
  - Data integrity (not_null, unique)
  - Revenue calculation
  - Timezone conversions
- All tests pass with warnings for known data quality issues
