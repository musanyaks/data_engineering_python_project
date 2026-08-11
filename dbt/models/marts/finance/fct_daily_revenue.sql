{{ config(materialized='table') }}

WITH daily_sales AS (
    SELECT
        sale_date,
        country,
        category,
        COUNT(DISTINCT sale_id) AS total_transactions,
        SUM(quantity) AS total_units,
        SUM(total_amount) AS gross_revenue,
        SUM(sale_profit) AS net_profit,
        AVG(total_amount) AS avg_transaction_value,
        COUNT(DISTINCT customer_id) AS unique_customers
    FROM {{ ref('int_sales_enriched') }}
    GROUP BY 1, 2, 3
)

SELECT
    sale_date,
    country,
    category,
    total_transactions,
    total_units,
    gross_revenue,
    net_profit,
    avg_transaction_value,
    unique_customers,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM daily_sales
