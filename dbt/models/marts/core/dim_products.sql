{{ config(materialized='table') }}

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

sales_stats AS (
    SELECT
        product_id,
        SUM(quantity) AS total_units_sold,
        SUM(total_amount) AS total_revenue,
        COUNT(DISTINCT customer_id) AS unique_customers
    FROM {{ ref('fct_sales') }}
    GROUP BY 1
),

product_metrics AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        p.cost_price,
        p.retail_price,
        p.retail_price - p.cost_price AS margin_amount,
        ROUND((p.retail_price - p.cost_price) / NULLIF(p.retail_price, 0) * 100, 2) AS margin_pct,
        COALESCE(s.total_units_sold, 0) AS total_units_sold,
        COALESCE(s.total_revenue, 0) AS total_revenue,
        COALESCE(s.unique_customers, 0) AS unique_customers,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM products p
    LEFT JOIN sales_stats s ON p.product_id = s.product_id
)

SELECT * FROM product_metrics
