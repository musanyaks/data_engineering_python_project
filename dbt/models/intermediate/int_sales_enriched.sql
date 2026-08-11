{{ config(materialized='table') }}

WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

enriched AS (
    SELECT
        s.sale_id,
        s.sale_date,
        s.quantity,
        s.unit_price,
        s.total_amount,

        -- Customer info
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.country,
        c.registration_date,

        -- Product info
        p.product_id,
        p.product_name,
        p.category,
        p.cost_price,
        p.retail_price,

        -- Calculated fields
        (p.retail_price - p.cost_price) AS product_margin,
        (s.total_amount - (s.quantity * p.cost_price)) AS sale_profit,
        DATEDIFF(day, c.registration_date, s.sale_date) AS customer_tenure_days

    FROM sales s
    LEFT JOIN customers c ON s.customer_id = c.customer_id
    LEFT JOIN products p ON s.product_id = p.product_id
)

SELECT * FROM enriched
