{{ config(materialized='table') }}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

sales_stats AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        SUM(total_amount) AS lifetime_value,
        MIN(sale_date) AS first_purchase_date,
        MAX(sale_date) AS last_purchase_date
    FROM {{ ref('fct_sales') }}
    GROUP BY 1
),

customer_metrics AS (
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.country,
        c.registration_date,
        COALESCE(s.total_orders, 0) AS total_orders,
        COALESCE(s.lifetime_value, 0) AS lifetime_value,
        s.first_purchase_date,
        s.last_purchase_date,
        CASE
            WHEN s.last_purchase_date >= DATEADD(day, -90, CURRENT_DATE()) THEN 'Active'
            WHEN s.last_purchase_date >= DATEADD(day, -180, CURRENT_DATE()) THEN 'At Risk'
            WHEN s.last_purchase_date IS NOT NULL THEN 'Churned'
            ELSE 'Never Purchased'
        END AS customer_segment,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM customers c
    LEFT JOIN sales_stats s ON c.customer_id = s.customer_id
)

SELECT * FROM customer_metrics
