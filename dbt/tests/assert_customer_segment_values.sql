SELECT *
FROM {{ ref('dim_customers') }}
WHERE customer_segment NOT IN ('Active', 'At Risk', 'Churned', 'Never Purchased')
