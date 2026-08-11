SELECT customer_id, COUNT(*) as cnt
FROM {{ ref('dim_customers') }}
GROUP BY 1
HAVING COUNT(*) > 1
