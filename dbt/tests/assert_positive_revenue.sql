SELECT *
FROM {{ ref('fct_sales') }}
WHERE total_amount < 0
   OR quantity < 0
   OR unit_price < 0
