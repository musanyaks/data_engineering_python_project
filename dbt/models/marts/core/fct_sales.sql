{{ config(
    materialized='incremental',
    unique_key='sale_id',
    partition_by={
      "field": "sale_date",
      "data_type": "date",
      "granularity": "day"
    }
) }}

WITH enriched AS (
    SELECT * FROM {{ ref('int_sales_enriched') }}
)

SELECT
    sale_id,
    sale_date,
    customer_id,
    product_id,
    quantity,
    unit_price,
    total_amount,
    product_margin,
    sale_profit,
    customer_tenure_days,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM enriched

{% if is_incremental() %}
WHERE sale_date >= (SELECT MAX(sale_date) FROM {{ this }})
{% endif %}
