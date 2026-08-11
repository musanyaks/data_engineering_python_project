{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'products') }}
),

renamed AS (
    SELECT
        product_id,
        TRIM(product_name) AS product_name,
        TRIM(category) AS category,
        CAST(cost_price AS DECIMAL(18,2)) AS cost_price,
        CAST(retail_price AS DECIMAL(18,2)) AS retail_price,
        created_at,
        _loaded_at
    FROM source
)

SELECT * FROM renamed
