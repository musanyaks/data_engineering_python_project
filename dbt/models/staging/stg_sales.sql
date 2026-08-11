{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'sales') }}
),

renamed AS (
    SELECT
        sale_id,
        product_id,
        customer_id,
        CAST(sale_date AS DATE) AS sale_date,
        CAST(quantity AS INTEGER) AS quantity,
        CAST(unit_price AS DECIMAL(18,2)) AS unit_price,
        CAST(total_amount AS DECIMAL(18,2)) AS total_amount,
        created_at,
        _loaded_at
    FROM source
)

SELECT * FROM renamed
