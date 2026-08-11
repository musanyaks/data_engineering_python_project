{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'customers') }}
),

renamed AS (
    SELECT
        customer_id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        LOWER(TRIM(email)) AS email,
        CAST(registration_date AS DATE) AS registration_date,
        UPPER(TRIM(country)) AS country,
        created_at,
        _loaded_at
    FROM source
    WHERE email IS NOT NULL
)

SELECT * FROM renamed
