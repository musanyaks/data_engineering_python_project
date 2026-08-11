{% snapshot snap_customers %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='_loaded_at',
    )
}}

SELECT * FROM {{ ref('dim_customers') }}

{% endsnapshot %}
