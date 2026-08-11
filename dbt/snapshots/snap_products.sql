{% snapshot snap_products %}

{{
    config(
      target_schema='snapshots',
      unique_key='product_id',
      strategy='check',
      check_cols=['retail_price', 'cost_price'],
    )
}}

SELECT * FROM {{ ref('dim_products') }}

{% endsnapshot %}
