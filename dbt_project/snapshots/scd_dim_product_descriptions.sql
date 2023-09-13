{% snapshot scd_dim_product_descriptions %}

{{config(
    target_schema= 'snapshots',
    unique_key = 'product_id',
    strategy = 'timestamp',
    updated_at = 'date_last_updated',
    invalidate_hard_deletes = True
)}}

select * FROM {{ ref('dim_product_descriptions') }}

{% endsnapshot %}