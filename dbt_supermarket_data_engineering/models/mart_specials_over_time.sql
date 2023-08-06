{{ config(
    materialized='incremental',
    unique_key = 'date_extracted')
}}
WITH left_descriptions AS (
    SELECT * FROM {{ ref('dim_product_descriptions') }}
),
right_specials AS (
    SELECT * FROM {{ ref('fct_product_specials') }}
)
SELECT
    right_specials.date_extracted,
    left_descriptions.product_name,
    left_descriptions.category AS product_category,
    left_descriptions.subcategory AS product_subcategory,
    right_specials.special_id,
    right_specials.special_description,
    right_specials.special_end_date_text,
    right_specials.special_type,
    right_specials.special_subtype,
    right_specials.min_purchase_quantity,
    right_specials.savings
    
FROM
    left_descriptions

RIGHT JOIN right_specials ON (left_descriptions.product_id = right_specials.product_id)