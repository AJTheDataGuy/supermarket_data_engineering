{{ config(
    materialized='incremental',
    unique_key = 'pricing_date') 
}}
WITH left_descriptions AS (
    SELECT * FROM {{ ref('dim_product_descriptions') }}
),
right_prices AS (
    SELECT * FROM {{ ref('fct_product_prices') }}
)
SELECT
    right_prices.pricing_date,
    left_descriptions.product_name,
    left_descriptions.category AS product_category,
    left_descriptions.subcategory AS product_subcategory,
    right_prices.price_aud,
    right_prices.size_quantity,
    right_prices.size_unit,
    ROUND(right_prices.price_aud / right_prices.size_quantity,4)  AS price_aud_per_quantity_unit
    
FROM
    left_descriptions

LEFT JOIN right_prices ON (left_descriptions.product_id = right_prices.product_id)

