{{ config(
    materialized='incremental',
    unique_key = 'date_extracted')  
}}
with raw_staging AS (
    SELECT * FROM {{ source("supermarket","raw_staging") }}
)
SELECT
    {{ dbt_utils.surrogate_key(['id','_id','date_extracted']) }} AS product_on_special_surrogate_key,
    id AS product_id,
    "pricing.multiBuyPromotion.id" AS special_id,
    "pricing.offerDescription" AS special_description,
    "pricing.promotionDescription" AS special_end_date_text,
    "pricing.onlineSpecial" AS has_online_special,
    "pricing.promotionType" AS special_type,
    "pricing.specialType" AS special_subtype,
    "pricing.multiBuyPromotion.minQuantity" AS min_purchase_quantity,
    "pricing.multiBuyPromotion.reward" AS savings,
    date_extracted,
    _id AS mongodb_document_id
FROM
    raw_staging
WHERE
    "pricing.promotionType" IS NOT NULL