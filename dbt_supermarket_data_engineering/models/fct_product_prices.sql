{{ config(
    materialized='incremental',
    unique_key = 'pricing_date')
}}
with raw_staging AS (
    SELECT * FROM {{ source("supermarket","raw_staging") }}
)
SELECT
    {{ dbt_utils.surrogate_key(['id','_id','date_extracted']) }} AS price_surrogate_key,
    id AS product_id,
    "pricing.now" AS price_AUD,
    regexp_replace(size, '\D','','g')::numeric AS size_quantity,
    trim(regexp_replace(size,'[[:digit:]]','','g')) AS size_unit,
    "pricing.unit.price" AS standard_pricing_price_AUD,
    "pricing.unit.ofMeasureType" AS standard_pricing_unit,
    "pricing.comparable" AS check_for_standard_pricing_units_AUD,
    date_extracted AS pricing_date,
    _id AS mongodb_document_id
FROM
    raw_staging