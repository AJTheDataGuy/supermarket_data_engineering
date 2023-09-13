{{ config(
    materialized='incremental',
    unique_key = 'description') 
}}
with raw_staging AS (
    SELECT * FROM {{ source("supermarket","raw_staging") }}
)
SELECT
    id AS product_id,
    name AS product_name,
    description,
    "merchandiseHeir.categoryGroup" AS fruit_or_veg,
    "merchandiseHeir.category" AS category,
    "merchandiseHeir.subCategory" AS subcategory,
    "merchandiseHeir.className" AS detail,
    date_extracted AS date_last_updated,
    _id AS mongodb_document_id
FROM
    raw_staging