-- models/gold/dm_lga.sql
{{ config(schema='gold', materialized='view') }}
SELECT DISTINCT
    lga_code,
    lga_name
FROM {{ ref('listings_clean') }}
WHERE lga_code IS NOT NULL
