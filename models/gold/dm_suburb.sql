-- models/gold/dm_suburb.sql
{{ config(schema='gold', materialized='view') }}
SELECT DISTINCT
    md5(lower(trim(suburb_name))) AS suburb_key,
    suburb_name,
    lga_code
FROM {{ ref('lga_suburb_clean') }}

