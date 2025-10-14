{{ config(schema='silver', materialized='view') }}

SELECT
    md5(lower(trim(suburb_name))) AS suburb_key,
    suburb_name,
    l.lga_code
FROM {{ ref('lga_suburb_clean') }} s
LEFT JOIN {{ ref('lga_code_clean') }} l
    ON LOWER(TRIM(s.lga_name)) = LOWER(TRIM(l.lga_name))
