{{ config(schema='silver', materialized='table') }}

SELECT DISTINCT
    LOWER(TRIM(s.suburb_name)) AS suburb_name,
    TRIM(l.lga_code) AS lga_code,
    LOWER(TRIM(l.lga_name)) AS lga_name
FROM {{ source('bronze', 'lga_suburb') }} s
LEFT JOIN {{ source('bronze', 'lga_code') }} l
    ON LOWER(TRIM(s.lga_name)) = LOWER(TRIM(l.lga_name))
WHERE s.suburb_name IS NOT NULL
