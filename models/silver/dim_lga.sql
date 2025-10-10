{{ config(schema='silver') }}

SELECT DISTINCT
    lga_code,
    lga_name
FROM {{ source('bronze', 'lga_code') }}

