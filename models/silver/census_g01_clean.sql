{{ config(schema='silver', materialized='table') }}

SELECT
    CAST(LGA_CODE_2016 AS TEXT) AS lga_code,
    Tot_P_P AS total_population,
    Indigenous_P_Tot_P AS indigenous_population,
    Birthplace_Elsewhere_P AS born_overseas_population
FROM {{ source('bronze', 'census_g01_raw') }}
WHERE LGA_CODE_2016 IS NOT NULL
