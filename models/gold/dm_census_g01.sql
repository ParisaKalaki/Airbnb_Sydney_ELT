{{ config(schema='gold', materialized='view') }}

SELECT
    md5(lga_code) AS census_g01_key,
    lga_code,
    total_population,
    indigenous_population,
    born_overseas_population
FROM {{ ref('census_g01_clean') }}
