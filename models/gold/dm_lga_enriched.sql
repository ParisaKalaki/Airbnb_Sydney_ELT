{{ config(schema='gold', materialized='table') }}

SELECT
    md5(l.lga_code) AS lga_key,
    l.lga_code,
    l.lga_name,
    g01.total_population,
    g01.indigenous_population,
    g01.born_overseas_population,
    g02.Median_age_persons,
    g02.Median_rent_weekly,
    g02.Median_tot_prsnl_inc_weekly,
    g02.Average_household_size
FROM {{ ref('lga_code_clean') }} l
LEFT JOIN {{ ref('census_g01_clean') }} g01
    ON l.lga_code = g01.lga_code
LEFT JOIN {{ ref('census_g02_clean') }} g02
    ON l.lga_code = g02.lga_code

