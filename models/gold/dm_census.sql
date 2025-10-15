{{ config(schema='gold', materialized='table') }}

SELECT
    g01.lga_code,
    g01.total_population,
    g01.indigenous_population,
    g01.born_overseas_population,
    g02.Median_age_persons,
    g02.Median_rent_weekly,
    g02.Median_tot_prsnl_inc_weekly,
    g02.Average_household_size
FROM {{ ref('dm_census_g01') }} g01
LEFT JOIN {{ ref('dm_census_g02') }} g02
    ON g01.lga_code = g02.lga_code
