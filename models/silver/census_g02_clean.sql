{{ config(schema='silver', materialized='table') }}

SELECT
    CAST(LGA_CODE_2016 AS TEXT) AS lga_code,
    Median_age_persons,
    Median_rent_weekly,
    Median_tot_prsnl_inc_weekly,
    Average_household_size
FROM {{ source('bronze', 'census_g02_raw') }}
WHERE LGA_CODE_2016 IS NOT NULL
