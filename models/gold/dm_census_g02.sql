{{ config(schema='gold', materialized='view') }}

SELECT
    md5(lga_code) AS census_g02_key,
    lga_code,
    Median_age_persons,
    Median_rent_weekly,
    Median_tot_prsnl_inc_weekly,
    Average_household_size
FROM {{ ref('census_g02_clean') }}
