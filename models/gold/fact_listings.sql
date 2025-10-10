{{ config(schema='gold', materialized='view') }}

SELECT
    f.listing_id,
    f.scraped_date,
    f.price,
    f.host_id,
    f.number_of_reviews,
    f.availability_30,
    h.host_name,
    h.host_is_superhost,
    f.lga_code,
    f.suburb_name
FROM {{ ref('listings_clean') }} f
LEFT JOIN {{ ref('host_snapshot') }} h
    ON f.host_id = h.host_id
   AND f.scraped_date BETWEEN h.dbt_valid_from AND COALESCE(h.dbt_valid_to, CURRENT_DATE)

