{{ config(schema='gold', materialized='view') }}

WITH mapped AS (
    SELECT
        f.*,
        s.suburb_key,
        l.lga_key
    FROM {{ ref('listings_clean') }} f
    LEFT JOIN {{ ref('dm_suburb') }} s
    ON LOWER(TRIM(f.listing_neighbourhood)) = LOWER(TRIM(s.suburb_name))
    LEFT JOIN {{ ref('dm_lga') }} l
        ON f.lga_code = l.lga_code

)
SELECT
    listing_id,
    scraped_date,
    price,
    number_of_reviews,
    availability_30,
    h.host_key,
    suburb_key,
    lga_key
FROM mapped
LEFT JOIN {{ ref('dm_host') }} h
    ON mapped.host_id = h.host_id
