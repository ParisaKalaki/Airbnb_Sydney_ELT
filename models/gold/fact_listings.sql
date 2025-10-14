{{ config(schema='gold', materialized='view') }}

SELECT
    f.listing_id,
    f.scraped_date,
    f.price,
    f.number_of_reviews,
    f.availability_30,
    h.host_key,        -- reference to dm_host
    s.suburb_key,      -- reference to dm_suburb
    l.lga_key          -- reference to dm_lga
FROM {{ ref('listings_clean') }} f
LEFT JOIN {{ ref('dm_host') }} h
    ON f.host_id = h.host_id
LEFT JOIN {{ ref('dm_suburb') }} s
    ON LOWER(TRIM(f.suburb_name)) = LOWER(TRIM(s.suburb_name))
LEFT JOIN {{ ref('dm_lga') }} l
    ON f.lga_code = l.lga_code
