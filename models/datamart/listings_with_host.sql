{{ config(schema='datamart') }}

SELECT
    f.listing_id,
    f.scraped_date,
    f.price,
    f.number_of_reviews,
    f.availability_30,
    h.host_name,
    h.host_is_superhost,
    h.host_neighbourhood,
    f.lga_code,
    f.suburb_name
FROM {{ ref('fact_listings') }} f
LEFT JOIN {{ ref('host_snapshot') }} h
    ON f.host_id = h.host_id
   AND f.scraped_date BETWEEN h.dbt_valid_from AND COALESCE(h.dbt_valid_to, CURRENT_DATE)
