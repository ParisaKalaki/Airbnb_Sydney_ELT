{{ config(
    schema='silver',
    materialized='incremental',
    unique_key=['listing_id', 'scraped_date']
) }}

WITH airbnb AS (
    SELECT
        listing_id,
        scrape_id,
        scraped_date::date AS scraped_date,
        host_id,
        host_name,
        host_since::date AS host_since,
        host_is_superhost,
        host_neighbourhood,
        listing_neighbourhood,
        property_type,
        room_type,
        accommodates::integer AS accommodates,
        CAST(price AS numeric) AS price,
        CASE
            WHEN has_availability = 't' THEN TRUE
            WHEN has_availability = 'f' THEN FALSE
            ELSE NULL
        END AS has_availability,
        availability_30::integer AS availability_30,
        number_of_reviews::integer AS number_of_reviews,
        review_scores_rating::numeric AS review_scores_rating,
        review_scores_accuracy::numeric AS review_scores_accuracy,
        review_scores_cleanliness::numeric AS review_scores_cleanliness,
        review_scores_checkin::numeric AS review_scores_checkin,
        review_scores_communication::numeric AS review_scores_communication,
        review_scores_value::numeric AS review_scores_value
    FROM {{ source('bronze', 'airbnb_raw') }}
    {% if is_incremental() %}
        WHERE scraped_date > (SELECT MAX(scraped_date) FROM {{ this }})
    {% endif %}
),

lga_mapping AS (
    SELECT
        s.suburb_name,
        l.lga_code
    FROM {{ source('bronze', 'lga_suburb') }} s
    INNER JOIN {{ source('bronze', 'lga_code') }} l
        ON LOWER(TRIM(s.lga_name)) = LOWER(TRIM(l.lga_name))
),

mapped AS (
    SELECT
        a.*,
        m_listing.lga_code AS listing_lga_code,
        m_listing.suburb_name AS listing_suburb_name,
        m_host.lga_code AS host_lga_code
    FROM airbnb a

    LEFT JOIN lga_mapping m_listing
        ON LOWER(REPLACE(REPLACE(TRIM(a.listing_neighbourhood), '.', ''), '''', ''))
           = LOWER(TRIM(m_listing.suburb_name))
    LEFT JOIN lga_mapping m_host
        ON LOWER(REPLACE(REPLACE(TRIM(a.host_neighbourhood), '.', ''), '''', ''))
           = LOWER(TRIM(m_host.suburb_name))
)

SELECT 
    a.listing_id, a.scrape_id, a.scraped_date, a.host_id, a.host_name, a.host_since,
    a.host_is_superhost, a.host_neighbourhood, a.listing_neighbourhood,
    a.property_type, a.room_type, a.accommodates, a.price, a.has_availability,
    a.availability_30, a.number_of_reviews, a.review_scores_rating,
    a.review_scores_accuracy, a.review_scores_cleanliness, a.review_scores_checkin,
    a.review_scores_communication, a.review_scores_value,

    listing_lga_code AS lga_code,
    listing_suburb_name AS suburb_name,
    host_lga_code
FROM mapped a
