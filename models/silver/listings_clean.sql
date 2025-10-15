{{ config(
    schema='silver',
    materialized='incremental',
    unique_key=['listing_id', 'scraped_date']
) }}

WITH base AS (
    SELECT
        listing_id,
        scrape_id,
        scraped_date,
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
        WHERE scraped_date > COALESCE(
            (SELECT MAX(scraped_date) FROM {{ this }}),
            '1900-01-01'  -- fallback if Silver is empty
        )
    {% endif %}
)

SELECT * FROM base
