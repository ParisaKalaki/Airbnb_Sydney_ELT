{% snapshot listings_snapshot %}

{{
  config(
    target_schema='silver',          
    unique_key='listing_id',         
    strategy='timestamp',           
    updated_at='scraped_date'       
  )
}}

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
    has_availability,
    availability_30::integer AS availability_30,
    number_of_reviews::integer AS number_of_reviews,
    review_scores_rating::numeric AS review_scores_rating,
    review_scores_accuracy::numeric AS review_scores_accuracy,
    review_scores_cleanliness::numeric AS review_scores_cleanliness,
    review_scores_checkin::numeric AS review_scores_checkin,
    review_scores_communication::numeric AS review_scores_communication,
    review_scores_value::numeric AS review_scores_value,
    lga_code,
    suburb_name
FROM {{ source('bronze', 'airbnb_raw') }}

{% endsnapshot %}
