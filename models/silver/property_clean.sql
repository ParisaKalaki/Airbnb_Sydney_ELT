{{ config(schema='silver', materialized='table') }}

SELECT DISTINCT
    listing_id,
    property_type,
    room_type,
    accommodates
FROM {{ ref('listings_clean') }}
WHERE listing_id IS NOT NULL
