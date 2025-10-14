{{ config(schema='gold', materialized='view') }}

SELECT
    listing_id,
    md5(cast(listing_id as text)) AS property_key,
    property_type,
    room_type,
    accommodates
FROM {{ ref('property_clean') }}
