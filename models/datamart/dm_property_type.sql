{{ config(schema='datamart', materialized='view') }}

WITH base AS (
    SELECT
        COALESCE(l.property_type, 'Unknown') AS property_type,
        COALESCE(l.room_type, 'Unknown') AS room_type,
        COALESCE(l.accommodates, 0) AS accommodates,
        EXTRACT(YEAR FROM f.scraped_date) AS year,
        EXTRACT(MONTH FROM f.scraped_date) AS month,
        l.host_id,
        COALESCE(l.has_availability::boolean, FALSE) AS has_availability,
        f.price,
        l.review_scores_rating,
        (30 - l.availability_30) AS number_of_stays,
        (30 - l.availability_30) * f.price AS estimated_revenue,
        CASE WHEN l.host_is_superhost = 't' THEN TRUE ELSE FALSE END AS host_is_superhost
    FROM {{ ref('fact_listings') }} f
    LEFT JOIN {{ ref('listings_clean') }} l
        ON f.listing_id = l.listing_id
    LEFT JOIN {{ ref('host_snapshot') }} h
        ON l.host_id = h.host_id
       AND f.scraped_date BETWEEN h.dbt_valid_from AND COALESCE(h.dbt_valid_to, CURRENT_DATE)
),

agg AS (
    SELECT
        property_type,
        room_type,
        accommodates,
        year,
        month,
        COUNT(*) AS total_listings,
        COUNT(CASE WHEN has_availability THEN 1 END) AS active_listings,
        MIN(CASE WHEN has_availability THEN price END) AS min_price,
        MAX(CASE WHEN has_availability THEN price END) AS max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN has_availability THEN price END) AS median_price,
        AVG(CASE WHEN has_availability THEN price END) AS avg_price,
        COUNT(DISTINCT host_id) AS distinct_hosts,
        COUNT(DISTINCT CASE WHEN has_availability AND host_is_superhost THEN host_id END) * 100.0 / NULLIF(COUNT(DISTINCT host_id),0) AS superhost_rate,
        AVG(CASE WHEN has_availability THEN review_scores_rating END) AS avg_review_score,
        SUM(CASE WHEN has_availability THEN number_of_stays END) AS total_stays,
        SUM(CASE WHEN has_availability THEN estimated_revenue END) AS total_estimated_revenue,
        SUM(CASE WHEN has_availability THEN estimated_revenue END) / NULLIF(COUNT(CASE WHEN has_availability THEN 1 END),0) AS avg_estimated_revenue_per_listing
    FROM base
    GROUP BY property_type, room_type, accommodates, year, month
),

pct_change AS (
    SELECT
        *,
        LAG(active_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY year, month) AS prev_active,
        LAG(total_listings - active_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY year, month) AS prev_inactive
    FROM agg
)

SELECT
    property_type,
    room_type,
    accommodates,
    year,
    month,
    active_listings * 100.0 / NULLIF(total_listings,0) AS active_listings_rate,
    min_price,
    max_price,
    median_price,
    avg_price,
    distinct_hosts,
    superhost_rate,
    avg_review_score,
    total_stays,
    (active_listings - prev_active) * 100.0 / NULLIF(prev_active,0) AS pct_change_active,
    ((total_listings - active_listings) - prev_inactive) * 100.0 / NULLIF(prev_inactive,0) AS pct_change_inactive,
    total_estimated_revenue,
    avg_estimated_revenue_per_listing
FROM pct_change
ORDER BY property_type, room_type, accommodates, year, month
