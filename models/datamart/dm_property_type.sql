{{ config(schema='datamart', materialized='view') }}

WITH base AS (
    SELECT
        f.scraped_date,
        f.price,
        f.availability_30,
        f.number_of_reviews,
        f.host_key,
        f.property_key
    FROM {{ ref('fact_listings') }} f
    WHERE f.scraped_date BETWEEN '2020-05-01' AND '2021-04-30'
),

-- Join only small dimensions (using indexed integer keys)
joined AS (
    SELECT
        COALESCE(p.property_type, 'Unknown') AS property_type,
        COALESCE(p.room_type, 'Unknown') AS room_type,
        COALESCE(p.accommodates, 0) AS accommodates,
        h.host_id,
        COALESCE(h.host_is_superhost, FALSE) AS host_is_superhost,
        f.price,
        f.availability_30,
        f.number_of_reviews,
        EXTRACT(YEAR FROM f.scraped_date) AS year,
        EXTRACT(MONTH FROM f.scraped_date) AS month
    FROM base f
    LEFT JOIN {{ ref('dm_property') }} p
        ON f.property_key = p.property_key
    LEFT JOIN {{ ref('dm_host') }} h
        ON f.host_key = h.host_key
),

-- Aggregate metrics efficiently
agg AS (
    SELECT
        property_type,
        room_type,
        accommodates,
        year,
        month,
        COUNT(*) AS total_listings,
        COUNT(CASE WHEN availability_30 > 0 THEN 1 END) AS active_listings,
        MIN(CASE WHEN availability_30 > 0 THEN price END) AS min_price,
        MAX(CASE WHEN availability_30 > 0 THEN price END) AS max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN availability_30 > 0 THEN price END) AS median_price,
        AVG(CASE WHEN availability_30 > 0 THEN price END) AS avg_price,
        COUNT(DISTINCT host_id) AS distinct_hosts,
        COUNT(DISTINCT CASE WHEN availability_30 > 0 AND host_is_superhost IS TRUE THEN host_id END)
            * 100.0 / NULLIF(COUNT(DISTINCT host_id), 0) AS superhost_rate,
        AVG(CASE WHEN availability_30 > 0 THEN number_of_reviews END) AS avg_review_score,
        SUM(CASE WHEN availability_30 > 0 THEN (30 - availability_30) END) AS total_stays,
        SUM(CASE WHEN availability_30 > 0 THEN (30 - availability_30) * price END) AS total_estimated_revenue,
        SUM(CASE WHEN availability_30 > 0 THEN (30 - availability_30) * price END)
            / NULLIF(COUNT(CASE WHEN availability_30 > 0 THEN 1 END), 0)
            AS avg_estimated_revenue_per_listing
    FROM joined
    GROUP BY property_type, room_type, accommodates, year, month
),

-- Add percentage change metrics
pct_change AS (
    SELECT
        *,
        LAG(active_listings) OVER (
            PARTITION BY property_type, room_type, accommodates
            ORDER BY year, month
        ) AS prev_active,
        LAG(total_listings - active_listings) OVER (
            PARTITION BY property_type, room_type, accommodates
            ORDER BY year, month
        ) AS prev_inactive
    FROM agg
)

SELECT
    property_type,
    room_type,
    accommodates,
    year,
    month,
    ROUND(active_listings * 100.0 / NULLIF(total_listings, 0), 2) AS active_listings_rate,
    min_price,
    max_price,
    median_price,
    avg_price,
    distinct_hosts,
    ROUND(superhost_rate, 2) AS superhost_rate,
    ROUND(avg_review_score, 2) AS avg_review_score,
    total_stays,
    ROUND((active_listings - prev_active) * 100.0 / NULLIF(prev_active, 0), 2) AS pct_change_active,
    ROUND(((total_listings - active_listings) - prev_inactive) * 100.0 / NULLIF(prev_inactive, 0), 2) AS pct_change_inactive,
    ROUND(total_estimated_revenue, 2) AS total_estimated_revenue,
    ROUND(avg_estimated_revenue_per_listing, 2) AS avg_estimated_revenue_per_listing
FROM pct_change
ORDER BY property_type, room_type, accommodates, year, month
