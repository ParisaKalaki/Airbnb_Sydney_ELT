{{ config(schema='datamart', materialized='view') }}

WITH base AS (
    SELECT
        s.suburb_name AS listing_neighbourhood,
        EXTRACT(YEAR FROM f.scraped_date) AS year,
        EXTRACT(MONTH FROM f.scraped_date) AS month,
        h.host_key,
        h.host_is_superhost,
        p.property_type,
        p.room_type,
        p.accommodates,
        f.price,
        f.number_of_reviews,
        f.availability_30,
        f.scraped_date,
        f.host_key,
        (30 - f.availability_30) AS number_of_stays,
        (30 - f.availability_30) * f.price AS estimated_revenue,
        f.suburb_key,
        f.lga_key,
        f.property_key
    FROM {{ ref('fact_listings') }} f
    LEFT JOIN {{ ref('dm_host') }} h
        ON f.host_key = h.host_key
    LEFT JOIN {{ ref('dm_suburb') }} s
        ON f.suburb_key = s.suburb_key
    LEFT JOIN {{ ref('dm_property') }} p
        ON f.property_key = p.property_key
),

agg AS (
    SELECT
        listing_neighbourhood,
        year,
        month,
        COUNT(*) AS total_listings,
        COUNT(CASE WHEN availability_30 > 0 THEN 1 END) AS active_listings,
        MIN(CASE WHEN availability_30 > 0 THEN price END) AS min_price,
        MAX(CASE WHEN availability_30 > 0 THEN price END) AS max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN availability_30 > 0 THEN price END) AS median_price,
        AVG(CASE WHEN availability_30 > 0 THEN price END) AS avg_price,
        COUNT(DISTINCT host_key) AS distinct_hosts,
        COUNT(DISTINCT CASE WHEN availability_30 > 0 AND host_is_superhost THEN host_key END) * 100.0 / NULLIF(COUNT(DISTINCT host_key),0) AS superhost_rate,
        AVG(CASE WHEN availability_30 > 0 THEN number_of_reviews END) AS avg_review_score,
        SUM(CASE WHEN availability_30 > 0 THEN number_of_stays END) AS total_stays,
        SUM(CASE WHEN availability_30 > 0 THEN estimated_revenue END) AS total_estimated_revenue,
        SUM(CASE WHEN availability_30 > 0 THEN estimated_revenue END) / NULLIF(COUNT(DISTINCT CASE WHEN availability_30 > 0 THEN host_key END),0) AS avg_estimated_revenue_per_host
    FROM base
    GROUP BY listing_neighbourhood, year, month
),

pct_change AS (
    SELECT
        *,
        LAG(active_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY year, month) AS prev_active,
        LAG(total_listings - active_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY year, month) AS prev_inactive
    FROM agg
)

SELECT
    listing_neighbourhood,
    year,
    month,
    active_listings * 100.0 / total_listings AS active_listings_rate,
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
    avg_estimated_revenue_per_host
FROM pct_change
ORDER BY listing_neighbourhood, year, month;
