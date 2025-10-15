{{ config(schema='datamart', materialized='view') }}

WITH base AS (
    SELECT
        COALESCE(NULLIF(TRIM(l.listing_neighbourhood), ''), 'Unknown') AS listing_neighbourhood,
        EXTRACT(YEAR FROM f.scraped_date) AS year,
        EXTRACT(MONTH FROM f.scraped_date) AS month,
        l.host_id,
        l.has_availability,
        f.price,
        l.review_scores_rating,
        l.host_is_superhost,
        (30 - l.availability_30) AS number_of_stays,
        (30 - l.availability_30) * f.price AS estimated_revenue
    FROM {{ ref('fact_listings') }} f
    LEFT JOIN {{ ref('listings_clean') }} l
        ON f.listing_id = l.listing_id
),
agg AS (
    SELECT
        listing_neighbourhood,
        year,
        month,
        COUNT(*) AS total_listings,
        COUNT(CASE WHEN has_availability='t' THEN 1 END) AS active_listings,
        MIN(CASE WHEN has_availability='t' THEN price END) AS min_price,
        MAX(CASE WHEN has_availability='t' THEN price END) AS max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN has_availability='t' THEN price END) AS median_price,
        AVG(CASE WHEN has_availability='t' THEN price END) AS avg_price,
        COUNT(DISTINCT host_id) AS distinct_hosts,
        COUNT(DISTINCT CASE WHEN has_availability='t' AND host_is_superhost='t' THEN host_id END) * 100.0 / COUNT(DISTINCT host_id) AS superhost_rate,
        AVG(CASE WHEN has_availability='t' THEN review_scores_rating END) AS avg_review_score,
        SUM(CASE WHEN has_availability='t' THEN number_of_stays END) AS total_stays,
        SUM(CASE WHEN has_availability='t' THEN estimated_revenue END) AS total_estimated_revenue,
        SUM(CASE WHEN has_availability='t' THEN estimated_revenue END) / NULLIF(COUNT(DISTINCT CASE WHEN has_availability='t' THEN host_id END),0) AS avg_estimated_revenue_per_host
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
ORDER BY listing_neighbourhood, year, month
