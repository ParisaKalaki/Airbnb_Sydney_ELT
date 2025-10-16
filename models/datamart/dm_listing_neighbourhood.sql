{{ config(schema='datamart', materialized='view') }}

WITH base AS (
    -- Use only keys and numeric columns from gold.fact_listings (smaller, indexed)
    SELECT
        f.scraped_date,
        f.price,
        f.availability_30,
        f.number_of_reviews,
        f.suburb_key,
        f.host_key,
        f.property_key
    FROM {{ ref('fact_listings') }} f
    WHERE f.scraped_date BETWEEN '2020-05-01' AND '2021-04-30'
),

-- Join dimensions (small lookup tables)
joined AS (
    SELECT
        s.suburb_name AS listing_neighbourhood,
        h.host_id,
        h.host_is_superhost,
        p.property_type,
        f.scraped_date,
        f.price,
        f.availability_30,
        f.number_of_reviews
    FROM base f
    LEFT JOIN {{ ref('dm_suburb') }} s
        ON f.suburb_key = s.suburb_key
    LEFT JOIN {{ ref('dm_host') }} h
        ON f.host_key = h.host_key
    LEFT JOIN {{ ref('dm_property') }} p
        ON f.property_key = p.property_key
),

-- Pre-aggregate base metrics to reduce rows early
preagg AS (
    SELECT
        COALESCE(listing_neighbourhood, 'Unknown') AS listing_neighbourhood,
        EXTRACT(YEAR FROM scraped_date) AS year,
        EXTRACT(MONTH FROM scraped_date) AS month,
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
            / NULLIF(COUNT(DISTINCT CASE WHEN availability_30 > 0 THEN host_id END), 0)
            AS avg_estimated_revenue_per_active_listing
    FROM joined
    GROUP BY listing_neighbourhood, year, month
),

-- Compute percentage changes month over month
final AS (
    SELECT
        p.*,
        LAG(active_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY year, month) AS prev_active,
        LAG(total_listings - active_listings) OVER (PARTITION BY listing_neighbourhood ORDER BY year, month) AS prev_inactive
    FROM preagg p
)

SELECT
    listing_neighbourhood,
    year,
    month,
    ROUND(active_listings * 100.0 / total_listings, 2) AS active_listings_rate,
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
    ROUND(avg_estimated_revenue_per_active_listing, 2) AS avg_estimated_revenue_per_active_listing
FROM final
ORDER BY listing_neighbourhood, year, month
