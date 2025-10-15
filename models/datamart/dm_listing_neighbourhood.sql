{{ config(schema='datamart', materialized='view') }}

WITH base AS (
    SELECT
        s.suburb_name AS listing_neighbourhood,
        EXTRACT(YEAR FROM f.scraped_date) AS year,
        EXTRACT(MONTH FROM f.scraped_date) AS month,
        h.host_id,
        h.host_is_superhost,
        f.price,
        f.availability_30,
        f.number_of_reviews,
        f.review_scores_rating,
        (30 - f.availability_30) AS number_of_stays,
        (30 - f.availability_30) * f.price AS estimated_revenue,
        CASE WHEN f.availability_30 < 30 THEN TRUE ELSE FALSE END AS is_active
    FROM {{ ref('fact_listings') }} f
    LEFT JOIN {{ ref('dm_host') }} h
        ON f.host_key = h.host_key
    LEFT JOIN {{ ref('dm_suburb') }} s
        ON f.suburb_key = s.suburb_key
),

agg AS (
    SELECT
        listing_neighbourhood,
        year,
        month,
        COUNT(*) AS total_listings,
        COUNT(CASE WHEN is_active THEN 1 END) AS active_listings,
        MIN(CASE WHEN is_active THEN price END) AS min_price,
        MAX(CASE WHEN is_active THEN price END) AS max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN is_active THEN price END) AS median_price,
        ROUND(AVG(CASE WHEN is_active THEN price END), 2) AS avg_price,
        COUNT(DISTINCT host_id) AS distinct_hosts,
        ROUND(
            COUNT(DISTINCT CASE WHEN is_active AND host_is_superhost = 't' THEN host_id END)
            * 100.0 / NULLIF(COUNT(DISTINCT host_id), 0), 2
        ) AS superhost_rate,
        ROUND(AVG(CASE WHEN is_active THEN review_scores_rating END), 2) AS avg_review_score,
        SUM(number_of_stays) AS total_stays,
        ROUND(SUM(CASE WHEN is_active THEN estimated_revenue END) / NULLIF(COUNT(CASE WHEN is_active THEN 1 END), 0), 2)
            AS avg_estimated_revenue_per_active
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
    ROUND(active_listings * 100.0 / NULLIF(total_listings, 0), 2) AS active_listings_rate,
    min_price,
    max_price,
    median_price,
    avg_price,
    distinct_hosts,
    superhost_rate,
    avg_review_score,
    total_stays,
    ROUND((active_listings - prev_active) * 100.0 / NULLIF(prev_active, 0), 2) AS pct_change_active,
    ROUND(((total_listings - active_listings) - prev_inactive) * 100.0 / NULLIF(prev_inactive, 0), 2) AS pct_change_inactive,
    avg_estimated_revenue_per_active
FROM pct_change
ORDER BY listing_neighbourhood, year, month;
