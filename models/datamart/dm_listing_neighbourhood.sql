{{ config(schema='datamart', materialized='view') }}

WITH base AS (
    SELECT
        -- Use suburb name as the "listing_neighbourhood"
        s.suburb_name AS listing_neighbourhood,
        EXTRACT(YEAR FROM f.scraped_date) AS year,
        EXTRACT(MONTH FROM f.scraped_date) AS month,
        h.host_id,
        f.price,
        f.availability_30,
        (30 - f.availability_30) AS number_of_stays,
        (30 - f.availability_30) * f.price AS estimated_revenue
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
        COUNT(DISTINCT host_id) AS distinct_hosts,
        SUM(number_of_stays) AS total_stays,
        SUM(estimated_revenue) AS total_estimated_revenue,
        ROUND(SUM(estimated_revenue) / NULLIF(COUNT(DISTINCT host_id), 0), 2) AS avg_revenue_per_host
    FROM base
    GROUP BY listing_neighbourhood, year, month
)

SELECT
    listing_neighbourhood,
    year,
    month,
    total_listings,
    distinct_hosts,
    total_stays,
    total_estimated_revenue,
    avg_revenue_per_host
FROM agg
ORDER BY listing_neighbourhood, year, month
