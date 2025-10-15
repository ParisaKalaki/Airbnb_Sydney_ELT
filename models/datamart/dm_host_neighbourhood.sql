{{ config(schema='datamart', materialized='view') }}

WITH base AS (
    SELECT
        COALESCE(NULLIF(TRIM(l.host_neighbourhood), ''), 'Unknown') AS host_neighbourhood,
        EXTRACT(YEAR FROM f.scraped_date) AS year,
        EXTRACT(MONTH FROM f.scraped_date) AS month,
        f.host_id,
        (30 - l.availability_30) * f.price AS estimated_revenue
    FROM {{ ref('fact_listings') }} f
    LEFT JOIN {{ ref('listings_clean') }} l
        ON f.listing_id = l.listing_id
),

agg AS (
    SELECT
        host_neighbourhood,
        year,
        month,
        COUNT(DISTINCT host_id) AS distinct_hosts,
        SUM(estimated_revenue) AS total_estimated_revenue,
        SUM(estimated_revenue) / NULLIF(COUNT(DISTINCT host_id),0) AS estimated_revenue_per_host
    FROM base
    GROUP BY host_neighbourhood, year, month
)

SELECT *
FROM agg
ORDER BY host_neighbourhood, year, month
