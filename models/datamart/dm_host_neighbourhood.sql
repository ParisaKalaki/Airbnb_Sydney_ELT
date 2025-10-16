{{ config(schema='datamart', materialized='view') }}

WITH base AS (
    SELECT
        f.lga_key,
        EXTRACT(YEAR FROM f.scraped_date) AS year,
        EXTRACT(MONTH FROM f.scraped_date) AS month,
        f.host_key,
        (30 - f.availability_30) * f.price AS estimated_revenue
    FROM {{ ref('fact_listings') }} f
    WHERE f.availability_30 IS NOT NULL
),

-- Join to LGA dimension for readable location names
joined AS (
    SELECT
        COALESCE(l.lga_name, 'Unknown') AS host_neighbourhood_lga,
        b.year,
        b.month,
        b.host_key,
        b.estimated_revenue
    FROM base b
    LEFT JOIN {{ ref('dm_lga') }} l
        ON b.lga_key = l.lga_key
),

agg AS (
    SELECT
        host_neighbourhood_lga,
        year,
        month,
        COUNT(DISTINCT host_key) AS distinct_hosts,
        SUM(estimated_revenue) AS total_estimated_revenue,
        ROUND(SUM(estimated_revenue) / NULLIF(COUNT(DISTINCT host_key), 0), 2) AS estimated_revenue_per_host
    FROM joined
    GROUP BY host_neighbourhood_lga, year, month
)

SELECT
    host_neighbourhood_lga,
    year,
    month,
    distinct_hosts,
    ROUND(total_estimated_revenue, 2) AS total_estimated_revenue,
    estimated_revenue_per_host
FROM agg
ORDER BY host_neighbourhood_lga, year, month
