{{ config(schema='silver') }}

SELECT DISTINCT
    lga_name,
    suburb_name
FROM {{ source('bronze', 'lga_suburb') }}
