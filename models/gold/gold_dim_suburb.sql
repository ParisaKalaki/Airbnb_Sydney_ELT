{{ config(schema='gold') }}

SELECT *
FROM {{ ref('dim_lga') }}  -- No SCD2, just unique LGA codes/names
