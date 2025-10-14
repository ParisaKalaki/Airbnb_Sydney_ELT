{{ config(schema='gold') }}

SELECT *
FROM {{ ref('dim_suburb') }}  -- No SCD2, just unique LGA codes/names
