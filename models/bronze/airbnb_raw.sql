{{ config(schema='bronze', materialized='table') }}

SELECT * FROM {{ source('bronze', 'airbnb_raw') }}
