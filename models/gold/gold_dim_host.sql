{{ config(schema='gold') }}

SELECT *
FROM {{ ref('host_snapshot') }}  -- Already has SCD2 columns: valid_from, valid_to
