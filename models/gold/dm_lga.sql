{{ config(schema='gold', materialized='view') }}

SELECT
    md5(cast(lga_code as text)) as lga_key,
    lga_code,
    lga_name
FROM {{ ref('lga_code_snapshot') }}
WHERE dbt_valid_to IS NULL
