{{ config(schema='gold', materialized='table') }}

SELECT
    host_id,
    md5(cast(host_id as text)) AS host_key,
    host_name,
    host_is_superhost
FROM {{ ref('host_snapshot') }}

