{{ config(materialized='view') }}

SELECT
    CAST(LocationID AS INTEGER) AS location_id,
    upper(substr(trim(Zone), 1, 1)) || lower(substr(trim(Zone), 2)) AS zone_name,
    upper(substr(trim(Borough), 1, 1)) || lower(substr(trim(Borough), 2)) AS borough,
    upper(substr(trim(service_zone), 1, 1)) || lower(substr(trim(service_zone), 2)) AS service_zone
FROM {{ ref('taxi_zone_lookup') }}
WHERE LocationID IS NOT NULL






