{{ config(materialized='table') }}

select distinct
    pickup_location_id as location_id,
    pickup_borough as borough,
    pickup_zone as zone
from {{ ref('int_trips_enhanced') }}
