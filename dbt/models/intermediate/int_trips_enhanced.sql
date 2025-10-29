{{ config(materialized='view') }}

with trips as (
    select * from {{ ref('stg_taxi_trips') }}
),

zones as (
    select * from {{ ref('stg_taxi_zones') }}
),

enhanced as (
    select
        trips.*,
        zones.borough as pickup_borough,
        zones.zone_name as pickup_zone,
        datediff('minute', pickup_datetime, dropoff_datetime) as trip_duration_min,
        case
            when trip_distance = 0 then null
            else trip_distance / (datediff('minute', pickup_datetime, dropoff_datetime) / 60.0)
        end as avg_speed_mph
    from trips
    left join zones
        on trips.pickup_location_id = zones.location_id
)

select * from enhanced
