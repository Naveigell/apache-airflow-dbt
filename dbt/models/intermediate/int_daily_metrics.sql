{{ config(materialized='view') }}

with trips as (
    select * from {{ ref('int_trips_enhanced') }}
),

daily as (
    select
        date(pickup_datetime) as trip_date,
        count(*) as total_trips,
        avg(trip_distance) as avg_distance,
        avg(total_amount) as avg_fare,
        avg(passenger_count) as avg_passengers,
        avg(avg_speed_mph) as avg_speed
    from trips
    group by 1
)

select * from daily
