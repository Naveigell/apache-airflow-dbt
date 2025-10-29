{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('nyc_taxi', 'yellow_tripdata') }}
),

cleaned as (
    select
        cast(VendorID as integer) as vendor_id,
        cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as double) as trip_distance,
        cast(RatecodeID as integer) as rate_code_id,
        store_and_fwd_flag,
        cast(PULocationID as integer) as pickup_location_id,
        cast(DOLocationID as integer) as dropoff_location_id,
        cast(payment_type as integer) as payment_type,
        cast(fare_amount as double) as fare_amount,
        cast(extra as double) as extra,
        cast(mta_tax as double) as mta_tax,
        cast(tip_amount as double) as tip_amount,
        cast(tolls_amount as double) as tolls_amount,
        cast(improvement_surcharge as double) as improvement_surcharge,
        cast(total_amount as double) as total_amount,
        cast(congestion_surcharge as double) as congestion_surcharge,
        cast(airport_fee as double) as airport_fee
    from source
    where trip_distance > 0
      and total_amount > 0
      and pickup_datetime < dropoff_datetime
)

select * from cleaned
