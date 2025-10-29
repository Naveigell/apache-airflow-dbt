{{ config(materialized='table') }}

select * from {{ ref('int_trips_enhanced') }}
