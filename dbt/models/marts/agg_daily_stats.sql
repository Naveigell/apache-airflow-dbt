{{ config(materialized='table') }}

select * from {{ ref('int_daily_metrics') }}
