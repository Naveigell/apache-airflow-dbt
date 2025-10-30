{{ config(
    materialized='table',
    tags=['anomaly']
) }}

WITH historical_comparison AS (
    SELECT
        trip_date,
        avg_distance,

        AVG(avg_distance) OVER (
            ORDER BY trip_date
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS avg_30_day_distance,

        (avg_distance / NULLIF(AVG(avg_distance) OVER (
            ORDER BY trip_date
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ), 0)) - 1 AS distance_ratio_change

    FROM {{ ref('agg_daily_stats') }}
)

SELECT
    *
FROM historical_comparison
WHERE ABS(distance_ratio_change) > 0.20
  AND trip_date = (SELECT MAX(trip_date) FROM {{ ref('agg_daily_stats') }})