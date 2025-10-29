-- 1. KPI Summary, Total Trips, Total Revenue, Average Fare
SELECT
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_fare
FROM fct_trips;


-- 2. Trend Analysis – daily trend or monthly trend
SELECT
    DATE(pickup_datetime) AS trip_date,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_fare
FROM fct_trips
GROUP BY trip_date
ORDER BY trip_date;


-- 3. Top Zones – 10 zone that got highest revenue
SELECT
    dz.zone AS pickup_zone,
    SUM(ft.total_amount) AS total_revenue,
    COUNT(*) AS total_trips
FROM fct_trips ft
JOIN dim_zones dz
  ON ft.pickup_location_id = dz.location_id
GROUP BY dz.zone
ORDER BY total_revenue DESC
LIMIT 10;


-- 4. Demand Heatmap – hourly and regional demand patterns.
SELECT
    dz.zone AS pickup_zone,
    EXTRACT(HOUR FROM ft.pickup_datetime) AS pickup_hour,
    COUNT(*) AS total_trips
FROM fct_trips ft
JOIN dim_zones dz
    ON ft.pickup_location_id = dz.location_id
GROUP BY dz.zone, pickup_hour
ORDER BY dz.zone, pickup_hour;



-- 5. Payment Breakdown – proportion of payment methods.
SELECT
    CASE ft.payment_type
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        ELSE 'Other'
    END AS payment_method,
    COUNT(*) AS total_trips,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        2
    ) AS percentage
FROM fct_trips ft
GROUP BY payment_method
ORDER BY total_trips DESC;
