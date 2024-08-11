
-- Use the `ref` function to select from other models
{{ config(materialized='table') }}


SELECT 
    substring(tdate,1,7) as monthdate,
    taxi_company,
    COUNT(taxi_id) AS total_taxi,
    SUM(trip_count) AS total_trips,
    SUM(total_fare) AS total_fare,
    SUM(total_tips) AS total_tips,
    SUM(total_miles) AS total_miles,
    SUM(total_seconds) AS total_seconds,
    SUM(total_tolls) AS total_tolls
FROM  
    dbt_taxi_trips.taxi_trips_summary
GROUP BY 
    substring(tdate,1,7), 
    taxi_company



