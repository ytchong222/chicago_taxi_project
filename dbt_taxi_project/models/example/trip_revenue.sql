{{ config(materialized='table') }}

WITH trip_revenue AS (
    SELECT
        taxi_id,
        company,
        COALESCE(CAST(pickup_community_area AS STRING), 'NA') AS pickup_community_area,
        COALESCE(CAST(dropoff_community_area AS STRING), 'NA') AS dropoff_community_area,
        trip_start_timestamp,
        trip_end_timestamp,
        fare,
        tips,
        tolls,
        extras,
        (fare + tips  + extras- tolls) AS total_revenue
    FROM
        `chicago_taxi_trips.taxi_trips`
    WHERE
           FORMAT_TIMESTAMP('%Y-%m-%d', trip_end_timestamp)  BETWEEN '2023-06-01' AND '2024-01-02'  
)

SELECT
    FORMAT_TIMESTAMP('%Y-%m', trip_end_timestamp) as monthdate,
    company,
    pickup_community_area,
    dropoff_community_area,
    COUNT(*) AS total_trips,
    SUM(total_revenue) AS total_revenue_generated,
    AVG(total_revenue) AS avg_revenue_per_trip,
    SUM(fare) AS total_fare,
    SUM(tips) AS total_tips,
    SUM(tolls) AS total_tolls,
    SUM(extras) AS total_extras
FROM
    trip_revenue
GROUP BY
    company,
    pickup_community_area,
    dropoff_community_area,
    monthdate
    
ORDER BY
    total_revenue_generated DESC
LIMIT 100