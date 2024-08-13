{{ config(materialized='table') }}

WITH trip_revenue AS (
    SELECT
        taxi_id,
        company,
        pickup_community_area,
        dropoff_community_area,
        trip_start_timestamp,
        trip_end_timestamp,
        EXTRACT(DAYOFWEEK FROM trip_start_timestamp) AS day_of_week,
        EXTRACT(MONTH FROM trip_start_timestamp) AS month,
        EXTRACT(YEAR FROM trip_start_timestamp) AS year,
        fare,
        tips,
        tolls,
        extras,
        (fare + tips  + extras- tolls) AS total_revenue
    FROM
        `chicago_taxi_trips.taxi_trips`
    WHERE
           FORMAT_TIMESTAMP('%Y-%m-%d', trip_end_timestamp)  BETWEEN '2023-10-01' AND '2024-01-02'  
)

SELECT
    company,
    pickup_community_area,
    dropoff_community_area,
    year,
    month,
    day_of_week,
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
    year,
    month,
    day_of_week
ORDER BY
    total_revenue_generated DESC
LIMIT 100
