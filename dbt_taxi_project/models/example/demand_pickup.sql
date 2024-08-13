{{ config(materialized='table') }}

SELECT
   CASE 
    WHEN EXTRACT(DAYOFWEEK FROM trip_start_timestamp) = 1 THEN 'Sunday'
    WHEN EXTRACT(DAYOFWEEK FROM trip_start_timestamp) = 2 THEN 'Monday'
    WHEN EXTRACT(DAYOFWEEK FROM trip_start_timestamp) = 3 THEN 'Tuesday'
    WHEN EXTRACT(DAYOFWEEK FROM trip_start_timestamp) = 4 THEN 'Wednesday'
    WHEN EXTRACT(DAYOFWEEK FROM trip_start_timestamp) = 5 THEN 'Thursday'
    WHEN EXTRACT(DAYOFWEEK FROM trip_start_timestamp) = 6 THEN 'Friday'
    WHEN EXTRACT(DAYOFWEEK FROM trip_start_timestamp) = 7 THEN 'Saturday'
    ELSE 'NA' 
  END AS day_of_week,
  COALESCE(CAST(pickup_community_area AS STRING), 'NA') AS pickup_community_area,
  COUNT(*) AS number_of_trips
FROM
  `chicago_taxi_trips.taxi_trips`
WHERE
        FORMAT_TIMESTAMP('%Y-%m-%d', trip_start_timestamp) BETWEEN '2023-10-01' AND '2024-01-02'
GROUP BY
   pickup_community_area
ORDER BY
  number_of_trips DESC

