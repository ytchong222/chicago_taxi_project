{{ config(materialized='table') }}
SELECT
  EXTRACT(DAYOFWEEK FROM trip_start_timestamp) AS day_of_week,
  EXTRACT(HOUR FROM trip_start_timestamp) AS hour_of_day,
  COUNT(*) AS number_of_trips
FROM
  `chicago_taxi_trips.taxi_trips`
GROUP BY
  day_of_week,
  hour_of_day
ORDER BY
  number_of_trips DESC 
