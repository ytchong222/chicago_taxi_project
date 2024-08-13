{{ config(materialized='table') }}

SELECT
  EXTRACT(DAYOFWEEK FROM trip_start_timestamp) AS day_of_week,
  COALESCE(CAST(pickup_community_area AS STRING), 'NA') AS pickup_community_area,
  COUNT(*) AS number_of_trips
FROM
  `chicago_taxi_trips.taxi_trips`
WHERE
        FORMAT_TIMESTAMP('%Y-%m-%d', trip_start_timestamp) BETWEEN '2023-06-01' AND '2024-01-02'
GROUP BY
   pickup_community_area
ORDER BY
  number_of_trips DESC
LIMIT 100
