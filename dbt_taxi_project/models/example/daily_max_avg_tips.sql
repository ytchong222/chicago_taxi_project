{{ config(materialized='table') }}

SELECT  FORMAT_TIMESTAMP('%Y-%m-%d', trip_end_timestamp) AS tdate,
  count(distinct taxi_id) as no_taxi,
  count(taxi_id) as taxi_trips,
  AVG(tips) AS average_tip,
  MAX(tips) AS max_tip
FROM
   chicago_taxi_trips.taxi_trips
where  FORMAT_TIMESTAMP('%Y-%m-%d', trip_start_timestamp) BETWEEN '2023-10-01' AND '2024-01-02'
group by FORMAT_TIMESTAMP('%Y-%m-%d', trip_end_timestamp) 
ORDER BY
  tdate DESC,max_tip DESC 