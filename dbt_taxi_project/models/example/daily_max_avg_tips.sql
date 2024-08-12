{{ config(materialized='table') }}

SELECT  FORMAT_TIMESTAMP('%Y-%m-%d', trip_end_timestamp) AS tdate,count(distinct taxi_id) as taxi_count,
  FORMAT('%3.2f',
    AVG(tips)) AS average_tip,
  FORMAT('%3.2f',
    MAX(tips)) AS max_tip
FROM
   chicago_taxi_trips.taxi_trips
group by FORMAT_TIMESTAMP('%Y-%m-%d', trip_end_timestamp) 
ORDER BY
  tdate DESC,average_tip DESC limit 30