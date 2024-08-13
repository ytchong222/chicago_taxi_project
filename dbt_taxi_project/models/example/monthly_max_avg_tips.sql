{{ config(materialized='table') }}

SELECT    FORMAT_DATE('%Y-%m-%d', LAST_DAY(CAST(trip_end_timestamp AS DATE))) AS monthdate,
  count(distinct taxi_id) as no_taxi,
  count(taxi_id) as taxi_trips,
  AVG(tips) AS average_tip,
  MAX(tips) AS max_tip,
  MIN(tips) AS min_tip
FROM
   chicago_taxi_trips.taxi_trips
where  FORMAT_TIMESTAMP('%Y-%m-%d', trip_start_timestamp) BETWEEN '2023-06-01' AND '2024-01-02'
group by FORMAT_TIMESTAMP('%Y-%m-%d', trip_end_timestamp) 
ORDER BY
  monthdate DESC,max_tip DESC 