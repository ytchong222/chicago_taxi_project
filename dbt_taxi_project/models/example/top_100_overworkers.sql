-- models/top_100_overworker.sql
{{ config(materialized='table') }}
 
WITH 
source AS (
  SELECT
    taxi_id,
    trip_start_timestamp,
    trip_end_timestamp,
    TIMESTAMP_DIFF(trip_end_timestamp, trip_start_timestamp, SECOND) AS trip_duration,
   FROM
    `chicago_taxi_trips.taxi_trips`
    where  TIMESTAMP_DIFF(trip_end_timestamp, trip_start_timestamp, SECOND)>0 
),
ranked_trips AS (
  SELECT
    taxi_id,
    trip_start_timestamp,
    trip_end_timestamp,
    trip_duration
    ROW_NUMBER() OVER (
      PARTITION BY taxi_id ,trip_start_timestamp
      ORDER BY trip_end_timestamp desc
    ) AS row_num
   FROM  source
),
top_100_overworker AS (
  SELECT
    taxi_id,
    trip_start_timestamp,
    trip_end_timestamp,
    trip_duration,
    LAG(trip_end_timestamp) OVER (
      PARTITION BY taxi_id 
      ORDER BY trip_start_timestamp, trip_end_timestamp
    ) AS prev_trip_end_timestamp,
   
  FROM
    ranked_trips
  where  row_num=1
 
)

SELECT
  taxi_id,
  COUNT(*) AS total_trips,
  SUM(trip_duration) / 3600 AS total_hours_trips, 
  AVG(TIMESTAMP_DIFF(trip_start_timestamp, prev_trip_end_timestamp, MINUTE)/60) AS avg_break_hours
FROM
  top_100_overworker
WHERE
  (TIMESTAMP_DIFF(trip_start_timestamp, prev_trip_end_timestamp, MINUTE)/60) >0  
  and (TIMESTAMP_DIFF(trip_start_timestamp, prev_trip_end_timestamp, MINUTE)/60) < 8  
GROUP BY
  taxi_id
ORDER BY
  total_hours_trips DESC
LIMIT 100;


