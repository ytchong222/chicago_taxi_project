-- models/top_100_overworker.sql
{{ config(materialized='table') }}
 WITH top_100_overworker AS (
   select   a.taxi_id,
        a.trip_start_timestamp,
        a.trip_end_timestamp,
        a.trip_seconds,trip_duration,
        LAG(a.trip_end_timestamp) OVER (PARTITION BY a.taxi_id ORDER BY a.trip_start_timestamp,a.trip_end_timestamp) AS prev_trip_end_timestamp
     from(
        SELECT
            taxi_id,
            trip_start_timestamp,
            case 
            when trip_end_timestamp is null 
                 then  
                 TIMESTAMP_ADD(trip_start_timestamp, INTERVAL trip_seconds SECOND) 
            else trip_end_timestamp end as trip_end_timestamp,
            case 
            when trip_end_timestamp is not null 
                 then 
                 TIMESTAMP_DIFF(trip_end_timestamp, trip_start_timestamp, SECOND) end AS trip_duration,
            CASE 
            WHEN trip_end_timestamp IS NULL THEN 
                trip_seconds 
            ELSE 
                0
        END AS trip_seconds
    FROM
        `chicago_taxi_trips.taxi_trips`
   
         )a
 where a.trip_end_timestamp>=a.trip_start_timestamp
)
SELECT
    taxi_id,
    COUNT(*) AS total_trips,
    SUM(ABS(trip_duration)+trip_seconds) / 3600 AS total_hours_trips, 
    AVG(ABS(TIMESTAMP_DIFF(trip_start_timestamp, prev_trip_end_timestamp, HOUR)))  AS avg_break_hours
FROM
    top_100_overworker
WHERE
    TIMESTAMP_DIFF(trip_start_timestamp, prev_trip_end_timestamp, HOUR) < 8
GROUP BY
    taxi_id
ORDER BY
    total_hours_trips DESC
limit 100

