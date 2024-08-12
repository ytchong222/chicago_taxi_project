-- models/top_100_overworker.sql
{{ config(materialized='table') }}
 WITH top_100_overworker AS (

        SELECT
            taxi_id,
            trip_start_timestamp,
            trip_end_timestamp,
            TIMESTAMP_DIFF(trip_end_timestamp, trip_start_timestamp, SECOND) end AS trip_duration,
             LAG(trip_end_timestamp) OVER (PARTITION BY taxi_id ORDER BY trip_start_timestamp,a.trip_end_timestamp) AS prev_trip_end_timestamp
    FROM
        `chicago_taxi_trips.taxi_trips`
 where trip_end_timestamp>=trip_start_timestamp
)
SELECT
    taxi_id,
    COUNT(*) AS total_trips,
    SUM(trip_duration) / 3600 AS total_hours_trips, 
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

