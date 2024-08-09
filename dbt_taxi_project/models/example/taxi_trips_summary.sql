
{{ config(materialized='table') }}

WITH raw_data AS (
    SELECT
        taxi_id,
        trip_start_timestamp,
        trip_end_timestamp,
        fare,
        tips,
        trip_miles,
        trip_seconds
    FROM
        chicago_taxi_trips.taxi_trips   -- referencing the raw table in BigQuery taxi_trips
)

SELECT
    taxi_id,
    COUNT(*) AS trip_count,
    SUM(fare) AS total_fare,
    SUM(tips) AS total_tips,
    SUM(trip_miles) AS total_miles,
    SUM(trip_seconds) AS total_seconds
FROM
    raw_data
GROUP BY
    taxi_id
