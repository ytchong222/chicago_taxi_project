{{ config(materialized='table') }}

WITH raw_data AS (
    SELECT
        taxi_id,
        company,
        trip_start_timestamp,
        trip_end_timestamp,
        fare,
        tips,
        trip_miles,
        payment_type,
        extras,
        tolls,
        CASE 
            WHEN trip_seconds IS NULL THEN 
                ABS(TIMESTAMP_DIFF(trip_end_timestamp, trip_start_timestamp, SECOND))
            ELSE 
                trip_seconds
        END AS trip_seconds
    FROM
        chicago_taxi_trips.taxi_trips   -- referencing the raw table in BigQuery taxi_trips
)

SELECT
    FORMAT_TIMESTAMP('%Y-%m-%d', trip_end_timestamp) AS tdate,
    taxi_id,
    company AS taxi_company,
    COUNT(taxi_id) AS trip_count,
    SUM(fare) AS total_fare,
    SUM(tips) AS total_tips,
    SUM(trip_miles) AS total_miles,
    SUM(trip_seconds) AS total_seconds,
    SUM(extras) AS total_extra_payment,
    SUM(tolls) AS total_tolls,
    payment_type
FROM
    raw_data
GROUP BY
    tdate,
    taxi_id,
    taxi_company,
    payment_type
