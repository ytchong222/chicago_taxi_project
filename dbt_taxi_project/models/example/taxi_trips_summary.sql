
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
		extra,
		tolls,
        case when trip_seconds is null then abs(TIMESTAMP_DIFF(trip_end_timestamp, trip_start_timestamp, SECOND)) end as trip_seconds
    FROM
        chicago_taxi_trips.taxi_trips   -- referencing the raw table in BigQuery taxi_trips
)

SELECT
    (FORMAT_TIMESTAMP('%Y-%m-%d', trip_end_timestamp)) as tdate,
    taxi_id,
	company as taxi_company,
    COUNT(taxi_id) AS trip_count,
    SUM(fare) AS total_fare,
    SUM(tips) AS total_tips,
    SUM(trip_miles) AS total_miles,
    SUM(trip_seconds) AS total_seconds,
	sum(extra) as total_extra_payment,
	sum(tolls) as total_tolls,
	payment_type
FROM
    raw_data
GROUP BY
    taxi_id,company,(FORMAT_TIMESTAMP('%Y-%m-%d', trip_end_timestamp)),payment_type


