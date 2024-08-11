
-- Use the `ref` function to select from other models
{{ config(materialized='table') }}


select  
    FORMAT_DATE('%Y-%W', PARSE_DATE('%Y-%m-%d', tdate)) AS weekdate,
	taxi_id,
	count(taxi_id) total_taxi,
	sum(trip_count) total_trips,
	sum(total_fare) as total_fare, 
	sum(total_tips) as total_tips,
	SUM(total_miles) AS total_miles,
	SUM(total_seconds) AS total_seconds,
	sum(total_tolls) as total_tolls,
	from  dbt_taxi_trips.taxi_trips_summary
	where tdate is not null
group by  FORMAT_DATE('%Y-%W', PARSE_DATE('%Y-%m-%d', tdate)),taxi_id





