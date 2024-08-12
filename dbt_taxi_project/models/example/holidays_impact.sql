-- models/holiday_impact.sql
{{ config(materialized='table') }}
WITH trip_analysis AS (
    SELECT
        FORMAT_TIMESTAMP('%Y-%m-%d', trip_end_timestamp) AS tdate,
        COUNT(*) AS total_trips,
        SUM(trip_total) AS total_revenue
    FROM
         `chicago_taxi_trips.taxi_trips`
    GROUP BY
        tdate
)
SELECT
    ta.tdate,
    ta.total_trips,
    ROUND(ta.total_revenue, 2) as total_revenue,
    case when d.holiday_name is null then 'working day' else d.holiday_name end as holiday_name
FROM
    trip_analysis ta
left JOIN
    `dbt_taxi_trips.dim_holiday` d ON cast(ta.tdate as date) =cast(d.holiday_date as date) 
ORDER BY
    tdate desc limit 100
